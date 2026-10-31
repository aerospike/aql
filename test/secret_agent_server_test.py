import base64
import json
import os
import shutil
import string
import tempfile
import time
import unittest
import uuid
from unittest import mock

import aerospike
from parameterized import parameterized

import utils
from secret_agent_test import b64, closed_port, make_ca, make_leaf, start_agent, write

SERVER_IMAGE = "aerospike/aerospike-server-enterprise"
SERVER_TAG = "8.2.0.0"
CONF_DIR = "/opt/work"
UNSET_VAR = "AQL_SA_UNSET"

SECRETS = {
    "pw": "sa-good-7Hq2",
    "wrong": "sa-bad-3Kx9",
    "literal_ref": "sa-ref-9Lm4",
    "prefix_lookalike": "env:" + UNSET_VAR,
    "longest": "p" * 63,
}

USERS = {
    "sa-user": SECRETS["pw"],
    "sa-literal": "secrets:aql:literal_ref",
    "sa-lookalike": SECRETS["prefix_lookalike"],
    "sa-longest": SECRETS["longest"],
}

SELECT_SEED = "SELECT * FROM test.sa WHERE PK = 'seed'"
SEED_ROW = '| "seed" | 7 |'
INVALID_CREDENTIAL = "AEROSPIKE_INVALID_CREDENTIAL"
LOGIN_FAILED = "Error 65: Failed to connect\n"


def server_conf(port: int) -> str:
    with open(utils.absolute_path("aerospike.conf")) as f:
        conf = string.Template(f.read()).substitute(
            state_directory="/opt/aerospike",
            service_port=port,
            fabric_port=3001,
            heartbeat_port=3002,
            info_port=3003,
            access_address="127.0.0.1",
        )
    return "security {\n}\n\n" + conf.replace("service {", "service {\n\tfeature-key-file env-b64:FEATURES", 1)


def start_server(test_cls, dir: str, featkey: str):
    port = closed_port()
    write(os.path.join(dir, "aerospike.conf"), server_conf(port))

    client = utils._get_docker_client()
    try:
        client.images.get("{}:{}".format(SERVER_IMAGE, SERVER_TAG))
    except Exception:
        client.images.pull(SERVER_IMAGE, tag=SERVER_TAG)

    container = client.containers.create(
        "{}:{}".format(SERVER_IMAGE, SERVER_TAG),
        command="/usr/bin/asd --foreground --config-file {}/aerospike.conf".format(CONF_DIR),
        name="aql-sa-sec-{}".format(os.getpid()),
        ports={"{}/tcp".format(port): ("127.0.0.1", port)},
        volumes={dir: {"bind": CONF_DIR, "mode": "ro"}},
        environment={"FEATURES": featkey},
    )
    test_cls.addClassCleanup(container.remove, force=True)
    container.start()
    return container, port


def admin_client(container, port: int, timeout: float = 60):
    config = {"hosts": [("127.0.0.1", port)], "user": "admin", "password": "admin"}
    deadline = time.time() + timeout

    while True:
        try:
            return aerospike.client(config).connect()
        except aerospike.exception.AerospikeError:
            if time.time() > deadline:
                raise RuntimeError(
                    "secured server not ready on port {}:\n{}".format(port, container.logs().decode(errors="replace"))
                )
            time.sleep(0.5)


class SecretAgentServerTest(unittest.TestCase):
    """Logs in to a secured Enterprise server with passwords from a real secret agent."""

    @classmethod
    def setUpClass(cls):
        featkey = os.environ.get("FEATKEY")
        if not featkey:
            if os.environ.get("CI"):
                raise RuntimeError("FEATKEY must be set to a base64 encoded feature key file")
            raise unittest.SkipTest("FEATKEY is not set to a base64 encoded feature key file")

        try:
            base64.b64decode("".join(featkey.split()), validate=True)
        except ValueError:
            raise RuntimeError("FEATKEY is not base64; set it to a base64 encoded feature key file")

        cls.dir = tempfile.mkdtemp()
        cls.addClassCleanup(shutil.rmtree, cls.dir)
        os.chmod(cls.dir, 0o755)

        write(os.path.join(cls.dir, "secrets.json"), json.dumps({k: b64(v) for k, v in SECRETS.items()}))
        write(os.path.join(cls.dir, "other.json"), "{}")
        cls.ca = make_ca(cls.dir, "ca")
        agent_cert = make_leaf(cls.dir, "agent", cls.ca, "DNS:localhost,IP:127.0.0.1")

        container, port = start_server(cls, cls.dir, featkey)
        cls.port = str(port)
        cls.agent_port = str(start_agent(cls, cls.dir, "sec-tcp", None))
        cls.tls_port = str(start_agent(cls, cls.dir, "sec-tls", agent_cert, cls.ca))

        admin = admin_client(container, port)
        admin.admin_grant_roles("admin", ["read-write"])
        admin.close()

        cls.client = admin_client(container, port)
        cls.addClassCleanup(cls.client.close)

        for user, password in USERS.items():
            cls.client.admin_create_user(user, password, ["read-write"])
            cls.addClassCleanup(cls.client.admin_drop_user, user)

        cls.client.put(("test", "sa", "seed"), {"a": 7}, policy={"key": aerospike.POLICY_KEY_SEND})

    def config(self, content: str) -> str:
        path = os.path.join(self.dir, "astools-{}.conf".format(uuid.uuid4().hex))
        write(path, content)
        return path

    def aql(self, args, config=None, command=SELECT_SEED):
        conf = ["--only-config-file", config] if config else ["--no-config-file"]
        with mock.patch.dict(os.environ, {}):
            os.environ.pop(UNSET_VAR, None)
            out = utils.run_aql(conf + ["-h", "127.0.0.1", "-p", self.port] + args + ["-c", command], timeout=60)
        return out.returncode, out.stdout.decode(errors="replace") + out.stderr.decode(errors="replace")

    def assert_logged_in(self, rc: int, out: str):
        self.assertEqual(rc, 0, out)
        self.assertIn(SEED_ROW, out)

    def assert_rejected(self, rc: int, out: str):
        self.assertNotEqual(rc, 0, out)
        self.assertIn(INVALID_CREDENTIAL, out)
        self.assertIn(LOGIN_FAILED, out)
        self.assertNotIn("--password:", out)
        self.assertNotIn("secret-agent:", out)

    def test_login_reads_and_writes(self):
        args = ["-U", "sa-user", "-P", "secrets:aql:pw", "--sa-port", self.agent_port]

        rc, out = self.aql(args, command="INSERT INTO test.sa (PK, a) VALUES ('k1', 1)")
        self.assertEqual(rc, 0, out)
        self.assertIn("OK, 1 record affected.", out)
        self.assertEqual(self.client.get(("test", "sa", "k1"))[2], {"a": 1})

        rc, out = self.aql(args, command="SELECT * FROM test.sa WHERE PK = 'k1'")
        self.assertEqual(rc, 0, out)
        self.assertIn('| "k1" | 1 |', out)

    def test_wrong_fetched_password_is_rejected_by_server(self):
        rc, out = self.aql(["-U", "sa-user", "-P", "secrets:aql:wrong", "--sa-port", self.agent_port])

        self.assert_rejected(rc, out)
        for value in [SECRETS["pw"], SECRETS["wrong"], b64(SECRETS["pw"]), b64(SECRETS["wrong"])]:
            self.assertNotIn(value, out)

    def test_reference_is_never_sent(self):
        rc, out = self.aql(["-U", "sa-literal", "-P", "b64:" + b64(USERS["sa-literal"])])
        self.assert_logged_in(rc, out)

        rc, out = self.aql(["-U", "sa-literal", "-P", "secrets:aql:literal_ref", "--sa-port", self.agent_port])
        self.assert_rejected(rc, out)

    def test_fetched_value_is_not_resolved_again(self):
        rc, out = self.aql(["-U", "sa-lookalike", "-P", "secrets:aql:prefix_lookalike", "--sa-port", self.agent_port])

        self.assert_logged_in(rc, out)

    @parameterized.expand(
        [
            ("hostname", "localhost", "--sa-ca-file"),
            ("ip", "127.0.0.1", "--sa-ca-file"),
            ("legacy_cafile", "localhost", "--sa-cafile"),
        ]
    )
    def test_tls_agent(self, _, address, ca_opt):
        rc, out = self.aql(
            ["-U", "sa-user", "-P", "secrets:aql:pw", "--sa-address", address, "--sa-port", self.tls_port, ca_opt, self.ca]
        )

        self.assert_logged_in(rc, out)

    @parameterized.expand(
        [
            ("default_sections", [], '[cluster]\nuser = "sa-user"\npassword = "secrets:aql:pw"\n[secret-agent]\nsa-port = {port}\n'),
            (
                "instance_sections",
                ["--instance", "e2e"],
                '[cluster]\nuser = "sa-user"\npassword = "secrets:aql:wrong"\n[secret-agent]\nsa-port = {closed}\n'
                '[cluster_e2e]\nuser = "sa-user"\npassword = "secrets:aql:pw"\n[secret-agent_e2e]\nsa-port = {port}\n',
            ),
        ]
    )
    def test_config_file(self, _, args, content):
        config = self.config(content.format(port=self.agent_port, closed=closed_port()))
        rc, out = self.aql(args, config)

        self.assert_logged_in(rc, out)

    def test_longest_password(self):
        rc, out = self.aql(["-U", "sa-longest", "-P", "secrets:aql:longest", "--sa-port", self.agent_port])

        self.assert_logged_in(rc, out)

    def test_command_line_beats_config_file(self):
        config = self.config(
            '[cluster]\nuser = "sa-user"\npassword = "secrets:aql:wrong"\n[secret-agent]\nsa-port = {}\n'.format(self.agent_port)
        )

        rc, out = self.aql([], config)
        self.assert_rejected(rc, out)

        rc, out = self.aql(["-P", "secrets:aql:pw"], config)
        self.assert_logged_in(rc, out)


if __name__ == "__main__":
    unittest.main()
