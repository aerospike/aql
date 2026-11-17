import os
import shutil
import tempfile
import unittest

import aerospike

import utils
from secret_agent_server_test import SERVER_IMAGE, SERVER_TAG, admin_client, feature_key, pid_alive
from secret_agent_test import closed_port, make_ca, make_leaf, write

CLIENT_IMAGE = "aql-e2e-fips-client"
OWNER_LABEL = "aql-fips-owner"
FIPS_ENV = {"OPENSSL_FORCE_FIPS_MODE": "1"}
TLS_NAME = "fips-server"
TLS_PORT = 4333
PKI_USER = "fips-pki"
PASSWORD_USER = "fips-user"
PASSWORD = "fips-pw-5Rt8"
SELECT_SEED = "SELECT * FROM test.fips WHERE PK = 'seed'"
SEED_ROW = '| "seed" | 7 |'
NON_FIPS_CIPHER = "ECDHE-RSA-CHACHA20-POLY1305"

SERVER_CONF = """security {{
}}

service {{
	cluster-name aql-fips
	feature-key-file env-b64:FEATURES
	run-as-daemon false
	work-directory /opt/aerospike
	proto-fd-max 1024
}}

logging {{
	console {{
		context any info
	}}
}}

network {{
	tls {tls_name} {{
		cert-file /opt/work/server.pem
		key-file /opt/work/server-key.pem
		ca-file /opt/work/ca.pem
	}}

	service {{
		address any
		port {port}
		access-address 127.0.0.1
		tls-port {tls_port}
		tls-name {tls_name}
		tls-authenticate-client any
	}}

	heartbeat {{
		mode mesh
		address any
		port 3002
		interval 100
		timeout 3
		connect-timeout-ms 100
	}}

	fabric {{
		port 3001
		address any
	}}

	info {{
		port 3003
		address any
	}}
}}

namespace test {{
	replication-factor 1
	default-ttl 0
	storage-engine memory {{
		data-size 1G
	}}
}}
"""


def build_client_image():
    try:
        utils._get_docker_client().images.build(
            path=utils.absolute_path("fips"), tag=CLIENT_IMAGE, rm=True
        )
    except Exception as e:
        raise RuntimeError("building {} failed: {}".format(CLIENT_IMAGE, e)) from None


def remove_leftovers(client):
    for container in client.containers.list(all=True, filters={"label": OWNER_LABEL}):
        owner = int(container.labels[OWNER_LABEL])
        if owner == os.getpid() or not pid_alive(owner):
            container.remove(force=True)
    for network in client.networks.list(filters={"label": OWNER_LABEL}):
        owner = int(network.attrs["Labels"][OWNER_LABEL])
        if owner == os.getpid() or not pid_alive(owner):
            network.remove()


class FipsTest(unittest.TestCase):
    """aql built against RHEL 9's OpenSSL, run with that OpenSSL forced into FIPS mode."""

    @classmethod
    def setUpClass(cls):
        feature_key()
        try:
            cls.start()
        except BaseException:
            cls.doClassCleanups()
            raise

    @classmethod
    def start(cls):
        docker = utils._get_docker_client()
        remove_leftovers(docker)
        labels = {OWNER_LABEL: str(os.getpid())}

        cls.dir = tempfile.mkdtemp()
        cls.addClassCleanup(shutil.rmtree, cls.dir)
        os.chmod(cls.dir, 0o755)
        ca = make_ca(cls.dir, "ca")
        make_leaf(cls.dir, "server", ca, "DNS:" + TLS_NAME)
        make_leaf(cls.dir, PKI_USER, ca, "DNS:" + PKI_USER, usage="clientAuth")

        network = docker.networks.create("aql-fips-{}".format(os.getpid()), labels=labels)
        cls.addClassCleanup(network.remove)

        port = closed_port()
        write(
            os.path.join(cls.dir, "aerospike.conf"),
            SERVER_CONF.format(port=port, tls_port=TLS_PORT, tls_name=TLS_NAME),
        )
        try:
            docker.images.get("{}:{}".format(SERVER_IMAGE, SERVER_TAG))
        except Exception:
            docker.images.pull(SERVER_IMAGE, tag=SERVER_TAG)
        server = docker.containers.create(
            "{}:{}".format(SERVER_IMAGE, SERVER_TAG),
            command="/usr/bin/asd --foreground --config-file /opt/work/aerospike.conf",
            name="aql-fips-server-{}".format(os.getpid()),
            ports={"{}/tcp".format(port): ("127.0.0.1", port)},
            volumes={cls.dir: {"bind": "/opt/work", "mode": "ro"}},
            labels=labels,
            environment={"FEATURES": feature_key()},
            network=network.name,
        )
        cls.addClassCleanup(server.remove, force=True)
        server.start()

        admin = admin_client(server, port)
        admin.admin_create_user(PKI_USER, "unused-" + PASSWORD, ["read-write"])
        admin.admin_create_user(PASSWORD_USER, PASSWORD, ["read-write"])
        admin.close()
        writer = aerospike.client(
            {"hosts": [("127.0.0.1", port)], "user": PASSWORD_USER, "password": PASSWORD}
        ).connect()
        writer.put(("test", "fips", "seed"), {"a": 7}, policy={"key": aerospike.POLICY_KEY_SEND})
        writer.close()

        server.reload()
        server_ip = server.attrs["NetworkSettings"]["Networks"][network.name]["IPAddress"]

        build_client_image()
        cls.client = docker.containers.run(
            CLIENT_IMAGE,
            command="sleep infinity",
            name="aql-fips-client-{}".format(os.getpid()),
            detach=True,
            labels=labels,
            network=network.name,
            volumes={
                utils.absolute_path(".."): {"bind": "/aql-src", "mode": "ro"},
                cls.dir: {"bind": "/certs", "mode": "ro"},
            },
        )
        cls.addClassCleanup(cls.client.remove, force=True)

        rc, out = cls.run_in_client(
            "cp -a /aql-src /build && { make -C /build cleanall; make -C /build AQL_VERSION=fips-e2e; }"
            " >/tmp/build.log 2>&1 || { tail -40 /tmp/build.log; exit 1; }",
            fips=False,
        )
        if rc != 0:
            raise RuntimeError("building aql in {} failed:\n{}".format(CLIENT_IMAGE, out))

        cls.tls = (
            "--no-config-file -h {}:{}:{} --tls-enable --tls-cafile /certs/ca.pem"
            " --tls-certfile /certs/{}.pem --tls-keyfile /certs/{}-key.pem"
        ).format(server_ip, TLS_NAME, TLS_PORT, PKI_USER, PKI_USER)

    @classmethod
    def run_in_client(cls, cmd: str, fips: bool = True) -> tuple[int, str]:
        result = cls.client.exec_run(["sh", "-c", cmd], environment=FIPS_ENV if fips else {})
        return result.exit_code, result.output.decode(errors="replace")

    def aql(self, args: str, fips: bool = True) -> tuple[int, str]:
        return self.run_in_client(
            '/build/target/Linux-*/bin/aql {} {} -c "{}" </dev/null'.format(self.tls, args, SELECT_SEED),
            fips=fips,
        )

    def test_forced_fips_mode_is_enforced(self):
        """Guards the other tests: the container's OpenSSL serves SHA-256 and refuses MD5."""
        self.assertEqual(self.run_in_client("openssl sha256 /dev/null")[0], 0)
        self.assertNotEqual(self.run_in_client("openssl md5 /dev/null")[0], 0)

    def test_aql_loads_the_container_openssl(self):
        rc, out = self.run_in_client("ldd /build/target/Linux-*/bin/aql", fips=False)

        self.assertEqual(rc, 0, out)
        self.assertRegex(out, r"libssl\.so\.3 => /(usr/)?lib64/libssl\.so\.3")
        self.assertRegex(out, r"libcrypto\.so\.3 => /(usr/)?lib64/libcrypto\.so\.3")

    def test_pki_login_over_tls(self):
        rc, out = self.aql("--auth PKI")

        self.assertEqual(rc, 0, out)
        self.assertIn(SEED_ROW, out)

    def test_password_login_over_tls(self):
        rc, out = self.aql("-U {} -P {}".format(PASSWORD_USER, PASSWORD))

        self.assertEqual(rc, 0, out)
        self.assertIn(SEED_ROW, out)

    def test_tls_refuses_a_cipher_fips_does_not_approve(self):
        rc, out = self.aql(
            "--auth PKI --tls-protocols TLSv1.2 --tls-cipher-suite " + NON_FIPS_CIPHER
        )

        self.assertNotEqual(rc, 0, out)
        self.assertIn("no compatible cipher found", out)

    def test_the_same_cipher_connects_without_fips_mode(self):
        """Guards the refusal above: the server itself accepts the cipher."""
        rc, out = self.aql(
            "--auth PKI --tls-protocols TLSv1.2 --tls-cipher-suite " + NON_FIPS_CIPHER, fips=False
        )

        self.assertEqual(rc, 0, out)
        self.assertIn(SEED_ROW, out)


if __name__ == "__main__":
    unittest.main()
