import base64
import functools
import glob
import os
import pty
import select
import shutil
import socket
import subprocess
import tempfile
import threading
import unittest
from unittest import mock

from parameterized import parameterized

import utils

SECRET = "s3cret"
PARSED_LOOKING_SECRET = "env:AQL_TEST_NEVER_SET"
VAR = "AQL_TEST_PW"
UNREACHABLE = ["-h", "127.0.0.1", "-p", "1", "-c", "show namespaces"]
CONNECT_FAILED = "Error -10:"
TLS_KEY_FAILED = "Error -9:"


def b64(value: str) -> str:
    return base64.b64encode(value.encode()).decode()


def make_key(path: str, password: str):
    subprocess.run(
        [
            "openssl", "genpkey", "-algorithm", "EC",
            "-pkeyopt", "ec_paramgen_curve:P-256",
            "-aes-256-cbc", "-pass", "pass:" + password, "-out", path,
        ],
        check=True,
        capture_output=True,
    )


def run(args, env=None, config=None):
    conf = ["--only-config-file", config] if config else ["--no-config-file"]
    with mock.patch.dict(os.environ, {}):
        os.environ.pop(VAR, None)
        os.environ.update(env or {})
        out = utils.run_aql(conf + args + UNREACHABLE)
    return out.returncode, out.stdout.decode() + out.stderr.decode()


def recv_exact(conn, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            break
        data += chunk
    return data


def capture_login(args, env=None, config=None) -> bytes:
    """Runs aql against a fake server and returns the login request it sends, which holds the credential."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(8)
    packets = []

    def serve():
        while True:
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            with conn:
                conn.settimeout(5)
                header = recv_exact(conn, 8)
                size = int.from_bytes(header, "big") & 0xFFFFFFFFFFFF
                packets.append(header + recv_exact(conn, size))

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()

    conf = ["--only-config-file", config] if config else ["--no-config-file"]
    target = ["-h", "127.0.0.1", "-p", str(srv.getsockname()[1]), "-c", "show namespaces"]
    try:
        with mock.patch.dict(os.environ, {}):
            os.environ.pop(VAR, None)
            os.environ.update(env or {})
            out = utils.run_aql(conf + args + target)
    finally:
        # close() alone does not wake accept() on Linux; macOS raises ENOTCONN here.
        try:
            srv.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        srv.close()
        thread.join(5)

    output = out.stdout.decode() + out.stderr.decode()
    if not packets or len(set(packets)) != 1:
        raise AssertionError("expected one distinct login request, got {}:\n{}".format(len(set(packets)), output))
    return packets[0]


@functools.lru_cache(maxsize=None)
def literal_login(password: str) -> bytes:
    """Login request for a literal -P value, shared by the tests that compare against it."""
    return capture_login(["-U", "admin", "-P", password])


def run_with_prompt(args, typed: str) -> str:
    aql = os.path.abspath((glob.glob("target/*/bin/aql") + glob.glob("../target/*/bin/aql"))[0])
    pid, fd = pty.fork()

    if pid == 0:
        os.execv(aql, [aql, "--no-config-file"] + args + UNREACHABLE)

    out = b""
    sent = False

    while True:
        ready, _, _ = select.select([fd], [], [], 10)
        if not ready:
            break
        try:
            chunk = os.read(fd, 1024)
        except OSError:
            break
        if not chunk:
            break
        out += chunk
        if not sent and b"Password: " in out:
            os.write(fd, typed.encode() + b"\n")
            sent = True

    os.close(fd)
    os.waitpid(pid, 0)
    return out.decode()


class PasswordSourceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp()
        cls.addClassCleanup(shutil.rmtree, cls.dir)

        cls.key = os.path.join(cls.dir, "key.pem")
        make_key(cls.key, SECRET)
        cls.parsed_looking_key = os.path.join(cls.dir, "key2.pem")
        make_key(cls.parsed_looking_key, PARSED_LOOKING_SECRET)

        cls.missing = os.path.join(cls.dir, "missing")
        cls.subdir = os.path.join(cls.dir, "subdir")
        os.mkdir(cls.subdir)

        for name, content in [
            ("empty", b""),
            ("newline", b"\n"),
            ("pw", (SECRET + "\n").encode()),
            ("pw_crlf", (SECRET + "\r\n").encode()),
            ("pw_bare", SECRET.encode()),
        ]:
            path = os.path.join(cls.dir, name)
            with open(path, "wb") as f:
                f.write(content)
            setattr(cls, name, path)

    def option_args(self, opt):
        if opt == "--password":
            return ["-U", "admin"]
        return ["--tls-enable", "--tls-keyfile", self.key]

    @parameterized.expand(
        [
            (opt.strip("-").replace("-", "_") + "_" + case, opt, value, env, expected)
            for opt in ["--password", "--tls-keyfile-password"]
            for case, value, env, expected in [
                ("env_unset", "env:" + VAR, {}, "environment variable {var} is not set or empty"),
                ("env_empty", "env:" + VAR, {VAR: ""}, "environment variable {var} is not set or empty"),
                ("env_b64_unset", "env-b64:" + VAR, {}, "environment variable {var} is not set or empty"),
                ("env_b64_invalid", "env-b64:" + VAR, {VAR: "not base64!"}, "invalid base64 in environment variable {var}"),
                ("env_b64_empty", "env-b64:" + VAR, {VAR: b64("\n")}, "environment variable {var} decodes to an empty value"),
                ("b64_invalid", "b64:" + "c2VjcmV0!", {}, "invalid base64 in b64: value"),
                ("b64_no_payload", "b64:", {}, "invalid base64 in b64: value"),
                ("b64_empty", "b64:" + b64("\n"), {}, "b64: value decodes to an empty value"),
                ("file_missing", "file:{missing}", {}, "cannot read file {missing}: No such file or directory"),
                ("file_directory", "file:{subdir}", {}, "cannot read file {subdir}: Is a directory"),
                ("file_empty", "file:{empty}", {}, "file {empty} is empty"),
                ("file_newline_only", "file:{newline}", {}, "file {newline} is empty"),
            ]
        ]
    )
    def test_error(self, _, opt, value, env, expected):
        names = {
            "var": VAR,
            "missing": self.missing,
            "subdir": self.subdir,
            "empty": self.empty,
            "newline": self.newline,
        }
        value = value.format(**names)
        rc, out = run(self.option_args(opt) + [opt + "=" + value], env)

        self.assertNotEqual(rc, 0)
        self.assertIn(opt + ": " + expected.format(**names) + "\n", out)
        self.assertNotIn(CONNECT_FAILED, out)
        if value.startswith("b64:") and len(value) > 4:
            self.assertNotIn(value[4:], out)

    @parameterized.expand(
        [
            ("literal", SECRET, {}),
            ("env", "env:" + VAR, {VAR: SECRET}),
            ("env_b64", "env-b64:" + VAR, {VAR: b64(SECRET)}),
            ("env_b64_trailing_newline", "env-b64:" + VAR, {VAR: b64(SECRET + "\n")}),
            ("env_b64_wrapped", "env-b64:" + VAR, {VAR: b64(SECRET)[:4] + "\n" + b64(SECRET)[4:]}),
            ("b64", "b64:" + b64(SECRET), {}),
            ("b64_trailing_newline", "b64:" + b64(SECRET + "\n"), {}),
            ("file", "file:{pw}", {}),
            ("file_crlf", "file:{pw_crlf}", {}),
            ("file_no_newline", "file:{pw_bare}", {}),
        ]
    )
    def test_tls_keyfile_password_resolves(self, _, value, env):
        value = value.format(pw=self.pw, pw_crlf=self.pw_crlf, pw_bare=self.pw_bare)
        rc, out = run(["--tls-enable", "--tls-keyfile", self.key, "--tls-keyfile-password", value], env)

        self.assertNotEqual(rc, 0)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    @parameterized.expand(
        [
            ("literal", "wrong", {}),
            ("env", "env:" + VAR, {VAR: "wrong"}),
            ("b64", "b64:" + b64("wrong"), {}),
        ]
    )
    def test_tls_keyfile_wrong_password(self, _, value, env):
        rc, out = run(["--tls-enable", "--tls-keyfile", self.key, "--tls-keyfile-password", value], env)

        self.assertNotEqual(rc, 0)
        self.assertIn(TLS_KEY_FAILED, out)

    def test_resolved_value_is_not_parsed_again(self):
        rc, out = run(
            ["--tls-enable", "--tls-keyfile", self.parsed_looking_key, "--tls-keyfile-password", "env:" + VAR],
            {VAR: PARSED_LOOKING_SECRET},
        )

        self.assertNotIn("--tls-keyfile-password:", out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    @parameterized.expand(
        [
            ("attached_long", ["--password=env:" + VAR]),
            ("separate_long", ["--password", "env:" + VAR]),
            ("attached_short", ["-Penv:" + VAR]),
            ("separate_short", ["-P", "env:" + VAR]),
        ]
    )
    def test_password_resolves(self, _, args):
        rc, out = run(["-U", "admin"] + args, {VAR: SECRET})

        self.assertNotIn("--password:", out)
        self.assertNotIn(SECRET, out)
        self.assertIn(CONNECT_FAILED, out)

    def test_password_from_config_file(self):
        config = os.path.join(self.dir, "astools.conf")
        with open(config, "w") as f:
            f.write('[cluster]\nuser = "admin"\npassword = "env:{}"\n'.format(VAR))

        rc, out = run([], {}, config)
        self.assertNotEqual(rc, 0)
        self.assertIn("--password: environment variable {} is not set or empty\n".format(VAR), out)

        rc, out = run([], {VAR: SECRET}, config)
        self.assertNotIn("--password:", out)
        self.assertIn(CONNECT_FAILED, out)

    def test_command_line_overrides_config_file_before_resolving(self):
        config = os.path.join(self.dir, "astools-override.conf")
        with open(config, "w") as f:
            f.write('[cluster]\nuser = "admin"\npassword = "env:{}"\n'.format(VAR))

        rc, out = run(["-P", "literal"], {}, config)

        self.assertNotIn("--password:", out)
        self.assertIn(CONNECT_FAILED, out)

    def test_tls_keyfile_password_from_config_file(self):
        config = os.path.join(self.dir, "astools-tls.conf")
        with open(config, "w") as f:
            f.write(
                '[cluster]\ntls-enable = true\ntls-keyfile = "{}"\ntls-keyfile-password = "b64:{}"\n'.format(
                    self.key, b64(SECRET)
                )
            )

        rc, out = run([], {}, config)

        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    def test_prompted_password_is_literal(self):
        out = run_with_prompt(["-U", "admin", "-P"], "env:" + VAR)

        self.assertIn("Enter Password: ", out)
        self.assertNotIn("--password:", out)
        self.assertIn(CONNECT_FAILED, out)

    def test_prompted_tls_keyfile_password_is_literal(self):
        out = run_with_prompt(
            ["--tls-enable", "--tls-keyfile", self.parsed_looking_key, "--tls-keyfile-password"],
            PARSED_LOOKING_SECRET,
        )

        self.assertIn("Enter TLS-Keyfile Password: ", out)
        self.assertNotIn("--tls-keyfile-password:", out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)


class PasswordReachesServerTest(unittest.TestCase):
    """The resolved --password is what the client sends at login, not just something that resolves."""

    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp()
        cls.addClassCleanup(shutil.rmtree, cls.dir)

    def test_fixture_tells_passwords_apart(self):
        self.assertNotEqual(literal_login(SECRET), literal_login("wrong"))

    @parameterized.expand(
        [
            ("env", "env:" + VAR, {VAR: SECRET}),
            ("b64", "b64:" + b64(SECRET), {}),
        ]
    )
    def test_command_line(self, _, value, env):
        self.assertEqual(capture_login(["-U", "admin", "-P", value], env), literal_login(SECRET))

    def test_command_line_wrong_value(self):
        self.assertEqual(capture_login(["-U", "admin", "-P", "env:" + VAR], {VAR: "wrong"}), literal_login("wrong"))

    def test_config_file(self):
        config = os.path.join(self.dir, "astools.conf")
        with open(config, "w") as f:
            f.write('[cluster]\nuser = "admin"\npassword = "env:{}"\n'.format(VAR))

        self.assertEqual(capture_login([], {VAR: SECRET}, config), literal_login(SECRET))


if __name__ == "__main__":
    unittest.main()
