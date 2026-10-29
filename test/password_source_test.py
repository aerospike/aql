import base64
import contextlib
import functools
import glob
import os
import pty
import select
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import termios
import threading
import time
import unittest
from unittest import mock

from parameterized import parameterized

import utils

SECRET = "s3cret"
PARSED_LOOKING_SECRET = "env:AQL_TEST_NEVER_SET"
VAR = "AQL_TEST_PW"
OTHER_VAR = "AQL_TEST_OTHER_PW"
UNREACHABLE = ["-h", "127.0.0.1", "-p", "1", "-c", "show namespaces"]
CONNECT_FAILED = "Error -10:"
TLS_KEY_FAILED = "Error -9:"
PKI_PASSWORD_REJECTED = "Error 64:"
NO_USER_WARNING = "Warning: {}: ignored because no user is set\n"
MACOS_GETPASS = "macOS getpass keeps at most 128 typed characters"

PASSWORD_HELP = [
    "Password used to authenticate with cluster. Default: none",
    "It can be one of the following:",
    "1) Environment variable: 'env:<VAR>', used exactly as set",
    "2) Base64 encoded environment variable: 'env-b64:<VAR>'",
    "3) Base64 encoded string: 'b64:<BASE64>'",
    "4) File: 'file:<PATH>', the whole file less one trailing '\\n' or",
    "   '\\r\\n'. PATH should name a regular file.",
    "5) Aerospike Secret Agent: 'secrets:<resource>:<key>'",
    "6) String: 'PASSWORD'",
    "Prefixes are case-sensitive. A base64 value may be line wrapped,",
    "and one trailing newline is dropped after decoding. A 'b64:' value",
    "on the command line is as visible as a literal password.",
    "A password that itself begins with 'env:', 'env-b64:', 'b64:',",
    "'file:' or 'secrets:' must be given as 'b64:' or typed at the",
    "prompt.",
    "The password is read only when a user is set and the auth mode is",
    "not PKI. It must be at most 63 bytes.",
    "A value fetched with 'secrets:' is used as-is.",
    "User will be prompted on command line if -P specified and no",
    "password is given. The typed password is used as is. With no",
    "terminal, the password is read from one line of stdin and has",
    "surrounding whitespace trimmed.",
]

TLS_KEYFILE_PASSWORD_HELP = [
    "Password to load protected tls-keyfile.",
    "It can be one of the following:",
    "1) Environment variable: 'env:<VAR>', used exactly as set",
    "2) Base64 encoded environment variable: 'env-b64:<VAR>'",
    "3) Base64 encoded string: 'b64:<BASE64>'",
    "4) File: 'file:<PATH>', the first line of the file",
    "5) Aerospike Secret Agent: 'secrets:<resource>:<key>'",
    "6) String: 'PASSWORD'",
    "Prefixes are case-sensitive. A base64 value may be line wrapped,",
    "and one trailing newline is dropped after decoding.",
    "A password that itself begins with 'env:', 'env-b64:', 'b64:',",
    "'file:' or 'secrets:' must be given as 'b64:'.",
    "Surrounding whitespace is trimmed before the form is read. A",
    "password with surrounding whitespace must be given as 'b64:'.",
    "The password is read only when a key file is set. It must be at",
    "most 1024 bytes.",
    "A value fetched with 'secrets:' is used as-is.",
    "Default: none",
    "User will be prompted on command line if --tls-keyfile-password",
    "specified and no password is given. The typed password",
    "accepts the forms above except 'secrets:'.",
    "With no terminal, the password is read from one line of stdin.",
]


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


def run(args, env=None, config=None, timeout=None, piped=None):
    conf = ["--only-config-file", config] if config else ["--no-config-file"]
    with mock.patch.dict(os.environ, {}):
        os.environ.pop(VAR, None)
        os.environ.update(env or {})
        out = utils.run_aql(conf + args + UNREACHABLE, timeout=timeout, piped=piped)
    return out.returncode, out.stdout.decode(errors="replace") + out.stderr.decode(errors="replace")


@contextlib.contextmanager
def fifo(path: str, data: bytes, endless: bool = False):
    """Feeds data through a FIFO at path and holds the write end open until the block exits."""
    os.mkfifo(path)
    done = threading.Event()

    def write():
        try:
            with open(path, "wb", buffering=0) as f:
                f.write(data)
                while endless and not done.is_set():
                    f.write(data)
                done.wait()
        except BrokenPipeError:
            pass

    thread = threading.Thread(target=write, daemon=True)
    thread.start()
    try:
        yield path
    finally:
        done.set()
        thread.join(5)


def recv_exact(conn, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            break
        data += chunk
    return data


def capture_login(
    args, env=None, config=None, typed=None, piped=None, stdin=None, terminal_stdin=False, **terminal_options
) -> bytes:
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
            if typed is None:
                out = utils.run_aql(conf + args + target, piped=piped, stdin=stdin)
                output = out.stdout.decode() + out.stderr.decode()
            elif terminal_stdin:
                output = run_with_terminal_stdin(args, typed, config, target, **terminal_options)[0]
            else:
                output = run_with_prompt(args, typed, config, target, stdin)
    finally:
        # close() alone does not wake accept() on Linux; macOS raises ENOTCONN here.
        try:
            srv.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        srv.close()
        thread.join(5)

    if not packets or len(set(packets)) != 1:
        raise AssertionError("expected one distinct login request, got {}:\n{}".format(len(set(packets)), output))
    return packets[0]


@functools.lru_cache(maxsize=None)
def literal_login(password: str) -> bytes:
    """Login request for a literal -P value, shared by the tests that compare against it."""
    return capture_login(["-U", "admin", "-P", password])


def run_with_prompt(args, typed: str, config=None, target=UNREACHABLE, stdin=None) -> str:
    """Runs aql with a pty as its controlling terminal and, if stdin is a file descriptor, that as its stdin."""
    aql = os.path.abspath((glob.glob("target/*/bin/aql") + glob.glob("../target/*/bin/aql"))[0])
    conf = ["--only-config-file", config] if config else ["--no-config-file"]
    argv = [aql] + conf + args + target
    pid, fd = pty.fork()

    if pid == 0:
        if stdin is not None:
            os.dup2(stdin, 0)
        os.execv(aql, argv)

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


def ignore_signals(signals):
    for sig in signals:
        signal.signal(sig, signal.SIG_IGN)


def run_with_terminal_stdin(
    args,
    typed: str,
    config=None,
    target=UNREACHABLE,
    interrupt=False,
    signum=signal.SIGINT,
    ignore=(),
    type_after_interrupt=False,
):
    """Runs aql in a new session with a pty as stdin but not as its controlling terminal, as under setsid
    or su -c, types into it or interrupts it with signum, and returns the output, what the terminal echoed
    and whether echo is on after. aql starts with the signals in ignore ignored, as under nohup."""
    aql = os.path.abspath((glob.glob("target/*/bin/aql") + glob.glob("../target/*/bin/aql"))[0])
    conf = ["--only-config-file", config] if config else ["--no-config-file"]
    master, slave = pty.openpty()
    proc = subprocess.Popen(
        [aql] + conf + args + target,
        stdin=slave,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
        preexec_fn=functools.partial(ignore_signals, ignore) if ignore else None,
    )
    echoed = b""

    try:
        # Type only once echo is off, or after a grace period if it never goes off.
        deadline = time.time() + 2
        while termios.tcgetattr(slave)[3] & termios.ECHO and time.time() < deadline:
            time.sleep(0.05)
        if interrupt:
            proc.send_signal(signum)
            if type_after_interrupt:
                time.sleep(0.5)
        if not interrupt or type_after_interrupt:
            os.write(master, typed.encode() + b"\n")

        while select.select([master], [], [], 1)[0]:
            try:
                chunk = os.read(master, 1024)
            except OSError:
                break
            if not chunk:
                break
            echoed += chunk

        out, err = proc.communicate(timeout=30)
        echo_after = bool(termios.tcgetattr(slave)[3] & termios.ECHO)
    finally:
        proc.kill()
        os.close(slave)
        os.close(master)

    return out.decode() + err.decode(), echoed.decode(errors="replace"), echo_after


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
            ("pw_blank_lines", (SECRET + "\n\n").encode()),
            ("pw_crlf_blank_lines", (SECRET + "\r\n\r\n").encode()),
            ("pw_cr", (SECRET + "\r").encode()),
            ("pw_more_lines", (SECRET + "\nmore").encode()),
            ("leading_newline", ("\n" + SECRET).encode()),
            ("nul", b"\0" + SECRET.encode() + b"\n"),
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
                ("env_no_name", "env:", {}, "env: requires a variable name"),
                ("env_unset", "env:" + VAR, {}, "environment variable {var} is not set or empty"),
                ("env_empty", "env:" + VAR, {VAR: ""}, "environment variable {var} is not set or empty"),
                ("env_b64_no_name", "env-b64:", {}, "env-b64: requires a variable name"),
                ("env_b64_unset", "env-b64:" + VAR, {}, "environment variable {var} is not set or empty"),
                ("env_b64_empty_var", "env-b64:" + VAR, {VAR: ""}, "environment variable {var} is not set or empty"),
                ("env_b64_invalid", "env-b64:" + VAR, {VAR: "not base64!"}, "invalid base64 in environment variable {var}"),
                ("env_b64_empty", "env-b64:" + VAR, {VAR: b64("\n")}, "password from environment variable {var} is empty"),
                ("env_b64_only_line_breaks", "env-b64:" + VAR, {VAR: "\r\n\n"}, "password from environment variable {var} is empty"),
                ("env_b64_nul", "env-b64:" + VAR, {VAR: b64("s3\0cret")}, "environment variable {var} decodes to a value containing a NUL byte"),
                ("b64_invalid", "b64:" + "c2VjcmV0!", {}, "invalid base64 in b64: value"),
                ("b64_no_payload", "b64:", {}, "password from b64: value is empty"),
                ("b64_only_line_breaks", "b64:\r\n\n", {}, "password from b64: value is empty"),
                ("b64_empty", "b64:" + b64("\n"), {}, "password from b64: value is empty"),
                ("b64_nul", "b64:" + b64("s3\0cret"), {}, "b64: value decodes to a value containing a NUL byte"),
                ("file_no_path", "file:", {}, "file: requires a path"),
                ("file_missing", "file:{missing}", {}, "cannot read file {missing}: No such file or directory"),
                ("file_directory", "file:{subdir}", {}, "cannot read file {subdir}: Is a directory"),
                ("file_empty", "file:{empty}", {}, "password from file {empty} is empty"),
                ("file_nul", "file:{nul}", {}, "file {nul} contains a NUL byte"),
                ("file_dev_zero", "file:/dev/zero", {}, "file /dev/zero contains a NUL byte"),
            ]
        ]
        + [
            ("password_file_newline_only", "--password", "file:{newline}", {}, "password from file {newline} is empty"),
            ("tls_keyfile_password_file_newline_only", "--tls-keyfile-password", "file:{newline}", {}, "password from first line of file {newline} is empty"),
        ]
    )
    def test_error(self, _, opt, value, env, expected):
        names = {
            "var": VAR,
            "missing": self.missing,
            "subdir": self.subdir,
            "empty": self.empty,
            "newline": self.newline,
            "nul": self.nul,
        }
        value = value.format(**names)
        rc, out = run(self.option_args(opt) + [opt + "=" + value], env)

        self.assertNotEqual(rc, 0)
        self.assertIn(opt + ": " + expected.format(**names) + "\n", out)
        self.assertNotIn(CONNECT_FAILED, out)
        if value.startswith("b64:") and value[4:].strip():
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
            ("file_first_line_blank_lines", "file:{pw_blank_lines}", {}),
            ("file_first_line_crlf_blank_lines", "file:{pw_crlf_blank_lines}", {}),
            ("file_first_line_cr", "file:{pw_cr}", {}),
            ("file_first_line_more_lines", "file:{pw_more_lines}", {}),
        ]
    )
    def test_tls_keyfile_password_resolves(self, _, value, env):
        value = value.format(
            pw=self.pw,
            pw_crlf=self.pw_crlf,
            pw_bare=self.pw_bare,
            pw_blank_lines=self.pw_blank_lines,
            pw_crlf_blank_lines=self.pw_crlf_blank_lines,
            pw_cr=self.pw_cr,
            pw_more_lines=self.pw_more_lines,
        )
        rc, out = run(["--tls-enable", "--tls-keyfile", self.key, "--tls-keyfile-password", value], env)

        self.assertNotEqual(rc, 0)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    def test_tls_keyfile_password_file_with_empty_first_line(self):
        rc, out = run(self.option_args("--tls-keyfile-password") + ["--tls-keyfile-password=file:" + self.leading_newline])

        self.assertNotEqual(rc, 0)
        self.assertIn("--tls-keyfile-password: password from first line of file {} is empty\n".format(self.leading_newline), out)
        self.assertNotIn(CONNECT_FAILED, out)

    def test_tls_keyfile_password_file_stops_at_first_line(self):
        with fifo(os.path.join(self.dir, "keyfile-fifo"), (SECRET + "\n").encode()) as path:
            rc, out = run(self.option_args("--tls-keyfile-password") + ["--tls-keyfile-password=file:" + path], timeout=10)

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

    def write_config(self, name, body):
        path = os.path.join(self.dir, name)
        with open(path, "w") as f:
            f.write(body)
        return path

    def option_config(self, opt):
        if opt == "--password":
            return 'user = "admin"\n'
        return 'tls-enable = true\ntls-keyfile = "{}"\n'.format(self.key)

    def test_password_from_config_file(self):
        config = self.write_config("astools.conf", '[cluster]\nuser = "admin"\npassword = "env:{}"\n'.format(VAR))

        rc, out = run([], {}, config)
        self.assertNotEqual(rc, 0)
        self.assertIn("password in config file: environment variable {} is not set or empty\n".format(VAR), out)

        rc, out = run([], {VAR: SECRET}, config)
        self.assertNotIn("password in config file:", out)
        self.assertIn(CONNECT_FAILED, out)

    def test_command_line_overrides_config_file_before_resolving(self):
        config = self.write_config("astools-override.conf", '[cluster]\nuser = "admin"\npassword = "env:{}"\n'.format(VAR))

        rc, out = run(["-P", "literal"], {}, config)

        self.assertNotIn("password in config file:", out)
        self.assertNotIn("--password:", out)
        self.assertIn(CONNECT_FAILED, out)

    @parameterized.expand([("password", "--password"), ("tls_keyfile_password", "--tls-keyfile-password")])
    def test_config_file_label(self, _, opt):
        key = opt[2:]
        config = self.write_config(
            key + "-label.conf", '[cluster]\n{}{} = "env:{}"\n'.format(self.option_config(opt), key, VAR)
        )

        rc, out = run([], {}, config)

        self.assertNotEqual(rc, 0)
        self.assertIn("{} in config file: environment variable {} is not set or empty\n".format(key, VAR), out)
        self.assertNotIn(opt + ":", out)
        self.assertNotIn(CONNECT_FAILED, out)

    @parameterized.expand([("password", "--password"), ("tls_keyfile_password", "--tls-keyfile-password")])
    def test_command_line_label_overrides_config_file(self, _, opt):
        key = opt[2:]
        config = self.write_config(
            key + "-override-label.conf", '[cluster]\n{}{} = "b64:{}"\n'.format(self.option_config(opt), key, b64(SECRET))
        )

        rc, out = run([opt + "=env:" + OTHER_VAR], {}, config)

        self.assertNotEqual(rc, 0)
        self.assertIn("{}: environment variable {} is not set or empty\n".format(opt, OTHER_VAR), out)
        self.assertNotIn("in config file", out)
        self.assertNotIn(CONNECT_FAILED, out)

    @parameterized.expand([("password", "--password"), ("tls_keyfile_password", "--tls-keyfile-password")])
    def test_instance_reads_its_cluster_section(self, _, opt):
        key = opt[2:]
        config = self.write_config(
            key + "-instance.conf",
            '[cluster]\n{opts}{key} = "b64:{good}"\n[cluster_x]\n{opts}{key} = "env:{var}"\n'.format(
                opts=self.option_config(opt), key=key, good=b64(SECRET), var=VAR
            ),
        )

        rc, out = run(["--instance", "x"], {}, config)
        self.assertNotEqual(rc, 0)
        self.assertIn("{} in config file: environment variable {} is not set or empty\n".format(key, VAR), out)
        self.assertNotIn(CONNECT_FAILED, out)

        rc, out = run([], {}, config)
        self.assertNotIn("in config file", out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    def test_tls_keyfile_password_from_config_file(self):
        config = self.write_config(
            "astools-tls.conf",
            '[cluster]\ntls-enable = true\ntls-keyfile = "{}"\ntls-keyfile-password = "b64:{}"\n'.format(
                self.key, b64(SECRET)
            ),
        )

        rc, out = run([], {}, config)

        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    def test_tls_keyfile_password_is_not_read_without_key_file(self):
        rc, out = run(["--tls-keyfile-password", "env:" + VAR])

        self.assertNotIn("--tls-keyfile-password:", out)
        self.assertIn(CONNECT_FAILED, out)

    def test_prompted_password_is_literal(self):
        out = run_with_prompt(["-U", "admin", "-P"], "env:" + VAR)

        self.assertIn("Enter Password: ", out)
        self.assertNotIn("--password:", out)
        self.assertIn(CONNECT_FAILED, out)

    @parameterized.expand(
        [
            ("env", "env:" + VAR),
            ("env_b64", "env-b64:" + VAR),
            ("b64", "b64:" + b64(SECRET)),
            ("file", "file:/nonexistent"),
        ]
    )
    def test_source_without_user_warns(self, _, value):
        rc, out = run(["-P", value])

        self.assertIn(NO_USER_WARNING.format("--password"), out)
        self.assertEqual(out.count("--password:"), 1)
        self.assertIn(CONNECT_FAILED, out)

    @parameterized.expand([("literal", ["-P", SECRET]), ("bare", ["-P"]), ("none", [])])
    def test_no_source_without_user_does_not_warn(self, _, args):
        rc, out = run(args)

        self.assertNotIn("ignored", out)
        self.assertIn(CONNECT_FAILED, out)

    def test_config_file_source_without_user_warns(self):
        config = self.write_config("astools-no-user.conf", '[cluster]\npassword = "env:{}"\n'.format(VAR))

        rc, out = run([], {}, config)

        self.assertIn(NO_USER_WARNING.format("password in config file"), out)
        self.assertEqual(out.count("password in config file:"), 1)
        self.assertIn(CONNECT_FAILED, out)

    @parameterized.expand([("upper", "PKI"), ("lower", "pki"), ("mixed", "Pki")])
    def test_pki_does_not_read_password(self, _, mode):
        rc, out = run(["-U", "admin", "-P", "env:" + VAR, "--auth", mode])

        self.assertNotIn("--password:", out)
        self.assertIn(PKI_PASSWORD_REJECTED, out)

    def test_pki_does_not_check_password_length(self):
        rc, out = run(["-U", "admin", "-P", "p" * 64, "--auth", "PKI"])

        self.assertNotIn("longer than", out)
        self.assertIn("Invalid password for user name `admin`", out)

    def test_pki_without_user_does_not_warn(self):
        rc, out = run(["-P", "env:" + VAR, "--auth", "PKI"])

        self.assertNotIn("ignored", out)
        self.assertNotIn("--password:", out)

    def test_pki_still_prompts(self):
        out = run_with_prompt(["-U", "admin", "--auth", "PKI", "-P"], "p" * 64)

        self.assertIn("Enter Password: ", out)
        self.assertNotIn("longer than", out)

    @parameterized.expand(
        [
            ("literal", SECRET),
            ("env", "env:" + VAR),
            ("b64", "b64:" + b64(SECRET)),
            ("file_first_line", "file:{pw_more_lines}"),
        ]
    )
    def test_prompted_tls_keyfile_password_resolves(self, _, typed):
        with mock.patch.dict(os.environ, {VAR: SECRET}):
            out = run_with_prompt(
                ["--tls-enable", "--tls-keyfile", self.key, "--tls-keyfile-password"],
                typed.format(pw_more_lines=self.pw_more_lines),
            )

        self.assertIn("Enter TLS-Keyfile Password: ", out)
        self.assertNotIn("--tls-keyfile-password:", out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    @parameterized.expand(
        [
            (source + "_" + case, source, value)
            for source in ["command_line", "config_file", "prompt"]
            for case, value in [("env", " env:" + VAR + " "), ("literal", "  " + SECRET + " \t")]
        ]
    )
    def test_padded_tls_keyfile_password_is_trimmed(self, name, source, value):
        opt = "--tls-keyfile-password"

        if source == "prompt":
            with mock.patch.dict(os.environ, {VAR: SECRET}):
                out = run_with_prompt(self.option_args(opt) + [opt], value)
        elif source == "config_file":
            config = self.write_config(
                name + ".conf",
                '[cluster]\n{}tls-keyfile-password = "{}"\n'.format(self.option_config(opt), value.replace("\t", "\\t")),
            )
            rc, out = run([], {VAR: SECRET}, config)
        else:
            rc, out = run(self.option_args(opt) + [opt + "=" + value], {VAR: SECRET})

        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    def test_tls_keyfile_password_leading_nbsp_is_literal(self):
        opt = "--tls-keyfile-password"
        rc, out = run(self.option_args(opt) + [opt + "=\u00a0env:" + VAR], {VAR: SECRET})

        self.assertNotIn(opt + ":", out)
        self.assertIn(TLS_KEY_FAILED, out)

    def test_piped_tls_keyfile_password_trims_every_whitespace_character(self):
        opt = "--tls-keyfile-password"
        rc, out = run(self.option_args(opt) + [opt], {VAR: SECRET}, piped="\v\f env:" + VAR + " \t\r\n")

        self.assertNotIn(opt + ":", out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    def test_piped_tls_keyfile_password_is_trimmed_and_resolved(self):
        opt = "--tls-keyfile-password"
        rc, out = run(self.option_args(opt) + [opt], {VAR: SECRET}, piped=" env:" + VAR + " \n")

        self.assertIn("Enter TLS-Keyfile Password: ", out)
        self.assertNotIn(opt + ":", out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    def test_piped_password_and_tls_keyfile_password_read_one_line_each(self):
        args = ["-U", "admin", "-P", "--tls-enable", "--tls-keyfile", self.key, "--tls-keyfile-password"]
        rc, out = run(args, {VAR: SECRET}, piped=SECRET + "\n env:" + VAR + " \n")

        self.assertNotIn("--tls-keyfile-password:", out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    def prompt_args(self, opt):
        return ["-U", "admin", "-P"] if opt == "--password" else self.option_args(opt) + [opt]

    @parameterized.expand(
        [
            ("password", "--password", "Enter Password: "),
            ("tls_keyfile_password", "--tls-keyfile-password", "Enter TLS-Keyfile Password: "),
        ]
    )
    def test_terminal_stdin_without_a_controlling_terminal_prompts(self, _, opt, prompt):
        """Under setsid or su -c, /dev/tty cannot be opened but stdin is still a terminal."""
        out, echoed, echo_after = run_with_terminal_stdin(self.prompt_args(opt), SECRET)

        self.assertIn(prompt, out)

    @parameterized.expand([("password", "--password"), ("tls_keyfile_password", "--tls-keyfile-password")])
    def test_terminal_stdin_without_a_controlling_terminal_does_not_echo(self, _, opt):
        out, echoed, echo_after = run_with_terminal_stdin(self.prompt_args(opt), SECRET)

        self.assertNotIn(SECRET, echoed)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    @parameterized.expand([("password", "--password"), ("tls_keyfile_password", "--tls-keyfile-password")])
    def test_terminal_stdin_without_a_controlling_terminal_restores_echo(self, _, opt):
        out, echoed, echo_after = run_with_terminal_stdin(self.prompt_args(opt), SECRET)

        self.assertTrue(echo_after)

    @parameterized.expand([("password", "--password"), ("tls_keyfile_password", "--tls-keyfile-password")])
    def test_terminal_stdin_without_a_controlling_terminal_restores_echo_when_interrupted(self, _, opt):
        out, echoed, echo_after = run_with_terminal_stdin(self.prompt_args(opt), SECRET, interrupt=True)

        self.assertTrue(echo_after)

    @parameterized.expand(
        [
            (opt.strip("-").replace("-", "_") + "_" + sig.name.lower(), opt, sig)
            for opt in ["--password", "--tls-keyfile-password"]
            for sig in [signal.SIGHUP, signal.SIGQUIT]
        ]
    )
    def test_terminal_stdin_without_a_controlling_terminal_keeps_reading_on_an_ignored_signal(self, _, opt, sig):
        """A signal ignored on entry, as under nohup, stays ignored and does not end the read. aql sets its own
        SIGINT and SIGTERM handlers at startup, so only SIGHUP and SIGQUIT can still be ignored at the prompt."""
        out, echoed, echo_after = run_with_terminal_stdin(
            self.prompt_args(opt), SECRET, interrupt=True, signum=sig, ignore=(sig,), type_after_interrupt=True
        )

        self.assertIn(CONNECT_FAILED, out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertNotIn(SECRET, echoed)


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

    @parameterized.expand(
        [
            ("unknown_prefix", "pass:word"),
            ("uppercase_env", "ENV:" + VAR),
            ("uppercase_b64", "B64:" + b64(SECRET)),
        ]
    )
    def test_unrecognized_prefix_is_literal(self, _, value):
        typed = capture_login(["-U", "admin", "-P", value], {VAR: SECRET})

        self.assertNotEqual(typed, literal_login(SECRET))
        self.assertEqual(typed, capture_login(["-U", "admin", "-P", "env:" + VAR], {VAR: value}))

    def test_resolved_value_is_sent_as_is(self):
        env = {VAR: "env:" + OTHER_VAR, OTHER_VAR: SECRET}
        via_env = capture_login(["-U", "admin", "-P", "env:" + VAR], env)

        self.assertNotEqual(via_env, literal_login(SECRET))
        self.assertEqual(via_env, capture_login(["-U", "admin", "-P", "b64:" + b64("env:" + OTHER_VAR)], env))

    def test_typed_value_is_sent_as_is(self):
        typed = capture_login(["-U", "admin", "-P"], {VAR: SECRET}, typed="env:" + VAR)

        self.assertNotEqual(typed, literal_login(SECRET))
        self.assertEqual(typed, capture_login(["-U", "admin", "-P", "b64:" + b64("env:" + VAR)]))

    def test_typed_value_is_not_trimmed(self):
        padded = " " + SECRET + " "

        self.assertEqual(capture_login(["-U", "admin", "-P"], typed=padded), literal_login(padded))

    def test_piped_value_is_trimmed(self):
        self.assertEqual(capture_login(["-U", "admin", "-P"], piped=" " + SECRET + " \r\n"), literal_login(SECRET))

    def test_piped_value_trims_every_whitespace_character(self):
        piped = "\v\f " + SECRET + " \t\r\n"

        self.assertEqual(capture_login(["-U", "admin", "-P"], piped=piped), literal_login(SECRET))

    def test_piped_value_keeps_other_spaces(self):
        padded = " " + SECRET + " "

        self.assertEqual(capture_login(["-U", "admin", "-P"], piped=padded + "\n"), literal_login(padded))

    def test_piped_value_prompts_on_stderr(self):
        """docker exec -i, kubectl exec -i and ssh without -t have no terminal, but a person may be typing."""
        out = utils.run_aql(
            ["--no-config-file", "-U", "admin", "-P"] + UNREACHABLE,
            piped=SECRET + "\n",
        )

        self.assertIn("Enter Password: ", out.stderr.decode())
        self.assertNotIn("Enter Password", out.stdout.decode())

    def test_piped_value_leaves_the_rest_of_stdin(self):
        """With no terminal, only the password line is read, so the commands piped after it still run."""
        r, w = os.pipe()
        os.write(w, (SECRET + "\nshow sets\n").encode())
        os.close(w)
        try:
            piped = capture_login(["-U", "admin", "-P"], stdin=r)
            left = os.read(r, 1024)
        finally:
            os.close(r)

        self.assertEqual(piped, literal_login(SECRET))
        self.assertEqual(left, b"show sets\n")

    def test_terminal_stdin_without_a_controlling_terminal_is_not_trimmed(self):
        padded = " " + SECRET + " "

        self.assertEqual(
            capture_login(["-U", "admin", "-P"], typed=padded, terminal_stdin=True), literal_login(padded)
        )

    def test_terminal_stdin_without_a_controlling_terminal_ignored_signal_keeps_typed_value(self):
        typed = capture_login(
            ["-U", "admin", "-P"],
            typed=SECRET,
            terminal_stdin=True,
            interrupt=True,
            signum=signal.SIGHUP,
            ignore=(signal.SIGHUP,),
            type_after_interrupt=True,
        )

        self.assertEqual(typed, literal_login(SECRET))

    def test_piped_value_is_sent_as_is(self):
        piped = capture_login(["-U", "admin", "-P"], {VAR: SECRET}, piped="env:" + VAR + "\n")

        self.assertNotEqual(piped, literal_login(SECRET))
        self.assertEqual(piped, capture_login(["-U", "admin", "-P", "b64:" + b64("env:" + VAR)]))

    def test_terminal_is_used_before_piped_stdin(self):
        commands = b"show sets\nshow bins\n"
        r, w = os.pipe()
        os.write(w, commands)
        os.close(w)
        try:
            typed = capture_login(["-U", "admin", "-P"], typed=SECRET, stdin=r)
            left = os.read(r, 1024)
        finally:
            os.close(r)

        self.assertEqual(typed, literal_login(SECRET))
        self.assertEqual(left, commands)

    def test_config_file(self):
        config = os.path.join(self.dir, "astools.conf")
        with open(config, "w") as f:
            f.write('[cluster]\nuser = "admin"\npassword = "env:{}"\n'.format(VAR))

        self.assertEqual(capture_login([], {VAR: SECRET}, config), literal_login(SECRET))

    def test_file_keeps_every_line(self):
        path = os.path.join(self.dir, "pw")
        with open(path, "w") as f:
            f.write(SECRET + "\nmore\n")

        self.assertEqual(capture_login(["-U", "admin", "-P", "file:" + path]), literal_login(SECRET + "\nmore"))


class PasswordLengthTest(unittest.TestCase):
    """Every --password value must fit the client's 63-byte limit and every --tls-keyfile-password value 1024 bytes."""

    LONGEST = "p" * 63
    TOO_LONG = "p" * 64
    KEY_LONGEST = "k" * 1024
    KEY_TOO_LONG = "k" * 1025
    FORMS = ["literal", "env", "env_b64", "env_b64_newline", "b64", "b64_newline", "file", "file_newline", "file_crlf"]

    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp()
        cls.addClassCleanup(shutil.rmtree, cls.dir)
        cls.key = os.path.join(cls.dir, "long-key.pem")
        make_key(cls.key, cls.KEY_LONGEST)

    def source(self, form: str, password: str):
        """Returns the option value, the environment and how the error names the source."""
        if form == "literal":
            return password, {}, "password"

        if form.startswith("file"):
            path = os.path.join(self.dir, "{}-{}".format(form, len(password)))
            ending = {"file": "", "file_newline": "\n", "file_crlf": "\r\n"}[form]
            with open(path, "w", newline="") as f:
                f.write(password + ending)
            return "file:" + path, {}, "value from file " + path

        ending = "\n" if form.endswith("_newline") else ""
        if form == "env":
            return "env:" + VAR, {VAR: password}, "value from environment variable " + VAR
        if form.startswith("env_b64"):
            return "env-b64:" + VAR, {VAR: b64(password + ending)}, "decoded value from environment variable " + VAR
        return "b64:" + b64(password + ending), {}, "decoded b64: value"

    @parameterized.expand([(form,) for form in FORMS])
    def test_longest_value_is_sent(self, form):
        arg, env, _ = self.source(form, self.LONGEST)
        self.assertEqual(capture_login(["-U", "admin", "-P", arg], env), literal_login(self.LONGEST))

    @parameterized.expand([(form,) for form in FORMS])
    def test_too_long_value_is_rejected(self, form):
        arg, env, described = self.source(form, self.TOO_LONG)
        rc, out = run(["-U", "admin", "-P", arg], env)

        self.assertNotEqual(rc, 0)
        self.assertIn("--password: {} is longer than 63 bytes\n".format(described), out)
        self.assertNotIn(CONNECT_FAILED, out)
        self.assertNotIn("Invalid password", out)
        self.assertNotIn(self.TOO_LONG, out)
        self.assertNotIn(b64(self.TOO_LONG), out)

    def test_typed_longest_value_is_sent(self):
        self.assertEqual(capture_login(["-U", "admin", "-P"], typed=self.LONGEST), literal_login(self.LONGEST))

    def test_typed_too_long_value_is_rejected(self):
        out = run_with_prompt(["-U", "admin", "-P"], self.TOO_LONG)

        self.assertIn("Enter Password: ", out)
        self.assertIn("--password: password is longer than 63 bytes", out)
        self.assertNotIn(CONNECT_FAILED, out)
        self.assertNotIn("Invalid password", out)

    def test_piped_padded_longest_value_filling_the_bound_is_sent(self):
        """A last line with no newline that exactly fills the read bound is complete, not cut off."""
        piped = "   " + self.LONGEST

        self.assertEqual(capture_login(["-U", "admin", "-P"], piped=piped), literal_login(self.LONGEST))

    def test_piped_cut_off_value_is_rejected(self):
        rc, out = run(["-U", "admin", "-P"], piped=" " * 10 + "x" * 100)

        self.assertNotEqual(rc, 0)
        self.assertIn("Enter Password: ", out)
        self.assertIn("--password: password is longer than 63 bytes\n", out)
        self.assertNotIn(CONNECT_FAILED, out)

    @parameterized.expand(
        [
            ("cut_off", "admin\0" + "0" * 200),
            ("whole_line", "ad\0min\n"),
            ("only_nul", "\0" * 200),
        ]
    )
    def test_piped_value_with_a_nul_is_rejected(self, _, piped):
        """A NUL ended the password early, which let a cut-off line past the length check and ran the
        rest of the line as an aql command."""
        rc, out = run(["-U", "admin", "-P"], piped=piped)

        self.assertNotEqual(rc, 0)
        self.assertIn("--password: password from stdin contains a NUL byte\n", out)
        self.assertNotIn(CONNECT_FAILED, out)

    def test_config_file_too_long_value_is_rejected(self):
        config = os.path.join(self.dir, "astools-long.conf")
        with open(config, "w") as f:
            f.write('[cluster]\nuser = "admin"\npassword = "{}"\n'.format(self.TOO_LONG))

        rc, out = run([], {}, config)

        self.assertNotEqual(rc, 0)
        self.assertIn("password in config file: password is longer than 63 bytes\n", out)
        self.assertNotIn(CONNECT_FAILED, out)

    def test_endless_file_is_rejected(self):
        with fifo(os.path.join(self.dir, "endless"), b"p" * 4096, endless=True) as path:
            rc, out = run(["-U", "admin", "-P", "file:" + path], timeout=10)

        self.assertNotEqual(rc, 0)
        self.assertIn("--password: value from file {} is longer than 63 bytes\n".format(path), out)
        self.assertNotIn(CONNECT_FAILED, out)

    def key_args(self, *value):
        return ["--tls-enable", "--tls-keyfile", self.key, "--tls-keyfile-password"] + list(value)

    def assert_key_loaded(self, out):
        self.assertNotIn("longer than", out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    def assert_key_password_rejected(self, out, described, label="--tls-keyfile-password", end="\n"):
        self.assertIn("{}: {} is longer than 1024 bytes{}".format(label, described, end), out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertNotIn(CONNECT_FAILED, out)
        self.assertNotIn(self.KEY_TOO_LONG, out)

    @parameterized.expand([(form,) for form in FORMS])
    def test_tls_keyfile_longest_value_loads_key(self, form):
        arg, env, _ = self.source(form, self.KEY_LONGEST)
        rc, out = run(self.key_args(arg), env)

        self.assert_key_loaded(out)

    @parameterized.expand([(form,) for form in FORMS])
    def test_tls_keyfile_too_long_value_is_rejected(self, form):
        arg, env, described = self.source(form, self.KEY_TOO_LONG)
        rc, out = run(self.key_args(arg), env)

        self.assertNotEqual(rc, 0)
        self.assert_key_password_rejected(out, described)

    @parameterized.expand([("longest", KEY_LONGEST, False), ("too_long", KEY_TOO_LONG, True)])
    def test_tls_keyfile_password_file_first_line_limit(self, _, password, rejected):
        path = os.path.join(self.dir, "keyfile-pw-lines-{}".format(len(password)))
        with open(path, "w") as f:
            f.write(password + "\nmore\n")

        rc, out = run(self.key_args("file:" + path))

        self.assertNotEqual(rc, 0)
        if rejected:
            self.assert_key_password_rejected(out, "value from file " + path)
        else:
            self.assert_key_loaded(out)

    def test_tls_keyfile_password_endless_file_is_rejected(self):
        with fifo(os.path.join(self.dir, "keyfile-endless"), b"k" * 4096, endless=True) as path:
            rc, out = run(self.key_args("file:" + path), timeout=10)

        self.assertNotEqual(rc, 0)
        self.assertIn("--tls-keyfile-password: value from file {} is longer than 1024 bytes\n".format(path), out)

    def test_tls_keyfile_password_piped_padded_longest_value_filling_the_bound_loads_key(self):
        rc, out = run(self.key_args(), piped="   " + self.KEY_LONGEST)

        self.assert_key_loaded(out)

    def test_tls_keyfile_password_piped_cut_off_value_is_rejected(self):
        rc, out = run(self.key_args(), piped=" " * 10 + "x" * 2000)

        self.assertNotEqual(rc, 0)
        self.assertIn("Enter TLS-Keyfile Password: ", out)
        self.assert_key_password_rejected(out, "password")

    @parameterized.expand([("cut_off", "k\0" + "k" * 2000), ("whole_line", "k\0k\n")])
    def test_tls_keyfile_password_piped_value_with_a_nul_is_rejected(self, _, piped):
        rc, out = run(self.key_args(), piped=piped)

        self.assertNotEqual(rc, 0)
        self.assertIn("--tls-keyfile-password: password from stdin contains a NUL byte\n", out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertNotIn(CONNECT_FAILED, out)

    def test_tls_keyfile_password_config_file_too_long_value_is_rejected(self):
        config = os.path.join(self.dir, "astools-long-tls.conf")
        with open(config, "w") as f:
            f.write(
                '[cluster]\ntls-enable = true\ntls-keyfile = "{}"\ntls-keyfile-password = "{}"\n'.format(
                    self.key, self.KEY_TOO_LONG
                )
            )

        rc, out = run([], {}, config)

        self.assertNotEqual(rc, 0)
        self.assert_key_password_rejected(out, "password", "tls-keyfile-password in config file")

    def test_typed_tls_keyfile_source_too_long_is_rejected(self):
        with mock.patch.dict(os.environ, {VAR: self.KEY_TOO_LONG}):
            out = run_with_prompt(self.key_args(), "env:" + VAR)

        self.assertIn("Enter TLS-Keyfile Password: ", out)
        self.assert_key_password_rejected(out, "value from environment variable " + VAR, end="")

    @unittest.skipIf(sys.platform == "darwin", MACOS_GETPASS)
    def test_typed_tls_keyfile_longest_value_loads_key(self):
        out = run_with_prompt(self.key_args(), self.KEY_LONGEST)

        self.assertIn("Enter TLS-Keyfile Password: ", out)
        self.assert_key_loaded(out)

    @unittest.skipIf(sys.platform == "darwin", MACOS_GETPASS)
    def test_typed_tls_keyfile_too_long_value_is_rejected(self):
        out = run_with_prompt(self.key_args(), self.KEY_TOO_LONG)

        self.assertIn("Enter TLS-Keyfile Password: ", out)
        self.assert_key_password_rejected(out, "password", end="")


class HelpTest(unittest.TestCase):
    @parameterized.expand(
        [
            ("password", " -P, --password\n", PASSWORD_HELP),
            ("tls_keyfile_password", " --tls-keyfile-password=TLS_KEYFILE_PASSWORD\n", TLS_KEYFILE_PASSWORD_HELP),
        ]
    )
    def test_help(self, _, header, body):
        out = utils.run_aql(["--help"]).stdout.decode()

        self.assertIn(header + "".join(" " * 22 + line + "\n" for line in body), out)


if __name__ == "__main__":
    unittest.main()
