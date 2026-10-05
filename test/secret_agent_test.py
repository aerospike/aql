import base64
import glob
import json
import os
import re
import shutil
import socket
import ssl
import struct
import subprocess
import tempfile
import threading
import time
import unittest
import uuid

from parameterized import parameterized

import utils
from password_source_test import (
    CONNECT_FAILED,
    PARSED_LOOKING_SECRET,
    SECRET,
    TLS_KEY_FAILED,
    capture_login,
    literal_login,
    make_key,
    run,
    run_with_prompt,
)

AGENT_IMAGE = "aerospike/aerospike-secret-agent"
AGENT_TAG = "1.1.0"
AGENT_PORT = "3005/tcp"
AGENT_DIR = "/opt/work"
AGENT_MAGIC = 0x51DEC1CC

SECRETS = {
    "pw": SECRET,
    "wrong": "wrong",
    "env_looking": PARSED_LOOKING_SECRET,
    "trailing_newline": SECRET + "\n",
    "with_nul": "ab\0xyzzy",
    "empty": "",
}


def b64(value: str) -> str:
    return base64.b64encode(value.encode()).decode()


def closed_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def fake_agent(test, response: bytes | None) -> str:
    """Answers one request with response, or never answers when response is None."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    done = threading.Event()

    def serve():
        try:
            conn, _ = srv.accept()
        except OSError:
            return
        with conn:
            conn.recv(4096)
            if response is not None:
                conn.sendall(struct.pack(">II", AGENT_MAGIC, len(response)) + response)
            done.wait(10)

    def stop():
        done.set()
        try:
            srv.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        srv.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    test.addCleanup(stop)
    return str(srv.getsockname()[1])


def write(path: str, content: str, mode: int = 0o644):
    with open(path, "w") as f:
        f.write(content)
    os.chmod(path, mode)


def make_cert(dir: str):
    key = os.path.join(dir, "agent-key.pem")
    cert = os.path.join(dir, "agent-cert.pem")
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", key, "-out", cert, "-days", "2", "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    os.chmod(key, 0o644)
    return cert, key


def wait_for_agent(container, port: int, cafile: str | None, timeout: float = 60):
    # A bare TCP connect is not enough: the docker port proxy accepts before the agent listens.
    body = json.dumps({"Resource": "aql", "SecretKey": "probe"}).encode()
    request = struct.pack(">II", AGENT_MAGIC, len(body)) + body
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=2) as raw:
                conn = raw
                if cafile:
                    ctx = ssl.create_default_context(cafile=cafile)
                    conn = ctx.wrap_socket(raw, server_hostname="localhost")
                conn.sendall(request)
                header = conn.recv(8)
                if len(header) == 8 and struct.unpack(">I", header[:4])[0] == AGENT_MAGIC:
                    return
        except (OSError, ssl.SSLError):
            pass
        time.sleep(0.2)

    raise RuntimeError(
        "secret agent not ready on port {}:\n{}".format(port, container.logs().decode(errors="replace"))
    )


def start_agent(test_cls, dir: str, name: str, tls: tuple[str, str] | None) -> int:
    tls_conf = ""
    if tls:
        tls_conf = '    tls:\n      cert-file: "{0}/{1}"\n      key-file: "{0}/{2}"\n'.format(
            AGENT_DIR, os.path.basename(tls[0]), os.path.basename(tls[1])
        )

    config = "{}.yaml".format(name)
    write(
        os.path.join(dir, config),
        "service:\n"
        "  tcp:\n"
        "    endpoint: 0.0.0.0:3005\n"
        "{tls}"
        "secret-manager:\n"
        "  file:\n"
        "    resources:\n"
        '      aql: "{dir}/secrets.json"\n'
        '      other: "{dir}/other.json"\n'
        "log:\n"
        "  level: debug\n".format(tls=tls_conf, dir=AGENT_DIR),
    )

    client = utils._get_docker_client()
    container = client.containers.run(
        "{}:{}".format(AGENT_IMAGE, AGENT_TAG),
        command="--config-file {}/{}".format(AGENT_DIR, config),
        name="aql-test-secret-agent-{}-{}".format(name, uuid.uuid4().hex[:12]),
        ports={AGENT_PORT: ("127.0.0.1", None)},
        volumes={dir: {"bind": AGENT_DIR, "mode": "ro"}},
        platform="linux/amd64",
        detach=True,
    )
    test_cls.addClassCleanup(container.remove, force=True)

    deadline = time.time() + 30
    while True:
        container.reload()
        bindings = (container.attrs["NetworkSettings"]["Ports"] or {}).get(AGENT_PORT)
        if bindings:
            port = int(bindings[0]["HostPort"])
            break
        if time.time() > deadline:
            raise RuntimeError("secret agent container {} has no port binding".format(container.name))
        time.sleep(0.2)

    wait_for_agent(container, port, tls[0] if tls else None)
    return port


class SecretAgentOptionTest(unittest.TestCase):
    """No secret agent and no server needed."""

    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp()
        cls.addClassCleanup(shutil.rmtree, cls.dir)
        cls.closed = str(closed_port())
        cls.missing = os.path.join(cls.dir, "missing.pem")

    def config(self, content: str) -> str:
        path = os.path.join(self.dir, "astools-{}.conf".format(uuid.uuid4().hex))
        write(path, content)
        return path

    @parameterized.expand(
        [
            ("password", ["-U", "admin", "-P", "secrets:aql:pw"], "--password"),
            (
                "tls_keyfile_password",
                ["--tls-enable", "--tls-keyfile", "/dev/null", "--tls-keyfile-password", "secrets:aql:pw"],
                "--tls-keyfile-password",
            ),
        ]
    )
    def test_agent_unreachable(self, _, args, opt):
        rc, out = run(args + ["--sa-port", self.closed])

        self.assertNotEqual(rc, 0)
        self.assertIn(opt + ": secret agent request for secrets:aql:pw failed: connection or protocol error\n", out)
        self.assertIn("secret-agent: ERR: connect failed", out)
        self.assertNotIn(CONNECT_FAILED, out)

    @parameterized.expand(
        [
            ("bracketed_ipv6", ["--sa-address", "[::1]", "--sa-port", "{closed}"]),
            ("bracketed_ipv6_port", ["--sa-address", "[::1]:{closed}"]),
            ("bare_ipv6", ["--sa-address", "::1", "--sa-port", "{closed}"]),
            ("host_port", ["--sa-address", "localhost:{closed}"]),
        ]
    )
    def test_address_forms_parse(self, _, sa_args):
        sa_args = [a.format(closed=self.closed) for a in sa_args]
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw"] + sa_args)

        self.assertNotEqual(rc, 0)
        self.assertNotIn("invalid value", out)
        self.assertIn("--password: secret agent request for secrets:aql:pw failed: connection or protocol error\n", out)

    @parameterized.expand(
        [
            ("timeout_text", "--sa-timeout", "abc", "--sa-timeout: invalid value abc, expected an integer from 1 to 2147483647"),
            ("timeout_zero", "--sa-timeout", "0", "--sa-timeout: invalid value 0, expected an integer from 1 to 2147483647"),
            ("timeout_negative", "--sa-timeout", "-1", "--sa-timeout: invalid value -1, expected an integer from 1 to 2147483647"),
            ("timeout_too_big", "--sa-timeout", "2147483648", "--sa-timeout: invalid value 2147483648, expected an integer from 1 to 2147483647"),
            ("port_zero", "--sa-port", "0", "--sa-port: invalid value 0"),
            ("port_too_big", "--sa-port", "65536", "--sa-port: invalid value 65536"),
            ("port_text", "--sa-port", "http", "--sa-port: invalid value http"),
            ("address_empty", "--sa-address", "", "--sa-address: invalid value "),
            ("address_no_host", "--sa-address", ":3005", "--sa-address: invalid value :3005"),
            ("address_empty_port", "--sa-address", "host:", "--sa-address: invalid value host:"),
            ("address_bad_port", "--sa-address", "host:99999", "--sa-address: invalid value host:99999"),
            ("address_unclosed_bracket", "--sa-address", "[::1", "--sa-address: invalid value [::1"),
            ("address_empty_brackets", "--sa-address", "[]:3005", "--sa-address: invalid value []:3005"),
            ("address_junk_after_bracket", "--sa-address", "[::1]x", "--sa-address: invalid value [::1]x"),
            ("address_extra_colon", "--sa-address", "host:3005:x", "--sa-address: invalid value host:3005:x"),
            ("address_ipv4_extra_colon", "--sa-address", "10.0.0.1:3005:1", "--sa-address: invalid value 10.0.0.1:3005:1"),
            ("address_bad_ipv6", "--sa-address", "1::2::3", "--sa-address: invalid value 1::2::3"),
            ("address_bad_ipv6_zone", "--sa-address", "1::2::3%lo0", "--sa-address: invalid value 1::2::3%lo0"),
            ("address_empty_zone", "--sa-address", "fe80::1%", "--sa-address: invalid value fe80::1%"),
            ("address_bracketed_not_ipv6", "--sa-address", "[host:3005:x]", "--sa-address: invalid value [host:3005:x]"),
            ("address_bracketed_not_ipv6_port", "--sa-address", "[host:3005:x]:3005", "--sa-address: invalid value [host:3005:x]:3005"),
            ("address_bracketed_one_colon", "--sa-address", "[a:b]", "--sa-address: invalid value [a:b]"),
            ("address_bracketed_empty_zone", "--sa-address", "[fe80::1%]:3005", "--sa-address: invalid value [fe80::1%]:3005"),
        ]
    )
    def test_bad_option(self, _, opt, value, expected):
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw", opt, value])

        self.assertNotEqual(rc, 0)
        self.assertIn(expected + "\n", out)
        self.assertNotIn("secret-agent:", out)
        self.assertNotIn(CONNECT_FAILED, out)

    # The library default is 1000 ms. Without the setting both cases take about 1 s and fail.
    @parameterized.expand(
        [
            ("command_line_short", False, 200, None, 0.8),
            ("command_line_long", False, 2000, 1.9, None),
            ("config_file_short", True, 200, None, 0.8),
            ("config_file_long", True, 2000, 1.9, None),
        ]
    )
    def test_timeout(self, _, from_config, timeout_ms, at_least, under):
        port = fake_agent(self, None)
        args = ["-U", "admin", "-P", "secrets:aql:pw"]
        config = None

        if from_config:
            config = self.config("[secret-agent]\nsa-port = {}\nsa-timeout = {}\n".format(port, timeout_ms))
        else:
            args += ["--sa-port", port, "--sa-timeout", str(timeout_ms)]

        start = time.monotonic()
        rc, out = run(args, config=config)
        elapsed = time.monotonic() - start

        self.assertNotEqual(rc, 0)
        self.assertIn("secret-agent: ERR: socket poll timed out\n", out)
        self.assertIn("--password: secret agent request for secrets:aql:pw failed: timed out\n", out)
        self.assertNotIn(CONNECT_FAILED, out)
        if at_least is not None:
            self.assertGreaterEqual(elapsed, at_least)
        if under is not None:
            self.assertLess(elapsed, under)

    def test_malformed_response_is_not_echoed(self):
        port = fake_agent(self, '{{"SecretValue":"{}'.format(b64(SECRET)).encode())
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw", "--sa-port", port])

        self.assertNotEqual(rc, 0)
        self.assertIn("secret-agent: ERR: failed to parse response JSON line\n", out)
        self.assertIn("--password: secret agent request for secrets:aql:pw failed: bad request\n", out)
        self.assertNotIn(b64(SECRET)[:4], out)
        self.assertNotIn(SECRET, out)

    # A literal password never contacts the agent, so reaching the server proves the address parsed.
    @parameterized.expand(
        [
            ("zone", "fe80::1%lo0"),
            ("numeric_zone", "fe80::1%1"),
            ("bracketed_zone", "[fe80::1%lo0]"),
            ("bracketed_zone_port", "[fe80::1%lo0]:3005"),
            ("bracketed_port", "[::1]:3005"),
            ("ipv4_mapped", "::ffff:127.0.0.1"),
        ]
    )
    def test_ipv6_address_accepted(self, _, value):
        rc, out = run(["-U", "admin", "-P", "literal", "--sa-address", value])
        self.assertNotIn("invalid value", out)
        self.assertIn(CONNECT_FAILED, out)

        config = self.config('[secret-agent]\nsa-address = "{}"\n'.format(value))
        rc, out = run(["-U", "admin", "-P", "literal"], config=config)
        self.assertNotIn("Invalid parameter value", out)
        self.assertIn(CONNECT_FAILED, out)

    def test_unreadable_cafile(self):
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw", "--sa-cafile", self.missing, "--sa-port", self.closed])

        self.assertNotEqual(rc, 0)
        self.assertIn("--sa-cafile: cannot read file {}: No such file or directory\n".format(self.missing), out)
        self.assertNotIn("secret-agent:", out)
        self.assertNotIn(CONNECT_FAILED, out)

    def test_cafile_only_read_when_secrets_used(self):
        rc, out = run(["-U", "admin", "-P", "literal", "--sa-cafile", self.missing])

        self.assertNotIn("--sa-cafile", out)
        self.assertIn(CONNECT_FAILED, out)

    def test_prompted_password_is_literal(self):
        out = run_with_prompt(["-U", "admin", "-P", "--sa-port", self.closed], "secrets:aql:pw")

        self.assertIn("Enter Password: ", out)
        self.assertNotIn("secret-agent:", out)
        self.assertNotIn("--password:", out)
        self.assertIn(CONNECT_FAILED, out)

    @parameterized.expand(
        [
            ("timeout_text", 'sa-timeout = "1000"', "sa-timeout"),
            ("timeout_zero", "sa-timeout = 0", "sa-timeout"),
            ("timeout_negative", "sa-timeout = -1", "sa-timeout"),
            ("timeout_too_big", "sa-timeout = 2147483648", "sa-timeout"),
            ("port_zero", "sa-port = 0", "sa-port"),
            ("port_too_big", 'sa-port = "65536"', "sa-port"),
            ("port_text", 'sa-port = "http"', "sa-port"),
            ("port_bool", "sa-port = true", "sa-port"),
            ("address_int", "sa-address = 1", "sa-address"),
            ("address_bad", 'sa-address = "[::1"', "sa-address"),
            ("address_bad_port", 'sa-address = "host:0"', "sa-address"),
            ("address_extra_colon", 'sa-address = "host:3005:x"', "sa-address"),
            ("address_bad_ipv6_zone", 'sa-address = "1::2::3%lo0"', "sa-address"),
            ("address_bracketed_not_ipv6", 'sa-address = "[host:3005:x]:3005"', "sa-address"),
            ("cafile_int", "sa-cafile = 1", "sa-cafile"),
        ]
    )
    def test_bad_config_value(self, _, line, key):
        config = self.config("[secret-agent]\n{}\n".format(line))
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw"], config=config)

        self.assertNotEqual(rc, 0)
        self.assertIn("Invalid parameter value for `{}` in `secret-agent` section.".format(key), out)
        self.assertNotIn(CONNECT_FAILED, out)

    def test_bad_config_value_names_instance_section(self):
        config = self.config("[secret-agent]\n[secret-agent_a]\nsa-port = 0\n")
        rc, out = run(["--instance", "a", "-U", "admin", "-P", "secrets:aql:pw"], config=config)

        self.assertNotEqual(rc, 0)
        self.assertIn("Invalid parameter value for `sa-port` in `secret-agent_a` section.", out)

    def test_unknown_config_key(self):
        config = self.config('[secret-agent]\nsa-host = "127.0.0.1"\n')
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw"], config=config)

        self.assertNotEqual(rc, 0)
        self.assertIn("Unknown parameter `sa-host` in `secret-agent` section.", out)

    def test_config_unreachable_agent(self):
        config = self.config(
            '[cluster]\nuser = "admin"\npassword = "secrets:aql:pw"\n'
            '[secret-agent]\nsa-address = "127.0.0.1"\nsa-port = {}\nsa-timeout = 500\n'.format(self.closed)
        )
        rc, out = run([], config=config)

        self.assertNotEqual(rc, 0)
        self.assertIn("--password: secret agent request for secrets:aql:pw failed: connection or protocol error\n", out)
        self.assertNotIn(CONNECT_FAILED, out)

    def test_config_unreadable_cafile(self):
        config = self.config(
            '[cluster]\nuser = "admin"\npassword = "secrets:aql:pw"\n'
            '[secret-agent]\nsa-port = "{}"\nsa-cafile = "{}"\n'.format(self.closed, self.missing)
        )
        rc, out = run([], config=config)

        self.assertNotEqual(rc, 0)
        self.assertIn("--sa-cafile: cannot read file {}: No such file or directory\n".format(self.missing), out)

    def test_config_values_are_not_resolved(self):
        config = self.config('[secret-agent]\nsa-cafile = "env:AQL_TEST_NEVER_SET"\nsa-port = {}\n'.format(self.closed))
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw"], config=config)

        self.assertNotEqual(rc, 0)
        self.assertNotIn("environment variable", out)
        self.assertIn("--sa-cafile: cannot read file env:AQL_TEST_NEVER_SET: No such file or directory\n", out)

    def test_help(self):
        out = utils.run_aql(["-O"]).stdout.decode()

        for text in [
            "[secret-agent]",
            " --sa-address=HOST",
            "Default: 127.0.0.1:3005",
            " --sa-port=PORT",
            " --sa-timeout=ms",
            "It does not apply to the TCP connect or the name lookup.",
            " --sa-cafile=path",
            "the agent's certificate is not",
            "(cluster aql secret-agent include)",
        ]:
            self.assertIn(text, out)
        self.assertEqual(out.count("5) Aerospike Secret Agent: 'secrets:<resource>:<key>'"), 2)


LIB_SRC = utils.absolute_path("..", "modules", "secret-agent-client", "src")
MAIN_C = utils.absolute_path("..", "src", "main", "main.c")
C_STRING = r'"(?:[^"\\]|\\.)*"'

# Library log lines aql prints with their arguments, and the argument expressions reviewed as safe.
REVIEWED_LOG_ARGS = {
    "ERR: failed to lookup address: %s": "addr",
    "ERR: connect failed: %d, errno: %d": "sock_fd, errno",
    "ERR: response: %.*s": "(int)payload_len, payload_str",
    "ERR: SSL_connect failed: %s": "errbuf",
    "ERR: SSL_connect I/O error: %s": "errbuf",
}


def c_unescape(literal: str) -> str:
    return literal[1:-1].encode().decode("unicode_escape")


def strip_comments_and_chars(src: str, where: str) -> str:
    """Drops C comments (keeping their newlines) and turns char literals into 0; string literals stay."""
    out, i, n = [], 0, len(src)
    while i < n:
        c = src[i]
        if src.startswith("//", i):
            end = src.find("\n", i)
            i = n if end < 0 else end
        elif src.startswith("/*", i):
            end = src.find("*/", i + 2)
            assert end >= 0, "{}: unterminated comment at line {}".format(where, src.count("\n", 0, i) + 1)
            out.append(" " + "\n" * src.count("\n", i, end))
            i = end + 2
        elif c in "\"'":
            end = i + 1
            while end < n and src[end] != c and src[end] != "\n":
                end += 2 if src[end] == "\\" else 1
            assert end < n and src[end] == c, "{}: unterminated {} literal at line {}".format(
                where, "string" if c == '"' else "char", src.count("\n", 0, i) + 1
            )
            out.append(src[i:end + 1] if c == '"' else "0")
            i = end + 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


def call_text(src: str, start: int, where: str) -> str:
    """Text between the parentheses of a call whose '(' ends just before start, skipping string literals."""
    depth, i, in_string = 1, start, False
    while depth:
        assert i < len(src), "{}: sa_g_log_function call has unbalanced parentheses".format(where)
        c = src[i]
        if in_string:
            if c == "\\":
                i += 1
            elif c == '"':
                in_string = False
        elif c == '"':
            in_string = True
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        i += 1
    return src[start:i - 1]


def library_sources() -> list[str]:
    return sorted(glob.glob(os.path.join(LIB_SRC, "main", "*.c"))) + sorted(glob.glob(os.path.join(LIB_SRC, "include", "*.h")))


def library_log_calls() -> list[tuple[str, str | None, str]]:
    calls = []
    for path in library_sources():
        name = os.path.relpath(path, LIB_SRC)
        with open(path) as f:
            src = strip_comments_and_chars(f.read(), name)
        for m in re.finditer(r"\bsa_g_log_function\s*\(", src):
            where = "{}:{}".format(name, src.count("\n", 0, m.start()) + 1)
            text = call_text(src, m.end(), where)
            literal = re.match(r"\s*((?:{}\s*)+)(.*)$".format(C_STRING), text, re.S)
            if literal is None:
                calls.append((where, None, " ".join(text.split())))
                continue
            fmt = "".join(c_unescape(s) for s in re.findall(C_STRING, literal.group(1)))
            calls.append((where, fmt, " ".join(literal.group(2).lstrip(",").split())))
    return calls


def aql_log_allow_list() -> list[str]:
    with open(MAIN_C) as f:
        src = strip_comments_and_chars(f.read(), "main.c")
    match = re.search(r"SA_LOG_ALLOW\[\]\s*=\s*\{(.*?)\n\};", src, re.S)
    assert match, "SA_LOG_ALLOW array not found in {}".format(MAIN_C)
    entries = re.findall(r"\{\s*(" + C_STRING + r")\s*,", match.group(1))
    assert entries, "no entries read from SA_LOG_ALLOW in {}".format(MAIN_C)
    return [c_unescape(s) for s in entries]


class SecretAgentLogFilterTest(unittest.TestCase):
    """aql prints library log arguments only for allow-listed lines; a submodule bump must not slip past."""

    def test_library_sources_found(self):
        sources = library_sources()
        self.assertTrue(any(p.endswith(".c") for p in sources), "no library .c files under " + LIB_SRC)
        self.assertTrue(any(p.endswith(".h") for p in sources), "no library .h files under " + LIB_SRC)
        self.assertGreater(len(library_log_calls()), 20)

    def test_comments_and_char_literals_are_ignored(self):
        src = strip_comments_and_chars(
            'a("//", \'"\', \'(\'); // sa_g_log_function(x);\n/* sa_g_log_function(y);\n */ b("/*");\n', "sample"
        )
        self.assertNotIn("sa_g_log_function", src)
        self.assertEqual(src.count("\n"), 3)
        self.assertIn('a("//", 0, 0);', src)
        self.assertIn('b("/*");', src)

    def test_allow_list_is_reviewed(self):
        self.assertEqual(sorted(aql_log_allow_list()), sorted(REVIEWED_LOG_ARGS))

    def test_every_library_log_line_is_allowed_or_truncated(self):
        allowed = set(aql_log_allow_list())
        seen = set()
        problems = []

        for where, fmt, args in library_log_calls():
            if fmt is None:
                problems.append("{}: format is not a string literal: {}".format(where, args))
            elif fmt in allowed:
                seen.add(fmt)
                if args != REVIEWED_LOG_ARGS.get(fmt):
                    problems.append("{}: allowed line {!r} now passes {!r}, review it".format(where, fmt, args))
            elif "%" in fmt and not fmt[:fmt.index("%")].rstrip(" :,-("):
                problems.append("{}: {!r} has no fixed text to print".format(where, fmt))

        problems += ["allowed line {!r} is not in the library".format(fmt) for fmt in sorted(allowed - seen)]
        self.assertEqual(problems, [])


class SecretAgentTest(unittest.TestCase):
    """Runs aerospike-secret-agent in Docker with the file backend. No server needed."""

    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp()
        cls.addClassCleanup(shutil.rmtree, cls.dir)
        os.chmod(cls.dir, 0o755)

        write(os.path.join(cls.dir, "secrets.json"), json.dumps({k: b64(v) for k, v in SECRETS.items()}))
        write(os.path.join(cls.dir, "other.json"), json.dumps({"pw": b64("other")}))

        cls.key = os.path.join(cls.dir, "key.pem")
        make_key(cls.key, SECRET)
        cls.parsed_looking_key = os.path.join(cls.dir, "key2.pem")
        make_key(cls.parsed_looking_key, PARSED_LOOKING_SECRET)
        cls.cert, cert_key = make_cert(cls.dir)

        client = utils._get_docker_client()
        try:
            client.images.get("{}:{}".format(AGENT_IMAGE, AGENT_TAG))
        except Exception:
            client.images.pull(AGENT_IMAGE, tag=AGENT_TAG, platform="linux/amd64")

        cls.port = str(start_agent(cls, cls.dir, "tcp", None))
        cls.tls_port = str(start_agent(cls, cls.dir, "tls", (cls.cert, cert_key)))
        cls.closed = str(closed_port())

    def config(self, content: str) -> str:
        path = os.path.join(self.dir, "astools-{}.conf".format(uuid.uuid4().hex))
        write(path, content)
        return path

    def assert_no_secret(self, out: str):
        for value in [SECRET, b64(SECRET), "wrong", b64("wrong")]:
            self.assertNotIn(value, out)

    def assert_password_resolved(self, rc: int, out: str):
        self.assertNotEqual(rc, 0)
        self.assertNotIn("--password:", out)
        self.assertNotIn("secret-agent:", out)
        self.assertIn(CONNECT_FAILED, out)
        self.assert_no_secret(out)

    def test_tls_keyfile_password(self):
        rc, out = run(
            ["--tls-enable", "--tls-keyfile", self.key, "--tls-keyfile-password", "secrets:aql:pw", "--sa-port", self.port]
        )

        self.assertNotIn("--tls-keyfile-password:", out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    def test_tls_keyfile_wrong_password(self):
        rc, out = run(
            ["--tls-enable", "--tls-keyfile", self.key, "--tls-keyfile-password", "secrets:aql:wrong", "--sa-port", self.port]
        )

        self.assertNotEqual(rc, 0)
        self.assertNotIn("--tls-keyfile-password:", out)
        self.assertIn(TLS_KEY_FAILED, out)

    def test_value_is_not_trimmed(self):
        rc, out = run(
            [
                "--tls-enable", "--tls-keyfile", self.key,
                "--tls-keyfile-password", "secrets:aql:trailing_newline", "--sa-port", self.port,
            ]
        )

        self.assertNotEqual(rc, 0)
        self.assertIn(TLS_KEY_FAILED, out)

    def test_value_is_not_parsed_again(self):
        rc, out = run(
            [
                "--tls-enable", "--tls-keyfile", self.parsed_looking_key,
                "--tls-keyfile-password", "secrets:aql:env_looking", "--sa-port", self.port,
            ]
        )

        self.assertNotIn("--tls-keyfile-password:", out)
        self.assertNotIn("environment variable", out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    @parameterized.expand(
        [
            ("password_first_attached", ["-Psecrets:aql:pw", "--sa-address", "127.0.0.1:{port}"]),
            ("password_first_separate", ["-P", "secrets:aql:pw", "--sa-address=127.0.0.1:{port}"]),
            ("password_long", ["--password=secrets:aql:pw", "--sa-port", "{port}"]),
            ("agent_first", ["--sa-port", "{port}", "--password", "secrets:aql:pw"]),
            ("explicit_port_after_address", ["-P", "secrets:aql:pw", "--sa-address", "127.0.0.1:{closed}", "--sa-port", "{port}"]),
            ("explicit_port_before_address", ["-P", "secrets:aql:pw", "--sa-port", "{port}", "--sa-address", "127.0.0.1:{closed}"]),
            ("hostname", ["-P", "secrets:aql:pw", "--sa-address", "localhost", "--sa-port", "{port}"]),
            ("timeout", ["-P", "secrets:aql:pw", "--sa-port", "{port}", "--sa-timeout", "5000"]),
        ]
    )
    def test_password(self, _, args):
        args = [a.format(port=self.port, closed=self.closed) for a in args]
        rc, out = run(["-U", "admin"] + args)

        self.assert_password_resolved(rc, out)

    def test_login_uses_secret_from_command_line(self):
        login = capture_login(["-U", "admin", "-P", "secrets:aql:pw", "--sa-port", self.port])
        self.assertEqual(login, literal_login(SECRET))

    def test_login_uses_wrong_secret_from_command_line(self):
        login = capture_login(["-U", "admin", "-P", "secrets:aql:wrong", "--sa-port", self.port])
        self.assertEqual(login, literal_login("wrong"))
        self.assertNotEqual(login, literal_login(SECRET))

    def test_login_uses_secret_from_config_file(self):
        config = self.config(
            '[cluster]\nuser = "admin"\npassword = "secrets:aql:pw"\n'
            '[secret-agent]\nsa-port = {}\n'.format(self.port)
        )
        self.assertEqual(capture_login([], config=config), literal_login(SECRET))

    def test_password_from_config_file(self):
        config = self.config(
            '[cluster]\nuser = "admin"\npassword = "secrets:aql:pw"\n'
            '[secret-agent]\nsa-address = "127.0.0.1"\nsa-port = {}\nsa-timeout = 5000\n'.format(self.port)
        )
        rc, out = run([], config=config)

        self.assert_password_resolved(rc, out)

    def test_tls_keyfile_password_from_config_file(self):
        config = self.config(
            '[cluster]\ntls-enable = true\ntls-keyfile = "{}"\ntls-keyfile-password = "secrets:aql:pw"\n'
            '[secret-agent]\nsa-port = "{}"\n'.format(self.key, self.port)
        )
        rc, out = run([], config=config)

        self.assertNotIn("--tls-keyfile-password:", out)
        self.assertNotIn(TLS_KEY_FAILED, out)
        self.assertIn(CONNECT_FAILED, out)

    def test_config_explicit_port_beats_address_port(self):
        config = self.config(
            '[secret-agent]\nsa-port = {}\nsa-address = "127.0.0.1:{}"\n'.format(self.port, self.closed)
        )
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw"], config=config)

        self.assert_password_resolved(rc, out)

    def test_command_line_applies_to_config_file_password(self):
        config = self.config(
            '[cluster]\nuser = "admin"\npassword = "secrets:aql:pw"\n'
            '[secret-agent]\nsa-address = "127.0.0.1:{}"\n'.format(self.closed)
        )

        rc, out = run([], config=config)
        self.assertNotEqual(rc, 0)
        self.assertIn("--password: secret agent request for secrets:aql:pw failed", out)

        rc, out = run(["--sa-address", "127.0.0.1:" + self.port], config=config)
        self.assert_password_resolved(rc, out)

        rc, out = run(["--sa-port", self.port], config=config)
        self.assert_password_resolved(rc, out)

    def test_command_line_address_port_beats_config_port(self):
        config = self.config('[secret-agent]\nsa-port = {}\n'.format(self.closed))
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw", "--sa-address", "127.0.0.1:" + self.port], config=config)

        self.assert_password_resolved(rc, out)

    def test_instance_section_replaces_default_section(self):
        config = self.config(
            '[secret-agent]\nsa-port = {}\nsa-cafile = "{}"\n'
            '[secret-agent_a]\nsa-port = {}\n'.format(self.closed, os.path.join(self.dir, "missing.pem"), self.port)
        )

        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw"], config=config)
        self.assertNotEqual(rc, 0)
        self.assertIn("--sa-cafile: cannot read file", out)

        rc, out = run(["--instance", "a", "-U", "admin", "-P", "secrets:aql:pw"], config=config)
        self.assert_password_resolved(rc, out)

        rc, out = run(["--instance", "b", "-U", "admin", "-P", "secrets:aql:pw"], config=config)
        self.assertNotEqual(rc, 0)
        self.assertIn("--sa-cafile: cannot read file", out)

    def test_included_file(self):
        included = self.config('[secret-agent]\nsa-port = {}\n'.format(self.port))
        config = self.config(
            '[secret-agent]\nsa-port = {}\n[include]\nfile = "{}"\n'.format(self.closed, included)
        )
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw"], config=config)

        self.assert_password_resolved(rc, out)

    def test_tls_to_agent(self):
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw", "--sa-port", self.tls_port, "--sa-cafile", self.cert])
        self.assert_password_resolved(rc, out)

        config = self.config('[secret-agent]\nsa-port = {}\nsa-cafile = "{}"\n'.format(self.tls_port, self.cert))
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw"], config=config)
        self.assert_password_resolved(rc, out)

    def test_tls_agent_without_cafile(self):
        rc, out = run(["-U", "admin", "-P", "secrets:aql:pw", "--sa-port", self.tls_port, "--sa-timeout", "2000"])

        self.assertNotEqual(rc, 0)
        self.assertIn("--password: secret agent request for secrets:aql:pw failed: ", out)
        self.assertNotIn(CONNECT_FAILED, out)

    @parameterized.expand(
        [
            ("missing_resource", "secrets:nope:pw", "secret-agent: ERR: response: resource not found in config: nope\n"),
            ("missing_key", "secrets:aql:nope", "secret-agent: ERR: response: nope not present in file {}/secrets.json\n".format(AGENT_DIR)),
            ("empty_value", "secrets:aql:empty", "secret-agent: ERR: empty secret\n"),
            ("no_key", "secrets:", "secret-agent: ERR: empty secret key\n"),
        ]
    )
    def test_agent_error(self, _, path, detail):
        rc, out = run(["-U", "admin", "-P", path, "--sa-port", self.port])

        self.assertNotEqual(rc, 0)
        self.assertIn(detail, out)
        self.assertIn("--password: secret agent request for {} failed: bad request\n".format(path), out)
        self.assertNotIn(CONNECT_FAILED, out)
        self.assert_no_secret(out)

    def test_value_with_nul_byte(self):
        rc, out = run(["-U", "admin", "-P", "secrets:aql:with_nul", "--sa-port", self.port])

        self.assertNotEqual(rc, 0)
        self.assertIn("--password: secret agent returned a value containing a NUL byte for secrets:aql:with_nul\n", out)
        self.assertNotIn(CONNECT_FAILED, out)
        self.assertNotIn("xyzzy", out)

    def test_resource_selects_file(self):
        rc, out = run(
            ["--tls-enable", "--tls-keyfile", self.key, "--tls-keyfile-password", "secrets:other:pw", "--sa-port", self.port]
        )

        self.assertIn(TLS_KEY_FAILED, out)


if __name__ == "__main__":
    unittest.main()
