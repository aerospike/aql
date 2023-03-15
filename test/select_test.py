import sys
import time
import unittest
import subprocess
import docker
import aerospike
from parameterized import parameterized

as_client = None
set_name = "aql-tests"


def run_containers(count: int, config_file: str = None):
    client = docker.from_env()
    aerolab_cmd = [
        "aerolab",
        "cluster",
        "create",
        # "-F=--network={}".format(aql_net),
        "-n",
        set_name,
        "-c",
        str(count),
        "-v=6.2.0.3",
    ]

    if config_file:
        aerolab_cmd.append("--customconf={}".format(config_file))

    subprocess.run(aerolab_cmd)

    containers = client.containers.list()
    ips = []

    for c in containers:
        if "aerolab-aql" not in c.attrs["Name"]:
            continue

        nets = c.attrs["NetworkSettings"]["Networks"].values()

        for net in nets:
            if "IPAddress" in net:
                ips.append(net["IPAddress"])
                break

    time.sleep(3)  # TODO: Add mechanism to wait until containers can be connected to
    return ips


def shutdown_containers():
    aerolab_cmd = ["aerolab", "cluster", "destroy", "-n", set_name, "-f"]
    subprocess.run(aerolab_cmd)
    client = docker.from_env()
    client.networks.prune()


def run_aql(args=None) -> subprocess.CompletedProcess:
    cmd = ["target/Darwin-x86_64/bin/aql"]  # TODO: Do something platform independent
    args = [] if args is None else args
    cmd.extend(args)
    return subprocess.run(cmd, capture_output=True)


def create_client(seed: tuple[str, int] = ("127.0.0.1", 3000)):
    global as_client
    config = {"hosts": [seed], "timeout": 3000}
    as_client = aerospike.client(config)
    print("Successfully connected to seed", seed)


def populate_db():
    global as_client
    write_policy = {"key": aerospike.POLICY_KEY_SEND}
    keys = []

    for idx in range(100):
        key = ("test", set_name, "key" + str(idx))
        bins = {
            "str": str(idx),
            "int": idx % 5,
            "float": idx * 3.14,
        }
        keys.append(key)
        as_client.put(key, bins, policy=write_policy)

    print("Successfully populated DB")


def create_sindex(name, type_, ns, set, bin):
    as_client.info_all(
        "sindex-create:ns={};set={};indexname={};indexdata={},{}".format(
            ns, set, name, bin, type_
        )
    )
    time.sleep(3)  # TODO: Instead of sleep wait for sindex to exist
    print("Successfully created secondary index", name)


class SelectPositiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.addClassCleanup(shutdown_containers)
        cls.ips = run_containers(1)
        create_client((cls.ips[0], 3000))
        populate_db()
        create_sindex("int-index", "numeric", "test", set_name, "int")

    def test_select(self):
        output = run_aql(
            ["-h", self.ips[0], "-c", "select * from test.{}".format(set_name)]
        )
        self.assertEqual(output.returncode, 0)
        self.assertRegex(str(output.stdout), "100 rows in set")

    def test_select_where(self):
        output = run_aql(
            [
                "-h",
                self.ips[0],
                "-c",
                "select * from test.{} where int = 0".format(set_name),
            ]
        )
        self.assertEqual(output.returncode, 0)
        self.assertRegex(str(output.stdout), "20 rows in set")

    def test_select_limit(self):
        output = run_aql(
            ["-h", self.ips[0], "-c", "select * from test.{} limit 9".format(set_name)]
        )
        self.assertEqual(output.returncode, 0)
        self.assertRegex(str(output.stdout), "9 rows in set")

    def test_select_where_limit(self):
        output = run_aql(
            [
                "-h",
                self.ips[0],
                "-c",
                "select * from test.{} where int = 0 limit 9".format(set_name),
            ]
        )
        self.assertEqual(output.returncode, 0)
        self.assertRegex(str(output.stdout), "9 rows in set")

    @parameterized.expand(
        [
            ("select *", "Syntax error near token -  '*'"),
            ("select * from test.testset where", "Syntax error near token -  'where'"),
            ("select * from test.testset limit", "Syntax error near token -  'limit'"),
            (
                "select * from test.testset where a = 5 limit",
                "Syntax error near token -  'limit'",
            ),
            (
                "select * from test.testset limit 5.5",
                "Unsupported command format with token -  '5.5'",
            ),
            (
                "select * from test.testset where a = 5 limit true",
                "Unsupported command format with token -  'true'",
            ),
        ]
    )
    def test_select_syntax_error(self, cmd, assert_str):
        output = run_aql(["-h", self.ips[0], "-c", cmd])
        self.assertEqual(output.returncode, 0)
        print(str(output.stderr))
        self.assertTrue(assert_str in output.stderr.decode(sys.stdout.encoding))
