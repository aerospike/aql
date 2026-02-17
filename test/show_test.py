import unittest
from parameterized import parameterized
import utils

as_client = None

class ShowPositiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ips = utils.run_containers(utils.SET_NAME, 1, version=utils.AEROSPIKE_VERSION)
        cls.addClassCleanup(lambda: utils.shutdown_containers(utils.SET_NAME))
        utils.create_client((cls.ips[0], utils.PORT))
        utils.populate_db(utils.SET_NAME)
        utils.create_sindex("a-str-index", "string", "test", "a-str", set_=utils.SET_NAME)
        utils.create_sindex("b-str-index", "string", "test", "b-str", set_=utils.SET_NAME)
        utils.create_sindex("a-int-index", "numeric", "test", "a-int", set_=utils.SET_NAME)
        utils.create_sindex("b-int-index", "numeric", "test", "b-int", set_=utils.SET_NAME)
        

    @parameterized.expand(
        [
            ("set output json; show namespaces", 1, ["namespaces"]),
            (
                "set output json; show indexes",
                4,
                [
                    "bin",
                    "context",
                    "exp",
                    "indexname",
                    "indextype",
                    "ns",
                    "set",
                    "state",
                    "type",
                ],
            ),
            (
                "set output json; show sets",
                1,
                [
                    "data_used_bytes",
                    "default-read-touch-ttl-pct",
                    "default-ttl",
                    "disable-eviction",
                    "enable-index",
                    "index_populating",
                    "ns",
                    "objects",
                    "set",
                    "sindexes",
                    "stop-writes-count",
                    "stop-writes-size",
                    "tombstones",
                    "truncate_lut",
                    "truncating",
                ],
            ),
        ]
    )
    def test_show_correct_keys(self, cmd: str, row_count: int, column_keys: list[str]):
        output = utils.run_aql(["-h", self.ips[0], "-p", str(utils.PORT), "-c", cmd])
        self.assertEqual(output.returncode, 0)
        json_out = utils.parse_json_output(output.stdout)
        print(json_out)

        rows = json_out[0]
        status = json_out[1]

        self.assertEqual(len(rows[:-1]), row_count)

        for row in rows[:-1]:
            self.assertCountEqual(list(row.keys()), column_keys)

        self.assertCountEqual(list(rows[-1].keys()), ["node"])
        self.assertEqual(status[0]["Status"], 0)
