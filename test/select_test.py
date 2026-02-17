
import sys
import time
import unittest
from parameterized import parameterized
import utils

as_client = None

class SelectPositiveTest(unittest.TestCase):
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
        utils.create_sindex("mix-int-index", "numeric", "test", "int-str-mix", set_=utils.SET_NAME)
        utils.create_sindex("mix-str-index", "string", "test", "int-str-mix", set_=utils.SET_NAME)
        utils.create_sindex("a-int-index-no-set", "numeric", "test", "a-int")
        utils.create_sindex("b-int-index-no-set", "numeric", "test", "b-int")

    @parameterized.expand(
        [
            (
                "select * from test.{}".format(utils.SET_NAME),
                "100 rows in set",
            ),
            (
                "select * from test.{}".format(utils.SET_NAME),
                "100 rows in set",
            ),
        ]
    )
    def test_select(self, cmd, check_str):
        output = utils.run_aql(
            ["-h", self.ips[0], "-p", str(utils.PORT), "-c", cmd]
        )
        self.assertEqual(output.returncode, 0)
        self.assertRegex(str(output.stdout), check_str)

    @parameterized.expand(
        [
            (
                "select * from test.{} where a-int = 0".format(utils.SET_NAME),
                "20 rows in set",
            ),
            (
                "select * from test.{} where a-int = 0".format(utils.SET_NAME),
                "20 rows in set",
            ),
        ]
    )
    def test_select_where(self, cmd, check_str):
        output = utils.run_aql(
            [
                "-h",
                self.ips[0],
                "-p",
                str(utils.PORT),
                "-c",
                cmd,
            ]
        )
        self.assertEqual(output.returncode, 0)
        self.assertRegex(str(output.stdout), check_str)

    @parameterized.expand(
        [
            (
                "select * from test.{} limit 9".format(utils.SET_NAME),
                "9 rows in set",
            ),
            (
                "select * from test.{} limit 9".format(utils.SET_NAME),
                "9 rows in set",
            ),
        ]
    )
    def test_select_limit(self, cmd, check_str):
        output = utils.run_aql(
            ["-h", self.ips[0], "-p", str(utils.PORT), "-c", cmd]
        )
        self.assertEqual(output.returncode, 0)
        self.assertRegex(str(output.stdout), check_str)

    @parameterized.expand(
        [
            (
                "select * from test.{} where a-int = 0 limit 9".format(utils.SET_NAME),
                "9 rows in set",
            ),
            (
                "select * from test.{} where a-int = 0 limit 9".format(utils.SET_NAME),
                "9 rows in set",
            ),
            (
                "select * from test.{} limit 9 where a-int = 0 ".format(utils.SET_NAME),
                "9 rows in set",
            ),
        ]
    )
    def test_select_where_limit(self, cmd, check_str):
        output = utils.run_aql(
            [
                "-h",
                self.ips[0],
                "-p",
                str(utils.PORT),
                "-c",
                cmd,
            ]
        )
        self.assertEqual(output.returncode, 0)
        self.assertRegex(str(output.stdout), check_str)

    @parameterized.expand(
        [
            (
                "select * from test.{} where a-int = 0 and int = 0".format(utils.SET_NAME),
                "20 rows in set",
            ),
            (
                "select * from test.{} where int = 0 and a-int = 0".format(utils.SET_NAME),
                "20 rows in set",
            ),
            (
                "select * from test.{} where a-int = 0 and b-int = 5".format(utils.SET_NAME),
                "10 rows in set",
            ),
            (
                'select * from test.{} where b-str = "0" and a-str = "5"'.format(
                    utils.SET_NAME
                ),
                "10 rows in set",
            ),
            (
                'select * from test.{} where b-str = "0" and b-int = 5'.format(
                    utils.SET_NAME
                ),
                "10 rows in set",
            ),
            (
                'select * from test.{} where b-int = 5 and b-str = "5"'.format(
                    utils.SET_NAME
                ),
                "0 rows in set",
            ),
            (
                'select * from test.{} where int-str-mix = "4" and b-int = 9'.format(
                    utils.SET_NAME
                ),
                "2 rows in set",
            ),
            (
                'select * from test.{} where b-str = "0" and int-str-mix = 5'.format(
                    utils.SET_NAME
                ),
                "8 rows in set",
            ),
        ]
    )
    def test_select_double_where_table(self, cmd, check_str):
        output = utils.run_aql(
            [
                "-h",
                self.ips[0],
                "-p",
                str(utils.PORT),
                "-c",
                cmd,
            ]
        )
        self.assertEqual(output.returncode, 0)
        self.assertRegex(str(output.stdout), check_str)
        
    @parameterized.expand(
        [
            (
                "set output json; select * from test.{} where a-int = 0 and int = 0".format(utils.SET_NAME),
                20,
                ("a-int", 0),
                ("int", 0)
            ),
            (
                "set output json; select * from test.{} where int = 0 and a-int = 0".format(utils.SET_NAME),
                20,
                ("a-int", 0),
                ("int", 0)
            ),
            (
                "set output json; select * from test.{} where a-int = 0 and b-int = 5".format(utils.SET_NAME),
                10,
                ("a-int", 0),
                ("b-int", 5)
            ),
            (
                'set output json; select * from test.{} where b-str = "0" and a-str = "5"'.format(
                    utils.SET_NAME
                ),
                10,
                ("b-str", "0"),
                ("a-str", "5")
            ),
            (
                'set output json; select * from test.{} where b-str = "0" and b-int = 5'.format(
                    utils.SET_NAME
                ),
                10,
                ("b-str", "0"),
                ("b-int", 5)
            ),
        ]
    )
    def test_select_double_where_json(self, cmd, row_count, col_1_val, col_2_val):
        output = utils.run_aql(
            [
                "-h",
                self.ips[0],
                "-p",
                str(utils.PORT),
                "-c",
                cmd,
            ]
        )
        self.assertEqual(output.returncode, 0)
        json_out = utils.parse_json_output(output.stdout)
        print(json_out)

        rows = json_out[0]
        status = json_out[1]

        self.assertEqual(len(rows), row_count)

        for row in rows[:-1]:
            val1 = row[col_1_val[0]]
            val2 = row[col_2_val[0]]
            self.assertEqual(val1, col_1_val[1])
            self.assertEqual(val2, col_2_val[1])

        self.assertEqual(status[0]["Status"], 0)

    @parameterized.expand(
        [
            (
                "select * from test.{} where a-int = 0 and int = 0 limit 10".format(
                    utils.SET_NAME
                ),
                "10 rows in set",
            ),
            (
                "select * from test.{} where int = 0 and a-int = 0 limit 5".format(
                    utils.SET_NAME
                ),
                "5 rows in set",
            ),
            (
                "select * from test.{} where a-int = 0 and b-int = 5 limit 2".format(
                    utils.SET_NAME
                ),
                "2 rows in set",
            ),
            (
                'select * from test.{} where b-str = "0" and a-str = "5" limit 7'.format(
                    utils.SET_NAME
                ),
                "7 rows in set",
            ),
            (
                'select * from test.{} where b-str = "0" and b-int = 5 limit 7'.format(
                    utils.SET_NAME
                ),
                "7 rows in set",
            ),
            (
                'select * from test.{} where b-int = 5 and b-str = "5" limit 0'.format(
                    utils.SET_NAME
                ),
                "0 rows in set",
            ),
        ]
    )
    def test_select_double_where_limit(self, cmd, check_str):
        output = utils.run_aql(
            [
                "-h",
                self.ips[0],
                "-p",
                str(utils.PORT),
                "-c",
                cmd,
            ]
        )
        self.assertEqual(output.returncode, 0)
        self.assertRegex(str(output.stdout), check_str)


class SelectNegativeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ips = utils.run_containers(utils.SET_NAME, 1, version=utils.AEROSPIKE_VERSION)
        cls.addClassCleanup(lambda: utils.shutdown_containers(utils.SET_NAME))
        utils.create_client((cls.ips[0], utils.PORT))
        utils.create_sindex("b-int-index", "numeric", "test", "b", utils.SET_NAME)

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
            (
                "select * from test.testset where a between 1 and 2 and b = 3",
                "Double where clause only supports '=' expressions.",
            ),
            (
                "select * from test.testset where b = 3 and a between 1 and 2",
                "Double where clause only supports '=' expressions.",
            ),
            (
                "select * from test.testset in list  where b = 3 and a = 3",
                '"IN <indextype>" not supported with double where clause.',
            ),
            (
                "select * from test.testset where b = 3 and a = 3",
                "Error: at least one bin needs a secondary index defined",
            ),
            (
                "select * from test.testset where b = 3 and a = 3",
                "Error: at least one bin needs a secondary index defined",
            ),
            (
                "select * from test.{} where b = 3 and a = 3.3".format(utils.SET_NAME),
                "Error: Equality match is only available for int and string bins",
            ),
        ]
    )
    def test_select_syntax_error(self, cmd, assert_str):
        output = utils.run_aql(["-h", self.ips[0], "-p", str(utils.PORT), "-c", cmd])
        self.assertEqual(output.returncode, 0)
        print(str(output.stderr))
        self.assertTrue(assert_str in output.stderr.decode(sys.stdout.encoding))
