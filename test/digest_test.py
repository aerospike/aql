import base64
import sys
import unittest

import aerospike
from parameterized import parameterized
import utils

DIGEST = bytes(aerospike.calc_digest("test", utils.SET_NAME, "key0"))
DIGEST_HEX = DIGEST.hex()
DIGEST_HEX_MIXED = "".join(
    c.upper() if i % 2 else c for i, c in enumerate(DIGEST_HEX)
)
DIGEST_B64 = base64.b64encode(DIGEST).decode()

DELETE_DIGEST = bytes(aerospike.calc_digest("test", utils.SET_NAME, "key99"))
DELETE_B64 = base64.b64encode(DELETE_DIGEST).decode()

B64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"


def flip_unused_pad_bit(encoded: str) -> str:
    last = len(encoded) - 2
    flipped = B64_ALPHABET[B64_ALPHABET.index(encoded[last]) ^ 1]
    return encoded[:last] + flipped + encoded[last + 1:]


DIGEST_B64_PAD_BITS = flip_unused_pad_bit(DIGEST_B64)

BIN_NAME_COMMANDS = [
    ("select by digest",
            "select {} from test.SET where digest = '" + DIGEST_HEX + "'"),
    ("select by pk", "select {} from test.SET where pk = 'key0'"),
    ("scan", "select {} from test.SET"),
    ("query", "select {} from test.SET where a-int = 0"),
    ("insert", "insert into test.SET (PK, {}) values ('key0', 1)"),
]

NON_STRING_VALUES = [
    ("null", "null"),
    ("true", "true"),
    ("false", "false"),
    ("float", "1.5"),
    ("integer", "123"),
]

DIGEST_COMMANDS = [
    ("select", "select * from test.SET where {} = {}"),
    ("delete", "delete from test.SET where {} = {}"),
    ("execute", "execute test1.foo() on test.SET where {} = {}"),
]


class DigestPositiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ips = utils.run_containers(utils.SET_NAME, 1, version=utils.AEROSPIKE_VERSION)
        cls.addClassCleanup(lambda: utils.shutdown_containers(utils.SET_NAME))
        utils.create_client((cls.ips[0], utils.PORT))
        utils.populate_db(utils.SET_NAME)

    def run_aql(self, cmd) -> str:
        output = utils.run_aql(["-h", self.ips[0], "-p", str(utils.PORT), "-c", cmd])
        self.assertEqual(output.returncode, 0)
        return output.stdout.decode(sys.stdout.encoding)

    @parameterized.expand(
        [
            ("hex lower case", DIGEST_HEX),
            ("hex upper case", DIGEST_HEX.upper()),
            ("hex mixed case", DIGEST_HEX_MIXED),
        ]
    )
    def test_select_by_digest(self, _, digest):
        cmd = "select * from test.{} where digest = '{}'".format(
            utils.SET_NAME, digest
        )
        self.assertRegex(self.run_aql(cmd), "1 row in set")

    def test_select_by_edigest(self):
        cmd = "select * from test.{} where edigest = '{}'".format(
            utils.SET_NAME, DIGEST_B64
        )
        self.assertRegex(self.run_aql(cmd), "1 row in set")

    def test_select_bin_by_edigest(self):
        cmd = "select str from test.{} where edigest = '{}'".format(
            utils.SET_NAME, DIGEST_B64
        )
        self.assertRegex(self.run_aql(cmd), "1 row in set")

    def test_delete_by_edigest(self):
        cmd = "delete from test.{} where edigest = '{}'".format(
            utils.SET_NAME, DELETE_B64
        )
        self.assertRegex(self.run_aql(cmd), "1 record affected")

        with self.assertRaises(aerospike.exception.RecordNotFound):
            utils.as_client.get(("test", utils.SET_NAME, "key99"))

    def test_integer_pk_round_trip(self):
        cmd = "insert into test.{} (PK, str) values (12345, 'int-pk')".format(
            utils.SET_NAME
        )
        self.assertRegex(self.run_aql(cmd), "1 record affected")

        cmd = "select * from test.{} where pk = 12345".format(utils.SET_NAME)
        self.assertRegex(self.run_aql(cmd), "1 row in set")

    def test_insert_null_bin(self):
        cmd = "insert into test.{} (PK, str, a-int) values ('null-bin', null, 1)"
        self.assertRegex(self.run_aql(cmd.format(utils.SET_NAME)),
                         "1 record affected")

        rec = utils.as_client.get(("test", utils.SET_NAME, "null-bin"))
        self.assertNotIn("str", rec[2])
        self.assertEqual(rec[2]["a-int"], 1)

    def test_desc_module(self):
        cmd = "register module '{}'".format(utils.absolute_path("lua", "test1.lua"))
        self.assertRegex(self.run_aql(cmd), "1 module added")

        cmd = "desc module test1.lua"
        self.assertRegex(self.run_aql(cmd), "function")


class DigestNegativeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.ips = utils.run_containers(utils.SET_NAME, 1, version=utils.AEROSPIKE_VERSION)
        cls.addClassCleanup(lambda: utils.shutdown_containers(utils.SET_NAME))
        utils.create_client((cls.ips[0], utils.PORT))
        utils.populate_db(utils.SET_NAME)
        utils.create_sindex(
            "a-int-index", "numeric", "test", "a-int", set_=utils.SET_NAME
        )

    def run_aql(self, cmd) -> str:
        output = utils.run_aql(["-h", self.ips[0], "-p", str(utils.PORT), "-c", cmd])
        self.assertEqual(output.returncode, 0)
        return output.stderr.decode(sys.stdout.encoding)

    @parameterized.expand(
        [
            ("empty", "", "Digest must be 40 hex characters, got 0"),
            ("one character", "0", "Digest must be 40 hex characters, got 1"),
            ("one short", "0" * 39, "Digest must be 40 hex characters, got 39"),
            ("one long", "0" * 41, "Digest must be 40 hex characters, got 41"),
            ("huge", "0" * 65536, "Digest must be 40 hex characters, got 65536"),
        ]
    )
    def test_select_by_digest_bad_length(self, _, digest, err):
        cmd = "select * from test.{} where digest = '{}'".format(
            utils.SET_NAME, digest
        )
        self.assertIn(err, self.run_aql(cmd))

    @parameterized.expand(
        [
            ("all non hex", "g" * 40),
            ("hex prefix", "0x" + DIGEST_HEX[2:]),
            ("embedded space", DIGEST_HEX[:10] + " " + DIGEST_HEX[11:]),
            ("last character", DIGEST_HEX[:-1] + "z"),
        ]
    )
    def test_select_by_digest_bad_characters(self, _, digest):
        cmd = "select * from test.{} where digest = '{}'".format(
            utils.SET_NAME, digest
        )
        self.assertIn("Digest is not valid hex", self.run_aql(cmd))

    @parameterized.expand(
        [
            ("empty", "", "Edigest must be 28 base64 characters, got 0"),
            ("short", "A" * 24, "Edigest must be 28 base64 characters, got 24"),
            ("unpadded", "A" * 27, "Edigest must be 28 base64 characters, got 27"),
            ("one long", "A" * 29, "Edigest must be 28 base64 characters, got 29"),
            ("one group long", "A" * 32,
                    "Edigest must be 28 base64 characters, got 32"),
            ("huge", "A" * 65536,
                    "Edigest must be 28 base64 characters, got 65536"),
        ]
    )
    def test_select_by_edigest_bad_length(self, _, edigest, err):
        cmd = "select * from test.{} where edigest = '{}'".format(
            utils.SET_NAME, edigest
        )
        self.assertIn(err, self.run_aql(cmd))

    @parameterized.expand(
        [
            ("invalid character", "!" + DIGEST_B64[1:]),
            ("misplaced padding", DIGEST_B64[:13] + "=" + DIGEST_B64[14:]),
            ("19 bytes", base64.b64encode(bytes(19)).decode()),
            ("21 bytes", base64.b64encode(bytes(21)).decode()),
        ]
    )
    def test_select_by_edigest_bad_content(self, _, edigest):
        cmd = "select * from test.{} where edigest = '{}'".format(
            utils.SET_NAME, edigest
        )
        self.assertIn(
            "Edigest is not a valid base64 encoded digest", self.run_aql(cmd)
        )

    def test_select_by_edigest_unused_pad_bits(self):
        self.assertNotEqual(DIGEST_B64_PAD_BITS, DIGEST_B64)
        self.assertEqual(
            base64.b64decode(DIGEST_B64_PAD_BITS), base64.b64decode(DIGEST_B64)
        )

        cmd = "select * from test.{} where edigest = '{}'".format(
            utils.SET_NAME, DIGEST_B64_PAD_BITS
        )
        self.assertIn(
            "Edigest is not canonically base64 encoded", self.run_aql(cmd)
        )

    @parameterized.expand(
        [
            ("delete", "delete from test.{} where edigest = '{}'"),
            ("select", "select * from test.{} where edigest = '{}'"),
        ]
    )
    def test_edigest_is_checked_for_every_command(self, _, cmd):
        cmd = cmd.format(utils.SET_NAME, "A" * 65536)
        self.assertIn(
            "Edigest must be 28 base64 characters, got 65536", self.run_aql(cmd)
        )

    @parameterized.expand(
        [
            (cmd_name + " " + kind + " " + val_name, cmd, kind, value)
            for cmd_name, cmd in DIGEST_COMMANDS
            for kind in ["digest", "edigest"]
            for val_name, value in NON_STRING_VALUES
        ]
    )
    def test_digest_must_be_a_string(self, _, cmd, kind, value):
        cmd = cmd.format(kind, value).replace("test.SET", "test." + utils.SET_NAME)
        self.assertIn(
            "{} must be a string".format(kind.capitalize()), self.run_aql(cmd)
        )

    @parameterized.expand(
        [
            ("true", "true"),
            ("false", "false"),
            ("float", "1.5"),
        ]
    )
    def test_select_by_pk_bad_type(self, _, value):
        cmd = "select * from test.{} where pk = {}".format(utils.SET_NAME, value)
        self.assertIn(
            "Primary key must be a string or an integer", self.run_aql(cmd)
        )

    def test_select_by_null_pk(self):
        cmd = "select * from test.{} where pk = null".format(utils.SET_NAME)
        self.assertIn(
            "Primary key must be a string or an integer", self.run_aql(cmd)
        )

    @parameterized.expand(
        [
            ("null", "null"),
            ("true", "true"),
            ("false", "false"),
            ("float", "1.5"),
        ]
    )
    def test_execute_on_pk_bad_type(self, _, value):
        cmd = "execute test1.foo() on test.{} where pk = {}".format(
            utils.SET_NAME, value
        )
        self.assertIn(
            "Primary key must be a string or an integer", self.run_aql(cmd)
        )

    @parameterized.expand(
        [
            ("true", "true"),
            ("false", "false"),
        ]
    )
    def test_insert_with_pk_bad_type(self, _, value):
        cmd = "insert into test.{} (PK, str) values ({}, '1')".format(
            utils.SET_NAME, value
        )
        self.assertIn(
            "Primary key must be a string or an integer", self.run_aql(cmd)
        )

    def test_insert_with_null_pk(self):
        cmd = "insert into test.{} (PK, str) values (null, '1')".format(
            utils.SET_NAME
        )
        self.assertIn("Primary key must not be null", self.run_aql(cmd))

    def test_insert_with_float_pk(self):
        cmd = "insert into test.{} (PK, str) values (1.5, '1')".format(
            utils.SET_NAME
        )
        self.assertIn("PK cannot be floating point value", self.run_aql(cmd))

    @parameterized.expand(BIN_NAME_COMMANDS)
    def test_long_bin_name(self, _, cmd):
        cmd = cmd.format("b" * 16).replace("test.SET", "test." + utils.SET_NAME)
        self.assertIn("Bin name is too long", self.run_aql(cmd))

    @parameterized.expand(BIN_NAME_COMMANDS)
    def test_empty_bin_name(self, _, cmd):
        cmd = cmd.format("''").replace("test.SET", "test." + utils.SET_NAME)
        self.assertIn("Bin name is empty", self.run_aql(cmd))

    @parameterized.expand(BIN_NAME_COMMANDS)
    def test_max_length_bin_name_is_accepted(self, _, cmd):
        cmd = cmd.format("b" * 15).replace("test.SET", "test." + utils.SET_NAME)
        self.assertEqual(self.run_aql(cmd), "")


if __name__ == "__main__":
    unittest.main()
