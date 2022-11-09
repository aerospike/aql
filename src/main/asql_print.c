/*
 * Copyright 2015-2022 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

//==========================================================
// Includes.
//

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>

#include <asql.h>
#include <asql_conf.h>


//==========================================================
// Typedefs & constants.
//

typedef void (* print_fn)();

typedef struct {
	char* cmd;
	print_fn fn;
} print_entry;

//=========================================================
// Forward Declarations.
//

static void print_print_help();
static void print_ddl_help();
static void print_dml_help();
static void print_query_help();
static void print_admin_help();
static void print_setting_help();

static char* get_subcmd(char* cmd);

// C Client Version:
// [Defined in the Aerospike C Client, but not declared in any header file.]
extern char* aerospike_client_version;

const print_entry print_table[ASQL_OP_MAX] = {
	{ "EXPLAIN", print_query_help },
	{ "INSERT", print_dml_help },
	{ "DELETE", print_dml_help },
	{ "TRUNCATE", print_dml_help },
	{ "EXECUTE", print_dml_help },

	{ "SELECT", print_query_help },
	{ "AGGREGATE", print_query_help },

	{ "DROP", print_ddl_help },
	{ "CREATE", print_ddl_help },
	{ "GRANT", print_ddl_help },
	{ "REVOKE", print_ddl_help },
	{ "REGISTER", print_ddl_help },
	{ "REMOVE", print_ddl_help },


	{ "SHOW", print_admin_help },
	{ "DESC", print_admin_help },
	{ "STAT", print_admin_help },

	{ "KILL_QUERY", print_admin_help },
	{ "KILL_SCAN", print_admin_help },

	{ "RUN", print_admin_help },
	{ "ASINFO", print_admin_help },

	{ "SET", print_setting_help },
	{ "GET", print_setting_help },
	{ "RESET", print_setting_help },
	{ "PRINT", print_print_help },
	{ "SYSTEM", print_admin_help }
};


//=========================================================
// Public API.
//


//TODO: update to reflect website's documentation
void
print_version()
{
	fprintf(stdout, "Aerospike Query Client\n");
	fprintf(stdout, "Version %s\n", AQL_VERSION);
	fprintf(stderr, "C Client Version %s\n", aerospike_client_version);
	fprintf(stdout, "Copyright 2012-2021 Aerospike. All rights reserved.\n");
}

void
print_help(const char* cmd, bool bShowOptions)
{
	char* cmdstr[1] = { "aql" };

	if (!cmd || strlen(cmd) == strlen("HELP")) {
		if (bShowOptions) {
			print_config_help(1, cmdstr);
		}
		else {
			print_version();
			fprintf(stdout, "\n");
		}

		fprintf(stdout, "COMMANDS\n");
		print_ddl_help();
		print_dml_help();
		print_query_help();
		print_admin_help();
		print_setting_help();
		fprintf(stdout, "    OTHER\n");
		print_print_help();
		fprintf(stdout, "        HELP\n");
		fprintf(stdout, "        QUIT|EXIT|Q\n");
		fprintf(stdout, "\n\n");
	}
	else {
		char* subcmd = get_subcmd((char* )&cmd[4]);

		uint8_t i = 0;
		for (i = 0; i < ASQL_OP_MAX; i++) {
			if (!strcasecmp(subcmd, print_table[i].cmd)) {
				print_table[i].fn();
				break;
			}
		}

		if (i == ASQL_OP_MAX) {
			fprintf(stdout, "\nERROR: 404: COMMAND NOT FOUND : %s\n", subcmd);
		}
	}
}


//=========================================================
// Local API.
//

static void
print_print_help()
{
	fprintf(stdout, "        PRINT <string_to_echo> (DEPRECATED)\n");
	fprintf(stdout, "            Echo the given input.\n");
	fprintf(stdout, "            Example:\n");
	fprintf(stdout, "            \n");
	fprintf(stdout, "                PRINT Hello world.\n");
	fprintf(stdout, "                Hello world.\n");
	fprintf(stdout, "            \n");
}

static void
print_ddl_help()
{
	fprintf(stdout, "  DDL (DEPRECATED)\n");
	fprintf(stdout, "      CREATE INDEX <index> ON <ns>[.<set>] (<bin>) NUMERIC|STRING|GEO2DSPHERE\n");
	fprintf(stdout, "      CREATE LIST/MAPKEYS/MAPVALUES INDEX <index> ON <ns>[.<set>] (<bin>) NUMERIC|STRING|GEO2DSPHERE\n");
	fprintf(stdout, "      DROP INDEX <ns>[.<set>] <index>\n");
	fprintf(stdout, "      Examples:\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          CREATE INDEX idx_foo ON test.demo (foo) NUMERIC\n");
	fprintf(stdout, "          DROP INDEX test.demo idx_foo\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "  MANAGE UDFS\n");
	fprintf(stdout, "      REGISTER MODULE '<filepath>'\n");
	fprintf(stdout, "      REMOVE MODULE <filename>\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          <filepath> is file path to the UDF module(in single quotes).\n");
	fprintf(stdout, "          <filename> is file name of the UDF module.\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "      Examples:\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          REGISTER MODULE '~/test.lua' \n");
	fprintf(stdout, "          REMOVE MODULE test.lua\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "  USER ADMINISTRATION (DEPRECATED)\n");
	fprintf(stdout, "      CREATE USER <user> PASSWORD <password> ROLE[S] <role1>,<role2>...\n");
	fprintf(stdout, "          pre-defined roles: read|read-write|read-write-udf|sys-admin|user-admin\n");
	fprintf(stdout, "      DROP USER <user>\n");
	fprintf(stdout, "      SET PASSWORD <password> [FOR <user>]\n");
	fprintf(stdout, "      GRANT ROLE[S] <role1>,<role2>... TO <user>\n");
	fprintf(stdout, "      REVOKE ROLE[S] <role1>,<role2>... FROM <user>\n");
	fprintf(stdout, "      CREATE ROLE <role> PRIVILEGE[S] <priv1[.ns1[.set1]]>,<priv2[.ns2[.set2]]>... WHITELIST addr1,addr2...\n");
	fprintf(stdout, "          priv: read|read-write|read-write-udf|sys-admin|user-admin|data-admin\n");
	fprintf(stdout, "          ns:   namespace.  Applies to all namespaces if not set.\n");
	fprintf(stdout, "          set:  set name.  Applies to all sets within namespace if not set.\n");
	fprintf(stdout, "                sys-admin, user-admin and data-admin can't be qualified with namespace or set.\n");
	fprintf(stdout, "          addr: IP address.\n");
	fprintf(stdout, "      DROP ROLE <role>\n");
	fprintf(stdout, "      GRANT PRIVILEGE[S] <priv1[.ns1[.set1]]>,<priv2[.ns2[.set2]]>... TO <role>\n");
	fprintf(stdout, "      REVOKE PRIVILEGE[S] <priv1[.ns1[.set1]]>,<priv2[.ns2[.set2]]>... FROM <role>\n");
	fprintf(stdout, "      SET WHITELIST addr1,addr2... FOR <role>\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "      \n");
}

static void
print_dml_help()
{
	fprintf(stdout, "  DML\n");
	fprintf(stdout, "      INSERT INTO <ns>[.<set>] (PK, <bins>) VALUES (<key>, <values>)\n");
	fprintf(stdout, "      DELETE FROM <ns>[.<set>] WHERE PK = <key>\n");
	fprintf(stdout, "      TRUNCATE <ns>[.<set>] [upto <LUT>] \n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          <ns> is the namespace for the record.\n");
	fprintf(stdout, "          <set> is the set name for the record.\n");
	fprintf(stdout, "          <key> is the record's primary key.\n");
	fprintf(stdout, "          <bins> is a comma-separated list of bin names.\n");
	fprintf(stdout, "          <values> is comma-separated list of bin values, which may include type cast expressions. Set to NULL (case insensitive & w/o quotes) to delete the bin.\n");
	fprintf(stdout, "          <LUT> is last update time upto which set or namespace needs to be truncated. LUT is either nanosecond since Unix epoch like 1513687224599000000 or in date string in format like \"Dec 19 2017 12:40:00\".\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "        Type Cast Expression Formats:\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "            CAST(<Value> AS <TypeName>)\n");
	fprintf(stdout, "            <TypeName>(<Value>)\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "        Supported AQL Types:\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "              Bin Value Type                    Equivalent Type Name(s)\n");
	fprintf(stdout, "           ===============================================================\n");
	fprintf(stdout, "            Integer                           DECIMAL, INT, NUMERIC\n");
	fprintf(stdout, "            Floating Point                    FLOAT, REAL\n");
	fprintf(stdout, "            Aerospike CDT (List, Map, etc.)   JSON\n");
	fprintf(stdout, "            Aerospike List                    LIST\n");
	fprintf(stdout, "            Aerospike Map                     MAP\n");
	fprintf(stdout, "            GeoJSON                           GEOJSON\n");
	fprintf(stdout, "            String                            CHAR, STRING, TEXT, VARCHAR\n");
	fprintf(stdout, "           ===============================================================\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "        [Note:  Type names and keywords are case insensitive.]\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "      Examples:\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          INSERT INTO test.demo (PK, foo, bar, baz) VALUES ('key1', 123, 'abc', true)\n");
	fprintf(stdout, "          INSERT INTO test.demo (PK, foo, bar, baz) VALUES ('key1', CAST('123' AS INT), JSON('{\"a\": 1.2, \"b\": [1, 2, 3]}'), BOOL(1))\n");
	fprintf(stdout, "          INSERT INTO test.demo (PK, foo, bar) VALUES ('key1', LIST('[1, 2, 3]'), MAP('{\"a\": 1, \"b\": 2}'), CAST(0 as BOOL))\n");
	fprintf(stdout, "          INSERT INTO test.demo (PK, gj) VALUES ('key1', GEOJSON('{\"type\": \"Point\", \"coordinates\": [123.4, -56.7]}'))\n");
	fprintf(stdout, "          DELETE FROM test.demo WHERE PK = 'key1'\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "  INVOKING UDFS\n");
	fprintf(stdout, "      EXECUTE <module>.<function>(<args>) ON <ns>[.<set>]\n");
	fprintf(stdout, "      EXECUTE <module>.<function>(<args>) ON <ns>[.<set>] WHERE PK = <key>\n");
	fprintf(stdout, "      EXECUTE <module>.<function>(<args>) ON <ns>[.<set>] WHERE <bin> = <value>\n");
	fprintf(stdout, "      EXECUTE <module>.<function>(<args>) ON <ns>[.<set>] WHERE <bin> BETWEEN <lower> AND <upper>\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          <module> is UDF module containing the function to invoke.\n");
	fprintf(stdout, "          <function> is UDF to invoke.\n");
	fprintf(stdout, "          <args> is a comma-separated list of argument values for the UDF.\n");
	fprintf(stdout, "          <ns> is the namespace for the records to be queried.\n");
	fprintf(stdout, "          <set> is the set name for the record to be queried.\n");
	fprintf(stdout, "          <key> is the record's primary key.\n");
	fprintf(stdout, "          <bin> is the name of a bin.\n");
	fprintf(stdout, "          <value> is the value of a bin.\n");
	fprintf(stdout, "          <lower> is the lower bound for a numeric range query.\n");
	fprintf(stdout, "          <upper> is the lower bound for a numeric range query.\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "      Examples:\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          EXECUTE myudfs.udf1(2) ON test.demo\n");
	fprintf(stdout, "          EXECUTE myudfs.udf1(2) ON test.demo WHERE PK = 'key1'\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "      \n");
}

static void
print_query_help()
{
	fprintf(stdout, "  QUERY\n");
	fprintf(stdout, "      SELECT <bins> FROM <ns>[.<set>]\n");
	fprintf(stdout, "      SELECT <bins> FROM <ns>[.<set>] WHERE <bin> = <value>\n");
	fprintf(stdout, "      SELECT <bins> FROM <ns>[.<set>] WHERE <bin> BETWEEN <lower> AND <upper>\n");
	fprintf(stdout, "      SELECT <bins> FROM <ns>[.<set>] WHERE PK = <key>\n");
	fprintf(stdout, "      SELECT <bins> FROM <ns>[.<set>] IN <indextype> WHERE <bin> = <value>\n");
	fprintf(stdout, "      SELECT <bins> FROM <ns>[.<set>] IN <indextype> WHERE <bin> BETWEEN <lower> AND <upper>\n");
	fprintf(stdout, "      SELECT <bins> FROM <ns>[.<set>] IN <indextype> WHERE <bin> CONTAINS <GeoJSONPoint>\n");
	fprintf(stdout, "      SELECT <bins> FROM <ns>[.<set>] IN <indextype> WHERE <bin> WITHIN <GeoJSONPolygon>\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          <ns> is the namespace for the records to be queried.\n");
	fprintf(stdout, "          <set> is the set name for the record to be queried.\n");
	fprintf(stdout, "          <key> is the record's primary key.\n");
	fprintf(stdout, "          <bin> is the name of a bin.\n");
	fprintf(stdout, "          <value> is the value of a bin.\n");
	fprintf(stdout, "          <indextype> is the type of a index user wants to query. (LIST/MAPKEYS/MAPVALUES)\n");
	fprintf(stdout, "          <bins> can be either a wildcard (*) or a comma-separated list of bin names.\n");
	fprintf(stdout, "          <lower> is the lower bound for a numeric range query.\n");
	fprintf(stdout, "          <upper> is the lower bound for a numeric range query.\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "      Examples:\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          SELECT * FROM test.demo\n");
	fprintf(stdout, "          SELECT * FROM test.demo WHERE PK = 'key1'\n");
	fprintf(stdout, "          SELECT foo, bar FROM test.demo WHERE PK = 'key1'\n");
	fprintf(stdout, "          SELECT foo, bar FROM test.demo WHERE foo = 123\n");
	fprintf(stdout, "          SELECT foo, bar FROM test.demo WHERE foo BETWEEN 0 AND 999\n");
	fprintf(stdout, "          SELECT * FROM test.demo WHERE gj CONTAINS CAST('{\"type\": \"Point\", \"coordinates\": [0.0, 0.0]}' AS GEOJSON)\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "  AGGREGATION\n");
	fprintf(stdout, "      AGGREGATE <module>.<function>(<args>) ON <ns>[.<set>]\n");
	fprintf(stdout, "      AGGREGATE <module>.<function>(<args>) ON <ns>[.<set>] WHERE <bin> = <value>\n");
	fprintf(stdout, "      AGGREGATE <module>.<function>(<args>) ON <ns>[.<set>] WHERE <bin> BETWEEN <lower> AND <upper>\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          <module> is UDF module containing the function to invoke.\n");
	fprintf(stdout, "          <function> is UDF to invoke.\n");
	fprintf(stdout, "          <args> is a comma-separated list of argument values for the UDF.\n");
	fprintf(stdout, "          <ns> is the namespace for the records to be queried.\n");
	fprintf(stdout, "          <set> is the set name for the record to be queried.\n");
	fprintf(stdout, "          <bin> is the name of a bin.\n");
	fprintf(stdout, "          <value> is the value of a bin.\n");
	fprintf(stdout, "          <lower> is the lower bound for a numeric range query.\n");
	fprintf(stdout, "          <upper> is the lower bound for a numeric range query.\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "      Examples:\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          AGGREGATE myudfs.udf2(2) ON test.demo WHERE foo = 123\n");
	fprintf(stdout, "          AGGREGATE myudfs.udf2(2) ON test.demo WHERE foo BETWEEN 0 AND 999\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "  EXPLAIN\n");
	fprintf(stdout, "      EXPLAIN SELECT * FROM <ns>[.<set>] WHERE PK = <key>\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          <ns> is the namespace for the records to be queried.\n");
	fprintf(stdout, "          <set> is the set name for the record to be queried.\n");
	fprintf(stdout, "          <key> is the record's primary key.\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "      Examples:\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          EXPLAIN SELECT * FROM test.demo WHERE PK = 'key1'\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "      \n");
}

static void
print_admin_help()
{
	fprintf(stdout, "  INFO\n");
	fprintf(stdout, "      SHOW NAMESPACES | SETS | BINS | INDEXES\n" );
	fprintf(stdout, "      (DEPRECATED) SHOW SCANS | QUERIES\n" );
	fprintf(stdout, "      (DEPRECATED) STAT NAMESPACE <ns> | INDEX <ns> <indexname>\n");
	fprintf(stdout, "      (DEPRECATED) STAT SYSTEM\n");
	fprintf(stdout, "      (DEPRECATED) ASINFO <ASInfoCommand>\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "  JOB MANAGEMENT (DEPRECATED)\n");
	fprintf(stdout, "      KILL_QUERY <transaction_id>\n");
	fprintf(stdout, "      KILL_SCAN <scan_id>\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "  USER ADMINISTRATION (DEPRECATED)\n");
	fprintf(stdout, "      SHOW USER [<user>]\n");
	fprintf(stdout, "      SHOW USERS\n");
	fprintf(stdout, "      SHOW ROLE <role>\n");
	fprintf(stdout, "      SHOW ROLES\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "  MANAGE UDFS\n");
	fprintf(stdout, "      SHOW MODULES\n");
	fprintf(stdout, "      DESC MODULE <filename>\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          <filepath> is file path to the UDF module(in single quotes).\n");
	fprintf(stdout, "          <filename> is file name of the UDF module.\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "      Examples:\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "          SHOW MODULES\n");
	fprintf(stdout, "          DESC MODULE test.lua\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "  RUN <filepath>\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "  SYSTEM <bash command>\n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "      \n");

}

static void
print_setting_help()
{
	fprintf(stdout, "  SETTINGS\n");
	print_option_help();
	fprintf(stdout, "  \n");
	fprintf(stdout, "      \n");
	fprintf(stdout, "      To get the value of a setting, run:\n");
	fprintf(stdout, "      	\n");
	fprintf(stdout, "          aql> GET <setting>\n");
	fprintf(stdout, "      	\n");
	fprintf(stdout, "      To set the value of a setting, run:\n");
	fprintf(stdout, "      	\n");
	fprintf(stdout, "          aql> SET <setting> <value>\n");
	fprintf(stdout, "      	\n");
	fprintf(stdout, "      To reset the value of a setting back to default, run:\n");
	fprintf(stdout, "      	\n");
	fprintf(stdout, "          aql> RESET <setting>\n");
	fprintf(stdout, "      	\n");
	fprintf(stdout, "      	\n");
}

static char*
get_subcmd(char* cmd)
{
	// skip empty spaces
	while (cmd[0] != '\0' && cmd[0] == ' ') {
		cmd++;
	}

	char* subcmd = cmd;

	// Skip non-empty character.
	while (cmd[0] != '\0' && cmd[0] != ' ') {
		cmd++;
	}

	// Consider first string with no space
	cmd[0] = '\0';
	return subcmd;
}
