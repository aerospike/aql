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

#pragma once


//==========================================================
// Includes.
//
#include <stddef.h>

#include <aerospike/aerospike.h>

#include <asql_value.h>
#include <renderer.h>


//==========================================================
// Typedefs & constants.
//

#define ENABLE_ASQL_CODE 0  //set to 1 to enable undeveloped code segments

typedef bool (* validate_fn)(void* in);

// ENUM
//
typedef enum {
	TABLE = 0,
	JSON = 1,
	MUTE = 2,
	RAW = 3
} output_t;

typedef enum {
	SECONDARY_INDEX_OP = 0,
	PRIMARY_INDEX_OP,
	INFO_OP,
	SCAN_OP,
	RUNFILE_OP,
	TRUNCATE_OP,
	OP_MAX = 6
} atype;

typedef enum {
	ASQL_OP_EXPLAIN = 0,
	ASQL_OP_INSERT,
	ASQL_OP_DELETE,
	ASQL_OP_TRUNCATE,
	ASQL_OP_EXECUTE,

	ASQL_OP_SELECT,
	ASQL_OP_AGGREGATE,

	ASQL_OP_REGISTER,
	ASQL_OP_REMOVE,

	ASQL_OP_SHOW,
	ASQL_OP_DESC,

	ASQL_OP_RUN,

	ASQL_OP_SET,
	ASQL_OP_GET,
	ASQL_OP_RESET,

	ASQL_OP_MAX
} asql_optype;

typedef struct asql_base_config {

	char* host;
	char* tls_name;
	bool use_services_alternate;
	int port;

	char* user;
	char* password;
	as_config_tls tls;
	int threadpoolsize;

	// Env specific config with set option.
	bool verbose;
	bool echo;
	output_t outputmode;
	bool outputtypes;
	int timeout_ms;
	int socket_timeout_ms;
	char* lua_userpath;
	char* auth_mode;
} asql_base_config;

typedef struct asql_config {

	// Base Config
	asql_base_config base;

	// Operation specific config with set option.
	int record_ttl_sec;
	bool record_print_metadata;
	bool key_send;
	bool durable_delete;
	int scan_records_per_second;
	bool no_bins;


} asql_config;

typedef struct aconfig {
	atype type;
	asql_optype optype;
} aconfig;

typedef struct {
	asql_config* c;
	aconfig* ac;
	bool backout;
} asql_op;

typedef struct {
	as_vector* bnames;
	as_vector* values;
} insert_param;

typedef struct {
	asql_name udfpkg;
	asql_name udfname;
	as_vector* params;
} udf_param;

typedef struct {
	as_vector* bnames;
} select_param;

typedef struct {
  int value;
  const char* name;
} map_enum_string;

typedef enum {
	ASQL_SET_OPTION_TYPE_BOOL,
	ASQL_SET_OPTION_TYPE_INT,
	ASQL_SET_OPTION_TYPE_ENUM,
	ASQL_SET_OPTION_TYPE_STRING,
} asql_set_option_type;

typedef struct asql_set_option_s {
	asql_set_option_type type;
	long offset;
	const char* name;
	const char* help;

	const char* default_string;
	int default_value;
	map_enum_string* enum_map;
	validate_fn validate;
} asql_set_option;

typedef struct runfile_config {
  atype type;
  asql_optype optype;
  char* fname;
} runfile_config;


//==========================================================
// Globals.
//

extern char* DEFAULTPASSWORD;
extern asql_config* g_config;
extern aerospike* g_aerospike;

//==========================================================
// Public API.
//

bool parse_and_run(asql_config* c, char* cmd);
bool parse_and_run_colon_delim(asql_config* c, char* cmd);
const char* map_enum_to_string(map_enum_string map[], int value);
