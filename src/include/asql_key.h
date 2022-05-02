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

#include <aerospike/as_hashmap.h>
#include <aerospike/as_record.h>

//==========================================================
// Typedefs & constants.
//

typedef enum {
	DELETE_OP = 0,
	WRITE_OP = 1,
	READ_OP = 2
} pk_op;

typedef struct pk_config {
	atype type;
	asql_optype optype;
	bool explain;

	pk_op op;

	asql_name ns;
	asql_name set;

	insert_param i;
	select_param s;
	udf_param u;
	asql_value key;
} pk_config;


//=========================================================
// Public API.
//

int asql_key(asql_config* c, aconfig* ac);
void asql_record_set_renderer(as_record* rec, as_hashmap* m, char* bin_name, as_val* val);
