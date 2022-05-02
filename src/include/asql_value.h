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

#include <aerospike/as_arraylist.h>
#include <aerospike/as_vector.h>
#include <aerospike/as_error.h>

//==========================================================
// Typedefs & constants.
//

// NB: Types GEOJSON, JSON, and STRING are represented internally as AS_STRINGs.
// Before sending to the server via the C Client, a JSON value is converted
// from a string to a CDT (i.e., list, map, etc.)
typedef enum asql_value_type_e {
	ASQL_VALUE_TYPE_NONE,
	ASQL_VALUE_TYPE_INT,
	ASQL_VALUE_TYPE_FLOAT,
	ASQL_VALUE_TYPE_GEOJSON,
	ASQL_VALUE_TYPE_JSON,
	ASQL_VALUE_TYPE_LIST,
	ASQL_VALUE_TYPE_MAP,
	ASQL_VALUE_TYPE_STRING,
	ASQL_VALUE_TYPE_DIGEST,
	ASQL_VALUE_TYPE_EDIGEST
} asql_value_type_t;

typedef struct {
	as_val_t type;        // Type for the C Client
	asql_value_type_t vt; // ASQL-internal type
	union {               // Parsed representation of value
		double dbl;
		int64_t i64;
		char* str;
	} u;
} asql_value;

typedef char* asql_name;


//==========================================================
// Public API.
//

int asql_set_args(as_error* err, as_vector *udfargs, as_arraylist* arglist);
int asql_parse_value_as(char* s, asql_value* value, asql_value_type_t vtype);
void asql_free_value(void*);
char* asql_val_str(const as_val* val);
asql_value_type_t asql_value_type_from_type_name(char* str);
