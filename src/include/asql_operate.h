/*
 * Copyright 2015-2021 Aerospike, Inc.
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
// Typedefs & constants.
//

typedef struct {
	char* cmd;
	char* bname;
	as_vector* params;
	as_vector* policy;
} binop;

typedef struct operate_config_s {
	atype type;
	asql_optype optype;

	char* ns;
	char* set;
	asql_value key;
	as_vector* binops;
} operate_config;


//==========================================================
// Public API.
//

int aql_operate(asql_config* c, aconfig* ac);
void asql_print_ops(char* indent);
