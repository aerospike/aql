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
// Includes.
//



//==========================================================
// Typedefs & constants.
//

typedef struct scan_config {
	atype type;
	asql_optype optype;

	asql_name ns;
	asql_name set;

	select_param s;
	udf_param u;
} scan_config;


//=========================================================
// Public API.
//

int asql_scan(asql_config* c, aconfig* ac);
