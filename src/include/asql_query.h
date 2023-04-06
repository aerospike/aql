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


//==========================================================
// Typedefs & constants.
//

typedef enum asql_query_type_e {
	ASQL_QUERY_TYPE_NONE,
	ASQL_QUERY_TYPE_EQUALITY,
	ASQL_QUERY_TYPE_RANGE,
	ASQL_QUERY_TYPE_WITHIN, // GeoJSON Point-Within-Region Lookup
	ASQL_QUERY_TYPE_CONTAINS // GeoJSON Region-Cointains-Point Lookup
} asql_query_type_t;

typedef struct {
	as_val_t type;
	asql_query_type_t qtype; // Geospatial query type
	asql_value beg;  // NUMERIC Range Lookup
	asql_value end;
	asql_name ibname; //Lookup w/ IndexedBinName (e.g. WHERE ibn = 4)
} asql_where;

typedef struct sk_config {
	atype type;
	asql_optype optype;

	asql_name ns;
	asql_name set;

	select_param s;
	udf_param u;

	asql_name itype;

	asql_where where;
	asql_where* where2;

	asql_value* limit;
} sk_config;

//=========================================================
// Public API.
//

int asql_query(asql_config* c, aconfig* ac);
