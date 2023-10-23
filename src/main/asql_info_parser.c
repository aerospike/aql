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

#include <errno.h>
#include <stdlib.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_info.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_string.h>
#include <aerospike/as_udf.h>

#include <citrusleaf/cf_b64.h>

#include <asql.h>
#include <asql_info.h>

#include "renderer/table.h"


//==========================================================
// Typedefs & constants.
//


//==========================================================
// Forward Declarations.
//

static bool value_parser(as_hashmap* map, const char* req, const as_val* name, const char* value);
static bool pair_parser(as_hashmap* map, const char* req, const char* pair);
static char* pair_decode_parser(as_hashmap* map, const char* req, const char* pair);

//==========================================================
// Public API.
//

// NB: Only for udf-get
bool
udf_get_res_parser(as_vector* result, const as_node* node, const char* req, const char* res)
{
	as_hashmap* map = as_hashmap_new(128);

	char* pair_save = NULL;
	char* pair = strtok_r((char*)res, ";\n\t", &pair_save);

	while (pair) {
		if (!strncasecmp(pair, "content", 7)) {
			pair_decode_parser(map, req, pair);
		}
		else {
			pair_parser(map, req, pair);
		}
		pair = strtok_r(NULL, ";\n\t", &pair_save);
	}

	as_vector_append(result, (void *)&map);

	return true;
}

bool
bins_res_parser(as_vector* result, const as_node* node, const char* req, const char* res)
{
	char* entry_save = NULL;
	char* entry = strtok_r((char*)res, ";\n", &entry_save);
	while (entry) {
		char* L = NULL;
		char* R = NULL;

		char* namespace = NULL;
		char* num_bin_names = NULL;
		char* bin_names_quota = NULL;

		L = entry;
		R = strchr(entry, ':');
		if (strcmp(R, ":[single-bin]") == 0) {
			R++;
			as_hashmap* map = as_hashmap_new(64);
			as_string* key_namespace = as_string_new("namespace", false);
			as_string* key_count = as_string_new("count", false);
			as_string* key_bin = as_string_new("bin", false);

			char* ns_save = NULL;
			namespace = strtok_r((char*)L, ":", &ns_save);
			as_hashmap_set(map, (as_val*)key_namespace,
			        (as_val*)as_string_new(namespace, false));

			as_hashmap_set(map, (as_val*)key_bin,
			               (as_val*)as_string_new(R, false));

			as_hashmap_set(map, (as_val*)key_count,
			               (as_val*)as_integer_new(1));

			as_vector_append(result, (void*)&map);
			goto next_tok_parse;
		}

		*R = '\0';

		namespace = L;

		L = ++R;
		R = strchr(L, '=');

		L = ++R;
		R = strchr(L, ',');
		*R = '\0';

		num_bin_names = L;

		L = ++R;
		R = strchr(L, '=');

		L = ++R;
		R = strchr(L, ',');

		if (!R) {
			goto next_tok_parse;
		}

		bin_names_quota = L;

		*R = '\0';
		L = R + 1;
		as_string* key_namespace = as_string_new("namespace", false);
		as_string* key_count = as_string_new("count", false);
		as_string* key_quota = as_string_new("quota", false);
		as_string* key_bin = as_string_new("bin", false);

		char* bin_save = NULL;
		char* bin = strtok_r((char*)L, ",", &bin_save);
		while (bin) {
			as_hashmap* map = as_hashmap_new(64);	

			as_hashmap_set(map, (as_val*)as_val_reserve(key_namespace),
					(as_val*)as_string_new(namespace, false));

			as_hashmap_set(map, (as_val *)as_val_reserve(key_bin),
						   (as_val *)as_string_new(bin, false));

			as_hashmap_set(map, (as_val*)as_val_reserve(key_count),
					(as_val*)as_integer_new(atoi(num_bin_names)));

			as_hashmap_set(map, (as_val*)as_val_reserve(key_quota),
					(as_val*)as_integer_new(atoi(bin_names_quota)));

			as_vector_append(result, (void*)&map);
			bin = strtok_r(NULL, ",", &bin_save);
		}

		as_string_destroy(key_namespace);
		as_string_destroy(key_count);
		as_string_destroy(key_quota);
		as_string_destroy(key_bin);

next_tok_parse:

		entry = strtok_r(NULL, ";\n", &entry_save);
	}

	return true;
}

bool
list_udf_parser(as_vector* result, const as_node* node, const char* req, const char* res)
{
	char* save = NULL;
	char* entry = strtok_r((char*)res, ";\n", &save);

	while (entry) {
		as_hashmap* map = as_hashmap_new(128);

		if (strchr(entry, ',') != NULL) {
			char* pair_save = NULL;
			char* pair = strtok_r(entry, ",", &pair_save);
			while (pair) {
				pair_parser(map, req, pair);
				pair = strtok_r(NULL, ",", &pair_save);
			}

		}
		else {
			as_string* str = as_string_new((char*)req, false);
			value_parser(map, req, (as_val*)str, strdup(entry));
		}

		as_vector_append(result, (void *)&map);
		entry = strtok_r(NULL, ";\n", &save);
	}

	return true;
}

bool
list_res_parser(as_vector* result, const as_node* node, const char* req, const char* res)
{
	as_vector* parsed_resp = result;
	char* save = NULL;
	char* orig_res = strdup(res);
	char* entry = strtok_r((char*)res, ";\n", &save);

	while (entry) {
		as_hashmap* map = as_hashmap_new(128);

		if (strchr(entry, ':') != NULL) {
			// For parsing: "sindex/test\tns=test:indexname=binInt-set-index:set=queryset:bin=binInt:type=numeric:indextype=default:context=null:state=RW;ns=test:indexname=binIntMod-set-index:set=queryset:bin=binIntMod:type=numeric:indextype=default:context=null:state=RW\n"
			char* pair_save = NULL;
			char* pair = strtok_r(entry, ":", &pair_save);
			while (pair) {
				pair_parser(map, req, pair);
				pair = strtok_r(NULL, ":", &pair_save);
			}
		}
		else {
			if (strchr(entry, '=') != NULL) {
				// For parsing: "entries=501;memory_used=16777216;entries_per_bval=1;entries_per_rec=1;load_pct=100;load_time=0;stat_gc_recs=0\n"
				char* pair_save = NULL;
				char* pair = strtok_r(orig_res, ";", &pair_save);
				while (pair) {
					pair_parser(map, req, pair);
					pair = strtok_r(NULL, ";", &pair_save);
				}
				as_vector_append(parsed_resp, (void *)&map);
				break;
			}
			else {	
				// For parsing: "test;bar\n"
				as_string* str= as_string_new((char*)req, false);
				value_parser(map, req, (as_val*)str, strdup(entry));
			}
		}

		as_vector_append(parsed_resp, (void *)&map);
		entry = strtok_r(NULL, ";\n", &save);
	}

	free(orig_res);
	return true;
}

char* 
info_res_split(char* res) {
	if (res == NULL || strlen(res) == 0)
	{
		return NULL;
	}

	char* resp = strchr(res, '\t');

	if (resp == NULL || strlen(resp) == 0)
	{
		return NULL;
	}

	resp++;

	if (resp == NULL || strlen(resp) == 0)
	{
		return NULL;
	}

	return resp;
}

static bool
value_parser(as_hashmap* map, const char* req, const as_val* name,
             const char* value)
{
	if (!value){
		as_hashmap_set(map, (as_val*)name, (as_val*)&as_nil);
		return true;
	}

	char* endptr = NULL;
	unsigned long long val = 0;

	val = strtoull(value, &endptr, 10);
	if (*endptr == '\0' && errno == 0) {
		// NB: Info only sends back unsigned int.
		char* str = (char *) malloc(sizeof(char) * 32);
		memset(str, 0, 32);
		sprintf(str, "%" PRIu64, (uint64_t)val);
		as_hashmap_set(map, (as_val*)name,
				(as_val*)as_string_new(str, true));
	}
	else {
		as_hashmap_set(map, (as_val*)name,
					   (as_val*)as_string_new((char*)value, true));
	}
	return true;
}

//==========================================================
// Local Helpers.
//

static char*
pair_decode_parser(as_hashmap* map, const char* req, const char* pair)
{
	char* name = (char*)pair;
	char* delim = strstr(name, "=");
	char* value = NULL;

	if (delim) {
		delim[0] = '\0';
		value = delim + 1;
	}

	name = strdup(name);
	value = strdup(value);

	uint32_t len = strlen(value);
	uint8_t* decode_buf = (uint8_t*)malloc(cf_b64_decoded_buf_size(len));
	cf_b64_decode(value, len, decode_buf, NULL);

	value_parser(map, req, (as_val*)as_string_new(name, true),
			(char*)decode_buf);

	return (char*)decode_buf;
}

static bool
pair_parser(as_hashmap* map, const char* req, const char* pair)
{
	char* name = (char *)pair;
	char* delim = strstr(name, "=");
	char* value = NULL;

	if (delim == NULL) {
		return false;
	}

	delim[0] = '\0';
	value = delim + 1;
	
	name = strdup(name);
	value = strdup(value);

	if (strcasecmp(name, "from") == 0) {
		// server version above 4.5.2 returns new 'from' field which has <IP>+<PORT> format,
		// replace '+' with ':' to show regular <IP>:<PORT> format

		for(int i = 0; i < strlen(value); i++)
		{
			if(value[i] == '+')
			{
				value[i] = ':';
			}
		}
	}

	value_parser(map, req, (as_val*)as_string_new(name, true), value);

	return true;
}