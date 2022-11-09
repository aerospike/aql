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


#include <asql_log.h>
#include <citrusleaf/cf_b64.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_rec.h>
#include <aerospike/as_stringmap.h>

#include <aerospike/as_arraylist.h>
#include <aerospike/as_list.h>

#include <renderer.h>
#include <json.h>
#include <jansson.h>

#include <asql.h>
#include <asql_explain.h>
#include <asql_key.h>


//=========================================================
// Forward Declarations.
//

int key_init(as_error* err, as_key* key, char* ns, char* set, asql_value* in_key);

static int key_select(asql_config* c, pk_config* p);
static int key_execute(asql_config* c, pk_config* p);
static int key_read(asql_config* c, pk_config* p);
static int key_delete(asql_config* c, pk_config* p);
static int key_write(asql_config* c, pk_config* p);

static void record_set_string(as_record* rec, as_error* err, as_hashmap *m, char* name, asql_value* val);

//=========================================================
// Public API.
//

int
asql_key(asql_config* c, aconfig* ac)
{
	pk_config* p = (pk_config*)ac;

	switch (p->op) {
		case WRITE_OP:
			return key_write(c, p);
		case DELETE_OP:
			return key_delete(c, p);
		case READ_OP:
			return key_read(c, p);
		default:
			return 0;
	}
}

void
asql_record_set_renderer(as_record* rec, as_hashmap* m, char* bin_name,
                         as_val* val)
{
	if (bin_name && (strlen(bin_name) > AS_BIN_NAME_MAX_LEN)) {
		as_stringmap_set((as_map*)m, bin_name, val);
		as_record_set_map(rec, "udf-fn:result", (as_map*)m);
	}
	else {
		as_record_set(rec, bin_name, (as_bin_value*)val);
	}
}

int
key_init(as_error* err, as_key* key, char* ns, char* set, asql_value* in_key)
{
	if (strlen(ns) >= AS_NAMESPACE_MAX_SIZE) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"Namespace is too long: '%s'", ns);
		return 1;
	}

	if (set && (strlen(set) >= AS_SET_MAX_SIZE)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT, "Set name is too long: '%s'",
				set);
		return 1;
	}

	if (in_key->type == AS_STRING) {
		as_key* k = NULL;
		if (in_key->vt == ASQL_VALUE_TYPE_DIGEST) {
			as_digest_value dig;
			int j = 0;
			for (int i = 0; i < 40; i += 2) {
				char d[5];
				sprintf(d, "0x%c%c", in_key->u.str[i], in_key->u.str[i + 1]);
				dig[j++] = strtol(d, NULL, 16);
			}
			k = as_key_init_digest(key, ns, set, dig);
		}
		else if (in_key->vt == ASQL_VALUE_TYPE_EDIGEST) {
			as_digest_value dig;
			uint32_t dig_size;
			cf_b64_decode(in_key->u.str, (uint32_t)strlen(in_key->u.str), dig,
					&dig_size);
			k = as_key_init_digest(key, ns, set, dig);
		}
		else {
			k = as_key_init_strp(key, ns, set, in_key->u.str, false);
		}

		if (k == NULL) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
					"Key is invalid: ('%s','%s','%s')", ns, set,
					in_key->u.str);
			return 1;
		}
	}
	else if (in_key->type == AS_INTEGER) {
		as_key* k = as_key_init_int64(key, ns, set, in_key->u.i64);
		if (k == NULL) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
					"Key is invalid: ('%s','%s',%ld)", ns, set,
					in_key->u.i64);
			return 1;
		}
	}

	return 0;
}


//==========================================================
// Local Helpers.
//

static int
key_select(asql_config* c, pk_config* p)
{
	as_error err;
	as_error_init(&err);

	as_policy_read read_policy;
	as_policy_read_init(&read_policy);
	read_policy.base.total_timeout = c->base.timeout_ms;
	if (c->base.socket_timeout_ms > -1) {
		// set if non-default value
		read_policy.base.socket_timeout = c->base.socket_timeout_ms;
	}

	if (p->key.vt == ASQL_VALUE_TYPE_EDIGEST
		   || p->key.vt == ASQL_VALUE_TYPE_DIGEST) {
		read_policy.key = AS_POLICY_KEY_DIGEST;
	}
	else if (c->key_send) {
		read_policy.key = AS_POLICY_KEY_SEND;
	}

	as_key key;

	if (key_init(&err, &key, p->ns, p->set, &p->key) != 0) {
		g_renderer->render_error(err.code, err.message, NULL);
		return 1;
	}

	as_record* rec = NULL;

	if (!p->s.bnames) {
		// select all bins
		aerospike_key_get(g_aerospike, &err, &read_policy, &key, &rec);
	}
	else {
		// select specific bins

		const char** bins = (const char**)alloca(
		        sizeof(char*) * (p->s.bnames->size + 1));

		for (int i = 0; i < p->s.bnames->size; i++) {
			char* bname = as_vector_get_ptr(p->s.bnames, i);
			if (strlen(bname) > AS_BIN_NAME_MAX_LEN) {
				as_error_update(&err, AEROSPIKE_ERR_CLIENT,
				                "Bin name is too long: '%s'", bname);
				break;
			}
			bins[i] = bname;
		}
		bins[p->s.bnames->size] = NULL;

		aerospike_key_select(g_aerospike, &err, &read_policy, &key, bins, &rec);
	}

	// Special case for when the key is already known.
	if (g_config->key_send && rec) {
		rec->key.valuep = key.valuep;
	}

	if (p->explain) {
		asql_key_select_explain(c, p, &key, &err);
	}
	else if (err.code == AEROSPIKE_OK) {
		print_rec(rec, p->s.bnames);
	}
	else {
		g_renderer->render_error(err.code, err.message, NULL);
	}

	as_record_destroy(rec);

	return 0;
}

static int
key_execute(asql_config* c, pk_config* p)
{
	as_error err;
	as_error_init(&err);

	as_policy_apply apply_policy;
	as_policy_apply_init(&apply_policy);
	apply_policy.base.total_timeout = c->base.timeout_ms;
	if (c->base.socket_timeout_ms > -1) {
		// set if non-default value
		apply_policy.base.socket_timeout = c->base.socket_timeout_ms;
	}
	apply_policy.durable_delete = c->durable_delete;

	if (p->key.vt == ASQL_VALUE_TYPE_EDIGEST
		   || p->key.vt == ASQL_VALUE_TYPE_DIGEST) {
		apply_policy.key = AS_POLICY_KEY_DIGEST;
	}

	as_key key;

	if (key_init(&err, &key, p->ns, p->set, &p->key) != 0) {
		g_renderer->render_error(err.code, err.message, NULL);
		return 1;
	}

	as_arraylist arglist;

	if (p->u.params) {
		as_arraylist_inita(&arglist, p->u.params->size);

		asql_set_args(&err, p->u.params, &arglist);

		if (err.code != AEROSPIKE_OK) {
			g_renderer->render_error(err.code, err.message, NULL);
			return 1;
		}
	}
	else {
		as_arraylist_inita(&arglist, 0);
	}

	as_val* val = NULL;

	aerospike_key_apply(g_aerospike, &err, &apply_policy, &key, p->u.udfpkg,
	                    p->u.udfname, (as_list*)&arglist, &val);

	int ret = 0;
	if (p->explain) {
		ret = asql_key_select_explain(c, p, &key, &err);
	} else if (err.code == AEROSPIKE_OK) {
		as_record rec;
		as_record_inita(&rec, 2);
		as_hashmap m;
		as_hashmap_init(&m, 2);
		// Consumes val
		asql_record_set_renderer(&rec, &m, p->u.udfname, val);
		print_rec(&rec, NULL);
		as_record_destroy(&rec);
		as_hashmap_destroy(&m);
	} else {
		g_renderer->render_error(err.code, err.message, NULL);
	}

	as_arraylist_destroy(&arglist);

	return ret;
}

static int
key_read(asql_config* c, pk_config* p)
{
	if (p->u.udfpkg) {
		return key_execute(c, p);
	}
	else {
		return key_select(c, p);
	}
}

static int
key_delete(asql_config* c, pk_config* p)
{
	as_error err;
	as_error_init(&err);

	as_policy_remove remove_policy;
	as_policy_remove_init(&remove_policy);
	remove_policy.base.total_timeout = c->base.timeout_ms;
	if (c->base.socket_timeout_ms > -1) {
		// set if non-default value
		remove_policy.base.socket_timeout = c->base.socket_timeout_ms;
	}
	remove_policy.durable_delete = c->durable_delete;

	if (p->key.vt == ASQL_VALUE_TYPE_EDIGEST
		   || p->key.vt == ASQL_VALUE_TYPE_DIGEST) {
		remove_policy.key = AS_POLICY_KEY_DIGEST;
	}
	else if (c->key_send) {
		remove_policy.key = AS_POLICY_KEY_SEND;
	}

	as_key key;

	if (key_init(&err, &key, p->ns, p->set, &p->key) != 0) {
		g_renderer->render_error(err.code, err.message, NULL);
		return 1;
	}

	aerospike_key_remove(g_aerospike, &err, &remove_policy, &key);

	if (p->explain) {
		asql_key_select_explain(c, p, &key, &err);
	}
	else if (err.code == AEROSPIKE_OK) {
		g_renderer->render_ok("1 record affected.", NULL);
	}
	else {
		g_renderer->render_error(err.code, err.message, NULL);
	}

	return 0;

}

static int
key_write(asql_config* c, pk_config* p)
{
	as_error err;
	as_error_init(&err);

	as_policy_write write_policy;
	as_policy_write_init(&write_policy);
	write_policy.base.total_timeout = c->base.timeout_ms;
	if (c->base.socket_timeout_ms > -1) {
		// set if non-default value
		write_policy.base.socket_timeout = c->base.socket_timeout_ms;
	}
	write_policy.durable_delete = c->durable_delete;

	if (p->key.vt == ASQL_VALUE_TYPE_EDIGEST
		   || p->key.vt == ASQL_VALUE_TYPE_DIGEST) {
		write_policy.key = AS_POLICY_KEY_DIGEST;
	}
	else if (c->key_send) {
		write_policy.key = AS_POLICY_KEY_SEND;
	}

	as_key key;

	if (key_init(&err, &key, p->ns, p->set, &p->key) != 0) {
		g_renderer->render_error(err.code, err.message, NULL);
		return 1;
	}

	as_hashmap m;
	as_hashmap_init(&m, 2);
	as_record rec;
	as_record_inita(&rec, p->i.bnames->size);
	rec.ttl = c->record_ttl_sec;

	for (int i = 0; i < p->i.bnames->size; i++) {
		char* name = as_vector_get_ptr(p->i.bnames, i);
		asql_value* value = as_vector_get(p->i.values, i);

		if (strlen(name) > AS_BIN_NAME_MAX_LEN) {
			as_error_update(&err, AEROSPIKE_ERR_CLIENT,
			                "Bin name is too long: '%s'", name);
			break;
		}

		switch (value->type) {
			case AS_INTEGER: {
				as_record_set_int64(&rec, name, value->u.i64);
				break;
			}
			case AS_DOUBLE: {
				as_record_set_double(&rec, name, value->u.dbl);
				break;
			}
			case AS_STRING: {
				record_set_string(&rec, &err, &m, name, value);
				break;
			}
			case AS_GEOJSON: {
				char* str = value->u.str;
				as_record_set_geojson_strp(&rec, name, str, false);
				break;
			}
			case AS_BOOLEAN:{
				bool bol = value->u.bol;
				as_record_set_bool(&rec, name, bol);
				break;
			}
			default: {
				as_error_update(&err, AEROSPIKE_ERR_CLIENT,
				                "Unknown value type: %s %d", name, value->type);
				break;
			}
		}
	}

	if (err.code == AEROSPIKE_OK) {
		aerospike_key_put(g_aerospike, &err, &write_policy, &key, &rec);
	}

	if (p->explain) {
		asql_key_select_explain(c, p, &key, &err);
	}
	else if (err.code == AEROSPIKE_OK) {
		g_renderer->render_ok("1 record affected.", NULL);
	}
	else {
		g_renderer->render_error(err.code, err.message, NULL);
	}

	as_record_destroy(&rec);
	as_hashmap_destroy(&m);


	return 0;
}

static void
record_set_string(as_record* rec, as_error* err, as_hashmap *m, char* name,
		asql_value* value)
{
	char* str = value->u.str;
	if (!str) {
		as_record_set_nil(rec, name);
	}
	// XXX -- In-band type to be deprecated in favor of ASQL internal value type.
	else if (!strncmp(str, "JSON", 4)
			|| (ASQL_VALUE_TYPE_JSON == value->vt)
			|| (ASQL_VALUE_TYPE_LIST == value->vt)
			|| (ASQL_VALUE_TYPE_MAP == value->vt)) {
		if (ASQL_VALUE_TYPE_NONE == value->vt) {
			str += 4;
		}
		json_t* json = json_string(str);
		as_val* val = NULL;
		if (json) {
			val = as_json_arg((char*)json_string_value(json),
					value->vt);
			json_decref(json);
		}

		if (val) {
			// Consumes val
			asql_record_set_renderer(rec, m, name, val);
		}
		else {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
					"Invalid %s value: %s %s", 
					(ASQL_VALUE_TYPE_LIST == value->vt ?  "LIST":
					 (ASQL_VALUE_TYPE_MAP == value->vt ?
					  "MAP": "JSON")),
					name, str);
		}
	}
	else {
		as_record_set_strp(rec, name, str, false);
	}
}
