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

#include <citrusleaf/cf_b64.h>
#include <citrusleaf/cf_digest.h>

#include <aerospike/as_msgpack.h>

#include <jansson.h>
#include <json.h>

#include "renderer.h"

//==========================================================
// Typedefs & constants.
//

#define JSON_INDENT_SPACE "  "

typedef struct json_print_data_s {
	int indent;
	bool indented; /// This line was already indented
} json_print_data;

typedef struct each_bin_data_s {
	json_t* obj;
} each_bin_data;


//==========================================================
// Forward Declarations.
//
extern bool rec_has_digest(as_record *p_rec);
extern char* removespaces(char* input);

static as_list* json_array_to_list(json_t* a);
static as_map* json_object_to_map(json_t* o);
static as_string* json_string_to_string(json_t* s);
static as_integer* json_number_to_integer(json_t* n);
static as_val* json_to_val(json_t* j);

static json_t* val_to_json_t(const as_val* val);

static bool each_bin(const char* name, const as_val* val, void* udata);
static bool each_map_entry(const as_val* key, const as_val* val, void* udata);
static bool each_list_entry_as_val(as_val* val, void* udata);
static bool each_list_entry_as_pair(as_val* val, void* udata);

static int json_print_callback(const char* buffer, size_t size, void* udata);
static void json_print_obj(json_t* obj, int indent);
static void json_print_as_record(as_record* rec, int indent, bool metadata, bool no_bins);
static void json_print_as_map(as_map* map, int indent);
static void json_print_as_list(as_list* list, int indent);


//==========================================================
// Inline and macros.
//

static inline void
print_indent(int indent)
{
	for (int i = 0; i < indent; i++) {
		fprintf(stdout, JSON_INDENT_SPACE);
	}
}

static inline void
json_obj_add_as_val(json_t* obj, const char* key, const as_val* val)
{
	json_t* obj_value = val_to_json_t(val);
	json_object_set_new(obj, key, obj_value);
}

static inline void
json_obj_add_as_record(json_t* obj, as_record* rec, bool metadata, bool no_bins)
{
	each_bin_data udata = { .obj = obj };

	// If we find a key-string coming back for the record, then we will
	// add key as a new column
	if (rec->key.valuep) {
		json_obj_add_as_val(obj, "PK", &rec->key.valuep->string._);
	}

	if (metadata) {
		// need to notice single-record calls doesn't have digest, and print N/A
		if (rec_has_digest(rec)) {
			uint32_t digest_len = cf_b64_encoded_len(sizeof(as_digest_value));
			char* digest64 = alloca(digest_len + 1);
			cf_b64_encode((uint8_t*)rec->key.digest.value,
			              sizeof(as_digest_value), digest64);
			digest64[digest_len] = 0;
			json_object_set_new(obj, "edigest", json_string(digest64));
		}
		else {
			json_object_set_new(obj, "edigest", json_string("N/A"));
		}

		// print setname if it exists
		if (rec->key.set[0]) {
			json_object_set_new(obj, "set", json_string(rec->key.set));
		}

		if ((int32_t)rec->ttl == -1) {
			json_object_set_new(obj, "ttl", json_integer(-1));
		}
		else {
			json_object_set_new(obj, "ttl", json_integer(rec->ttl));
		}
		json_object_set_new(obj, "gen", json_integer(rec->gen));

		udata.obj = json_object();
	} else {
		// In case of no_bin print digest
		// need to notice single-record calls doesn't have digest, and print N/A
		if (no_bins && rec_has_digest(rec)) {
			as_bytes b;
			as_bytes_init_wrap(&b, rec->key.digest.value,
					CF_DIGEST_KEY_SZ, false);
			json_object_set_new(obj, "digest",
					json_string(removespaces(as_val_tostring(&b))));
		}
	}


	uint16_t size = as_rec_numbins(&rec->_);
	if (size > 0) {
		as_rec_foreach(&rec->_, each_bin, &udata);
		if (obj != udata.obj) {
			json_object_set_new(obj, "bins", udata.obj);
		}
	}
}



//==========================================================
// Public API.
//

int
as_json_print(const as_val* val)
{
	if (!val) {
		printf("null");
		return 1;
	}
	switch (val->type) {
		case AS_NIL: {
			printf("null");
			break;
		}
		case AS_BOOLEAN: {
			printf("%s",
			       as_boolean_tobool((as_boolean*)val) ? "true": "false");
			break;
		}
		case AS_INTEGER: {
			printf("%" PRId64, as_integer_toint((as_integer*) val));
			break;
		}
		case AS_DOUBLE: {
			printf("%.16g", as_double_get((as_double*)val));
			break;
		}
		case AS_STRING: {
			printf("\"%s\"", as_string_tostring((as_string*)val));
			break;
		}
		case AS_LIST: {
			as_iterator* i = (as_iterator*)as_list_iterator_new(
			        (as_list*)val);
			bool delim = false;
			printf("[");
			while (as_iterator_has_next(i)) {
				if (delim)
					printf(",");
				printf(" ");
				as_json_print(as_iterator_next(i));
				delim = true;
			}
			printf(" ");
			printf("]");
			break;
		}
		case AS_MAP: {
			as_iterator* i = (as_iterator*)as_map_iterator_new((as_map*)val);
			bool delim = false;
			printf("{");
			while (as_iterator_has_next(i)) {
				as_pair* kv = (as_pair*)as_iterator_next(i);
				if (delim)
					printf(",");
				printf(" ");
				as_json_print(as_pair_1(kv));
				printf(": ");
				as_json_print(as_pair_2(kv));
				delim = true;
			}
			printf(" ");
			printf("}");
			break;
		}
		default: {
			printf("~~<%d>", val->type);
		}
	}
	return 0;
}

// Attempt to parse JSON-format string ARG as type VTYPE.
// (Only types LIST and MAP are enforced.)
as_val*
as_json_arg(char* arg, asql_value_type_t vtype)
{
	as_val* val = NULL;
	json_t* root = NULL;
	json_error_t error;

	root = json_loads(arg, 0, &error);

	if (!root) {
		// Let asql deal with base type. JSON only to pass nested types
		return NULL;
	}
	else if ((ASQL_VALUE_TYPE_LIST == vtype) && !json_is_array(root)) {
		return NULL;
	}
	else if ((ASQL_VALUE_TYPE_MAP == vtype) && !json_is_object(root)) {
		return NULL;
	}
	else {
		val = json_to_val(root);
		json_decref(root);
	}

	return val;
}

void
as_json_print_as_val(const as_val* val, int indent, bool metadata, bool no_bins)
{
	switch (as_val_type(val)) {
		case AS_REC: {
			as_record* p_rec = as_record_fromval(val);
			json_print_as_record(p_rec, indent, metadata, no_bins);
		}
			break;

		case AS_MAP: {
			as_map* map = as_map_fromval(val);
			json_print_as_map(map, indent);
		}
			break;

		case AS_LIST: {
			as_list* list = as_list_fromval((as_val*)val);
			json_print_as_list(list, indent);
		}
			break;
		default:
			break;
	}
}


//==========================================================
// Local Helpers.
//

static as_list*
json_array_to_list(json_t* a)
{

	int size = (int)json_array_size(a);
	as_list* l = (as_list*)as_arraylist_new(size, 0);

	for (int i = 0; i < json_array_size(a); i++) {
		as_val* v = json_to_val(json_array_get(a, i));
		as_list_append(l, v);
	}

	return l;
}

static as_map*
json_object_to_map(json_t* o)
{

	int n = (int)json_object_size(o);
	as_map* m = (as_map*)as_hashmap_new(n);
	const char* k = NULL;
	json_t* v = NULL;

	json_object_foreach(o, k, v)
	{
		as_val* key = (as_val*)as_string_new_strdup(k);
		as_val* val = json_to_val(v);
		as_map_set(m, key, val);
	}

	return m;
}

static as_string*
json_string_to_string(json_t* s)
{
	const char* str = json_string_value(s);
	return as_string_new_strdup(str);
}

static as_integer*
json_number_to_integer(json_t* n)
{
	return as_integer_new((int64_t)json_integer_value(n));
}

static as_val*
json_to_val(json_t* j)
{
	if (json_is_array(j))
		return (as_val*)json_array_to_list(j);
	if (json_is_object(j))
		return (as_val*)json_object_to_map(j);
	if (json_is_string(j))
		return (as_val*)json_string_to_string(j);
	if (json_is_integer(j))
		return (as_val*)json_number_to_integer(j);
	if (json_is_real(j))
		return (as_val*)as_double_new((double)json_real_value(j));
	if (json_is_boolean(j))
		return (as_val*)as_boolean_new(json_boolean_value(j));
	return (as_val*) NULL; //&as_nil;
}

static int
json_print_callback(const char* buffer, size_t size, void* udata)
{
	json_print_data* data = (json_print_data*)udata;
	for (size_t i = 0; i < size; i++) {
		if (!data->indented) {
			print_indent(data->indent);
			data->indented = true;
		}
		fputc(buffer[i], stdout);
		if (buffer[i] == '\n') {
			data->indented = false;
		}
	}
	return 0;
}

static void
json_print_obj(json_t* obj, int indent)
{
	json_print_data data = { .indent = indent, .indented = false };
	json_dump_callback(obj, json_print_callback, &data,
	JSON_INDENT(2) | JSON_PRESERVE_ORDER);
}

static bool
each_bin(const char* name, const as_val* val, void* udata)
{
	if (!name || !udata) {
		return false;
	}
	each_bin_data* data = (each_bin_data*)udata;
	json_obj_add_as_val(data->obj, name, val);
	return true;
}

static bool
each_map_entry(const as_val* key, const as_val* val, void* udata)
{
	return each_bin(as_string_get(as_string_fromval(key)), val, udata);
}

static bool
each_list_entry_as_val(as_val* val, void* udata)
{
	if (!val || !udata) {
		return false;
	}
	json_t* obj = (json_t*)udata;
	json_t* obj_value = val_to_json_t(val);
	json_array_append_new(obj, obj_value);
	return true;
}

static json_t*
val_to_json_t(const as_val* val)
{
	json_t* obj_value;

	// In some instances, C-client can return as_val with count=0.
	// In such cases, we should not be calling as_val_tostring or converting.
	if (val->count == 0) {
		obj_value = json_null();
	}
	else {
		switch (as_val_type(val)) {
			case AS_BOOLEAN:
				obj_value = json_boolean(
				        as_boolean_tobool((as_boolean*) val));
				break;
			case AS_INTEGER:
				obj_value = json_integer(as_integer_get((as_integer*)val));
				break;
			case AS_DOUBLE:
				obj_value = json_real(as_double_get((as_double*)val));
				break;
			case AS_STRING:
				obj_value = json_string(as_string_get((as_string*)val));
				break;
			case AS_UNDEF:
			case AS_NIL:
				obj_value = json_null();
				break;
			case AS_LIST: {
				obj_value = json_array();
				as_list_foreach((as_list*)val, each_list_entry_as_val,
				                obj_value);
			}
				break;
			case AS_MAP: {
				obj_value = json_object();
				each_bin_data udata = { .obj = obj_value };
				as_map_foreach((as_map*)val, each_map_entry, &udata);
			}
				break;
			case AS_REC: {
				obj_value = json_object();
				json_obj_add_as_record(obj_value, (as_record*)val, false, false);
			}
				break;
			case AS_PAIR:
				// as pairs get printed as ($1, $2) so they don't map to json
				// we could choose to map them as arrays or objects later
			case AS_BYTES:
			default: {
				char* str = as_val_tostring(val);
				obj_value = json_string(str);
				free(str);
			}
				break;
		}
	}
	return obj_value;
}

static bool
each_list_entry_as_pair(as_val* val, void* udata)
{
	// we assume as_list are lists of as_pairs for for root aql output
	as_pair* pair = as_pair_fromval(val);
	return each_bin(as_string_get(as_string_fromval(pair->_1)), pair->_2, udata);
}

static void
json_print_as_record(as_record* rec, int indent, bool metadata, bool no_bins)
{
	fprintf(stdout, "\n");
	json_t* obj = json_object();
	json_obj_add_as_record(obj, rec, metadata, no_bins);
	json_print_obj(obj, indent);
	json_decref(obj);
}

static void
json_print_as_map(as_map* map, int indent)
{
	uint16_t size = as_map_size(map);
	if (size > 0) {
		json_t* obj = val_to_json_t(&map->_);
		fprintf(stdout, "\n");
		json_print_obj(obj, indent);
		json_decref(obj);
	}
}

static void
json_print_as_list(as_list* list, int indent)
{
	uint16_t size = as_list_size(list);
	if (size > 0) {
		// we assume as_list are lists of as_pairs so this is a json_object rather than json_array
		json_t* obj = json_object();
		each_bin_data udata = { .obj = obj };
		as_list_foreach(list, each_list_entry_as_pair, &udata);
		fprintf(stdout, "\n");
		json_print_obj(obj, indent);
		json_decref(obj);
	}
}
