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

struct info_obj_s;

typedef bool (*res_parser)(struct info_obj_s* iobj, const as_error* err, const as_node* node, const char* req, const char* res);

typedef struct {
	uint32_t code;
	char message[1024];
} parser_error;

typedef struct info_obj_s {
	res_parser parse_fn;
	parser_error error;
	void* udata;
	void* rview;
} info_obj;


//==========================================================
// Forward Declarations.
//


static int udfput(asql_config* c, info_config* ic);
static int udfremove(asql_config* c, info_config* ic);
static int info_generic(asql_config* c, info_config* ic, const char* success, res_parser parse_fn, void* udata);

static info_obj* new_obj(res_parser parse_fn, void* udata);
static void free_obj(info_obj* iobj);
static bool value_parser(as_hashmap* map, const char* req, const as_val* name, const char* value);
static bool pair_parser(as_hashmap* map, const char* req, const char* pair);
static bool generic_cb(const as_error* err, const as_node* node, const char* req, char* res, void* udata);
static bool parse_response(info_obj* iobj, const as_error* err, const as_node* node, const char* req, const char* res);
static bool bins_res_parser(info_obj* parser, const as_error* err, const as_node* node, const char* req, const char* res);
static bool kv_res_parser(info_obj* parser, const as_error* err, const as_node* node, const char* req, const char* res);
static bool udf_get_res_parser(info_obj* parser, const as_error* err, const as_node* node, const char* req, const char* res);
static bool list_res_parser(info_obj* parser, const as_error* err, const as_node* node, const char* req, const char* res);
static bool list_udf_parser(info_obj* parser, const as_error* err, const as_node* node, const char* req, const char* res);
static bool dict_res_parser(info_obj* parser, const as_error* err, const as_node* node, const char* req, const char* res);


//==========================================================
// Public API.
//

info_config*
asql_info_config_create(char* cmd, char* backout_cmd, bool is_ddl)
{
	info_config* i = malloc(sizeof(info_config));
	i->optype = ASQL_OP_ASINFO;
	i->type = INFO_OP;
	i->is_ddl = is_ddl;
	i->cmd = cmd;
	i->backout_cmd = backout_cmd;
	return i;
}

int
asql_info(asql_config* c, aconfig* ac)
{
	info_config* ic = (info_config*)ac;
	int result = -1;

	if ((!ic) || (!ic->cmd)) {
		return result;
	}

	if (strstr(ic->cmd, "namespaces") == ic->cmd) {
		result = info_generic(c, ic, NULL, list_res_parser, NULL);
	}
	else if (strstr(ic->cmd, "sets") == ic->cmd) {
		result = info_generic(c, ic, NULL, list_res_parser, NULL);
	}
	else if (strstr(ic->cmd, "bins") == ic->cmd) {
		result = info_generic(c, ic, NULL, bins_res_parser, NULL);
	}
	else if (strstr(ic->cmd, "udf-list") == ic->cmd) {
		result = info_generic(c, ic, NULL, list_udf_parser, NULL);
	}
	else if (strstr(ic->cmd, "udf-put") == ic->cmd) {
		result = udfput(c, ic);
	}
	else if (strstr(ic->cmd, "udf-remove") == ic->cmd) {
		result = udfremove(c, ic);
	}
	else if (strstr(ic->cmd, "udf-get") == ic->cmd) {
		result = info_generic(c, ic, NULL, udf_get_res_parser, NULL);
	}
	else if (strstr(ic->cmd, "sindex-list") == ic->cmd) {
		result = info_generic(c, ic, NULL, list_res_parser, NULL);
	}
	else {
		// An error that should only appear during development.
		char err_msg[1024];
		snprintf(err_msg, 1023, "Unrecognized info command %s", ic->cmd);
		g_renderer->render_error(errno, err_msg, NULL);
	}

	return result;
}

//==========================================================
// Local Helpers.
//

static int
udfput(asql_config* c, info_config* ic)
{
	as_error err;

	as_policy_info info_policy;
	as_policy_info_init(&info_policy);
	info_policy.timeout = c->base.timeout_ms;

	char* udf_file_path = ic->backout_cmd;

	FILE* file = fopen(udf_file_path, "r");

	if (!file) {
		// If we get here it's likely that we're not running the example from
		// the right directory - the specific example directory.
		char err_msg[1024];
		snprintf(err_msg, 1023, "Cannot open file %s", udf_file_path);
		g_renderer->render_error(errno, err_msg, NULL);
		return false;
	}

	// determine the file-size
	int rv = fseek(file, 0, SEEK_END);
	if (rv != 0) {
		g_renderer->render_error(rv, "file-seek operation failed", NULL);
		return false;
	}

	long file_size = ftell(file);
	if (file_size == -1L) {
		g_renderer->render_error(rv, "ftell operation failed", NULL);
		return false;
	}

	rewind(file);

	// Read the file's content into a local buffer.
	uint8_t* content = (uint8_t*)malloc(sizeof(uint8_t)* file_size);

	if (!content) {
		g_renderer->render_error(-126, "Script content allocation failed",
		                         NULL);
		return false;
	}

	uint8_t* p_write = content;
	int read = (int)fread(p_write, 1, 512, file);
	int size = 0;

	while (read) {
		size += read;
		p_write += read;
		read = (int)fread(p_write, 1, 512, file);
	}

	fclose(file);

	// Wrap the local buffer as an as_bytes object.
	as_bytes udf_content;
	as_bytes_init_wrap(&udf_content, content, size, true);

	as_string string_filename;
	const char* base = as_basename(&string_filename, udf_file_path);

	// Register the UDF file in the database cluster.
	aerospike_udf_put(g_aerospike, &err, &info_policy, base, AS_UDF_TYPE_LUA,
	                  &udf_content);

	if (err.code == AEROSPIKE_OK) {
		g_renderer->render_ok("1 module added.", NULL);
	}
	else {
		g_renderer->render_error(err.code, err.message, NULL);
	}

	as_string_destroy(&string_filename);

	// This frees the local buffer.
	as_bytes_destroy(&udf_content);

	return 0;
}

static int
udfremove(asql_config* c, info_config* ic)
{
	as_error err;

	as_policy_info info_policy;
	as_policy_info_init(&info_policy);
	info_policy.timeout = c->base.timeout_ms;

	char* udf_file_name = ic->backout_cmd;

	aerospike_udf_remove(g_aerospike, &err, &info_policy, udf_file_name);

	if (err.code == AEROSPIKE_OK) {
		g_renderer->render_ok("1 module removed.", NULL);
	}
	else {
		g_renderer->render_error(err.code, err.message, NULL);
	}

	return 0;
}

static int
info_generic(asql_config* c, info_config* ic, const char* success,
                  res_parser parse_fn, void* udata)
{
	as_error err;
	int rv = 0;

	as_policy_info info_policy;
	as_policy_info_init(&info_policy);
	info_policy.timeout = c->base.timeout_ms;

	info_obj* iobj = new_obj(parse_fn, udata);

	if (ic->is_ddl) {
		char* res = NULL;
		as_status status = aerospike_info_any(g_aerospike, &err, &info_policy,
		                                       ic->cmd, &res);
		if (status == AEROSPIKE_OK) {
			generic_cb(&err, NULL, ic->cmd, res, iobj);
			free(res);
		}
	}
	else {
		aerospike_info_foreach(g_aerospike, &err, &info_policy, ic->cmd,
		                       generic_cb, iobj);
	}

	if (iobj->error.code != 0) {
		g_renderer->render_error(iobj->error.code, iobj->error.message,
		                         iobj->rview);
		rv = -1;
	}
	else if (err.code != AEROSPIKE_OK) {
		g_renderer->render_error(err.code, err.message, iobj->rview);
		rv = -1;
	}
	else if (success) {
		g_renderer->render_ok(success, iobj->rview);
	}
	else {
		g_renderer->render_ok("", iobj->rview);
	}

	free_obj(iobj);

	return rv;
}

static info_obj*
new_obj(res_parser parse_fn, void* udata)
{
	info_obj* iobj = (info_obj*)malloc(sizeof(info_obj));
	iobj->parse_fn= parse_fn;
	iobj->error.code = 0;
	memset(iobj->error.message, 0, 1024);
	iobj->udata = udata;
	iobj->rview = g_renderer->view_new(NULL);
	return iobj;
}

static void
free_obj(info_obj* iobj)
{
	g_renderer->view_destroy(iobj->rview);
	free(iobj);
}

static bool
bins_res_parser(info_obj* parser, const as_error* err,
            const as_node* node, const char* req, const char* res)
{
	void* rview = parser->rview;
	g_renderer->view_set_node(node, rview);

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
			as_hashmap map;
			as_hashmap_init(&map, 64);
			as_string key_namespace;
			as_string_init(&key_namespace, "namespace", false);

			as_string key_count;
			as_string_init(&key_count, "count", false);

			as_string key_quota;
			as_string_init(&key_quota, "quota", false);

			as_string key_bin;
			as_string_init(&key_bin, "bin", false);

			char* ns_save = NULL;
			namespace = strtok_r((char*)L, ":", &ns_save);
			as_string val_namespace, val_bin;
			as_integer val_count;
			as_hashmap_set(&map, (as_val*)as_val_reserve(&key_namespace),
			        (as_val*)as_string_init(&val_namespace, namespace, false));

			as_hashmap_set(&map, (as_val*)as_val_reserve(&key_bin),
			               (as_val*)as_string_init(&val_bin, R, false));

			as_hashmap_set(&map, (as_val*)as_val_reserve(&key_count),
			               (as_val*)as_integer_init(&val_count, 1));

			g_renderer->render((as_val*)&map, rview);
			as_hashmap_destroy(&map);
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

		as_hashmap map;
		as_hashmap_init(&map, 64);

		as_string key_namespace;
		as_string_init(&key_namespace, "namespace", false);

		as_string key_count;
		as_string_init(&key_count, "count", false);

		as_string key_quota;
		as_string_init(&key_quota, "quota", false);

		as_string key_bin;
		as_string_init(&key_bin, "bin", false);

		char* bin_save = NULL;
		char* bin = strtok_r((char*)L, ",", &bin_save);
		while (bin) {

			as_string val_namespace, val_bin;
			as_integer val_count, val_quota;

			as_hashmap_set(&map, (as_val*)as_val_reserve(&key_namespace),
					(as_val*)as_string_init(&val_namespace, namespace,
											 false));

			as_hashmap_set(&map, (as_val*)as_val_reserve(&key_bin),
						   (as_val*)as_string_init(&val_bin, bin, false));

			as_hashmap_set(&map, (as_val*)as_val_reserve(&key_count),
					(as_val*)as_integer_init(&val_count,
											  atoi(num_bin_names)));

			as_hashmap_set(&map, (as_val*)as_val_reserve(&key_quota),
					(as_val*)as_integer_init(&val_quota,
											  atoi(bin_names_quota)));

			g_renderer->render((as_val*)&map, rview);

			as_hashmap_clear(&map);
			bin = strtok_r(NULL, ",", &bin_save);
		}

		as_hashmap_destroy(&map);

next_tok_parse:

		entry = strtok_r(NULL, ";\n", &entry_save);
	}

	g_renderer->render((as_val*) NULL, rview);

	return true;
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
		char *str = (char *) malloc(sizeof(char) * 32);
		memset(str, 0, 32);
		sprintf(str, "%" PRIu64, (uint64_t)val);
		as_hashmap_set(map, (as_val*)name,
				(as_val*)as_string_new(str, true));
	}
	else {
		as_hashmap_set(map, (as_val*)name,
					   (as_val*)as_string_new((char*)value, false));
	}
	return true;
}

static char*
pair_decode_parser(as_hashmap* map, const char* req, const char* pair)
{
	char* name = strdup(pair);
	char* delim = strstr(name, "=");
	char* value = NULL;

	if (delim) {
		delim[0] = '\0';
		value = delim + 1;
	}

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
	char* name = strdup(pair);
	char* delim = strstr(name, "=");
	char* value = NULL;

	if (delim) {
		delim[0] = '\0';
		value = delim + 1;
	}

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

// NB: Only for udf-get
static bool
udf_get_res_parser(info_obj* parser, const as_error* err, const as_node* node,
          const char* req, const char* res)
{
	as_hashmap map;
	as_hashmap_init(&map, 128);

	char* pair_save = NULL;
	char* pair = strtok_r((char*)res, ";\n\t", &pair_save);
	char* dbuf = NULL;

	while (pair) {
		if (!strncasecmp(pair, "content", 7)) {
			dbuf = pair_decode_parser(&map, req, pair);
		}
		else {
			pair_parser(&map, req, pair);
		}
		pair = strtok_r(NULL, ";\n\t", &pair_save);
	}

	void* rview = parser->rview;
	g_renderer->view_set_node(node, rview);
	g_renderer->render((as_val*)&map, rview);
	g_renderer->render((as_val*) NULL, rview);

	as_hashmap_destroy(&map);

	if (dbuf) {
		free(dbuf);
	}

	return true;
}

static bool
kv_res_parser(info_obj* parser, const as_error* err, const as_node* node,
          const char* req, const char* res)
{
	as_hashmap map;
	as_hashmap_init(&map, 128);

	char* pair_save = NULL;
	char* pair = strtok_r((char*)res, ";\n\t", &pair_save);

	while (pair) {
		pair_parser(&map, req, pair);
		pair = strtok_r(NULL, ";\n\t", &pair_save);
	}

	void* rview = parser->rview;
	g_renderer->view_set_node(node, rview);
	g_renderer->render((as_val*)&map, rview);
	g_renderer->render((as_val*) NULL, rview);

	as_hashmap_destroy(&map);

	return true;
}

static bool
list_udf_parser(info_obj* parser, const as_error* err,
            const as_node* node, const char* req, const char* res)
{
	void* rview = parser->rview;
	g_renderer->view_set_node(node, rview);

	as_hashmap map;
	as_hashmap_init(&map, 128);

	char* save = NULL;
	char* entry = strtok_r((char*)res, ";\n", &save);

	while (entry) {

		if (strchr(entry, ',') != NULL) {
			char* pair_save = NULL;
			char* pair = strtok_r(entry, ",", &pair_save);
			while (pair) {
				pair_parser(&map, req, pair);
				pair = strtok_r(NULL, ",", &pair_save);
			}

			g_renderer->render((as_val*)&map, rview);
		}
		else {
			as_string str;
			as_string_init(&str, (char*)req, false);
			value_parser(&map, req, (as_val*)&str, entry);
			g_renderer->render((as_val*)&map, rview);
		}

		as_hashmap_clear(&map);

		entry = strtok_r(NULL, ";\n", &save);
	}

	as_hashmap_destroy(&map);

	g_renderer->render((as_val*) NULL, rview);

	return true;
}

static bool
list_res_parser(info_obj* parser, const as_error* err,
            const as_node* node, const char* req, const char* res)
{
	void* rview = parser->rview;
	g_renderer->view_set_node(node, rview);

	as_hashmap map;
	as_hashmap_init(&map, 128);

	char* save = NULL;
	char* entry = strtok_r((char*)res, ";\n", &save);

	while (entry) {

		if (strchr(entry, ':') != NULL) {
			char* pair_save = NULL;
			char* pair = strtok_r(entry, ":", &pair_save);
			while (pair) {
				pair_parser(&map, req, pair);
				pair = strtok_r(NULL, ":", &pair_save);
			}

			g_renderer->render((as_val*)&map, rview);
		}
		else {
			as_string str;
			as_string_init(&str, (char*)req, false);
			value_parser(&map, req, (as_val*)&str, entry);
			g_renderer->render((as_val*)&map, rview);
		}

		as_hashmap_clear(&map);

		entry = strtok_r(NULL, ";\n", &save);
	}

	as_hashmap_destroy(&map);

	g_renderer->render((as_val*) NULL, rview);

	return true;
}

static bool
dict_res_parser(info_obj* parser, const as_error* err,
            const as_node* node, const char* req, const char* res)
{
	void* rview = parser->rview;
	g_renderer->view_set_node(node, rview);

	// as_string* key_name = as_string_new("name", false);
	as_hashmap m,* map;
	map = as_hashmap_init(&m, 32);

	as_string col_name;
	as_string_init(&col_name, "name", false);

	as_string col_value;
	as_string_init(&col_value, "value", false);

	char* pair_save = NULL;
	char* pair = strtok_r((char*)res, ";\n", &pair_save);
	while (pair) {

		char* name = strdup(pair);
		char* delim = strstr(name, "=");
		char* value = NULL;

		if (delim) {
			delim[0] = '\0';
			value = delim + 1;
		}

		as_hashmap_set(map, (as_val*)as_val_reserve(&col_name),
		               (as_val*)as_string_new(name, true));

		value_parser(map, req, (as_val*)as_val_reserve(&col_value), value);

		g_renderer->render((as_val*)map, rview);

		as_hashmap_clear(map);
		pair = strtok_r(NULL, ";\n", &pair_save);
	}

	as_hashmap_destroy(map);

	g_renderer->render((as_val*) NULL, rview);

	return true;
}

static bool
parse_response(info_obj* iobj, const as_error* err, const as_node* node,
		const char* req, const char* res)
{
	if (!iobj
   		|| !iobj->parse_fn)	{
		return true;
	}
	iobj->parse_fn(iobj, err, node, req, res);
	return iobj->error.code ? false : true;
}

static bool
generic_cb(const as_error* err, const as_node* node, const char* req,
		char* res, void* udata)
{
	if (err->code != AEROSPIKE_OK) {
		g_renderer->render_error(err->code, err->message, NULL);
		return true;
	}

	if (res == NULL || strlen(res) == 0) {
		return true;
	}

	char* resp = strchr(res, '\t');

	if (resp == NULL || strlen(resp) == 0) {
		return true;
	}

	resp++;

	if (resp == NULL || strlen(resp) == 0) {
		return true;
	}

	if (udata && parse_response((info_obj*)udata, err,
										node, req, resp) == false) {
		return false;
	}

	return true;
}
