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
#include <asql_info_parser.h>

#include "renderer/table.h"


//==========================================================
// Typedefs & constants.
//

struct info_obj_s;

typedef void (*parser_callback)(void *udata, const as_node *node, const char *req, char *res);


typedef struct info_obj_s {
	parser_callback callback;
	as_error error;
	void* rview;
	void* udata;
} info_obj;


//==========================================================
// Forward Declarations.
//


static int udfput(asql_config* c, info_config* ic);
static int udfremove(asql_config* c, info_config* ic);
static int info_generic(asql_config *c, info_config *ic, info_obj *iobj);

static info_obj *new_obj(parser_callback callback, void *udata);
static void free_obj(info_obj* iobj);
static int display_obj(info_obj *iobj, const char *success);
static bool generic_cb(const as_error* err, const as_node* node, const char* req, char* res, void* udata);
static bool render_response(info_obj* iobj, const as_error* err, const as_node* node, const char* req, char* res);
static void generic_list_res_render(void *udata, const as_node *node, const char *req, char *res);
static void bins_res_render(void *udata, const as_node *node, const char *req, char *res);
static void udf_get_res_render(void *udata, const as_node *node, const char *req, char *res);
static void list_udf_res_render(void *udata, const as_node *node, const char *req, char *res);
static void list_render(info_obj *iobj, const as_node *node, const char *req, char *res);

//==========================================================
// Public API.
//

info_config*
asql_info_config_create(int optype, char* cmd, char *backout_cmd, bool is_ddl)
{
	info_config* i = malloc(sizeof(info_config));
	i->optype = optype; // must be set by caller
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
	int rv = -1;

	if ((!ic) || (!ic->cmd)) {
		return rv;
	}

	as_vector *parsed_resp = NULL;
	info_obj *iobj = NULL;

	if (strstr(ic->cmd, "namespaces") == ic->cmd) {
		parsed_resp = as_vector_create(sizeof(as_hashmap*), 128);
		iobj = new_obj(generic_list_res_render, (void*)parsed_resp);
		rv = info_generic(c, ic, iobj);
	}
	else if (strstr(ic->cmd, "sets") == ic->cmd) {
		parsed_resp = as_vector_create(sizeof(as_hashmap*), 128);
		iobj = new_obj(generic_list_res_render, (void*)parsed_resp);
		rv = info_generic(c, ic, iobj);
	}
	else if (strstr(ic->cmd, "bins") == ic->cmd) {
		parsed_resp = as_vector_create(sizeof(as_hashmap *), 128);
		iobj = new_obj(bins_res_render, (void *)parsed_resp);
		rv = info_generic(c, ic, iobj);
	}
	else if (strstr(ic->cmd, "udf-list") == ic->cmd) {
		parsed_resp = as_vector_create(sizeof(as_hashmap *), 128);
		iobj = new_obj(list_udf_res_render, (void *)parsed_resp);
		rv = info_generic(c, ic, iobj);
	}
	else if (strstr(ic->cmd, "udf-put") == ic->cmd) {
		rv = udfput(c, ic);
	}
	else if (strstr(ic->cmd, "udf-remove") == ic->cmd) {
		rv = udfremove(c, ic);
	}
	else if (strstr(ic->cmd, "udf-get") == ic->cmd) {
		parsed_resp = as_vector_create(sizeof(as_hashmap *), 128);
		iobj = new_obj(udf_get_res_render, (void *)parsed_resp);
		rv = info_generic(c, ic, iobj);
	}
	else if (strstr(ic->cmd, "sindex-list") == ic->cmd) {
		parsed_resp = as_vector_create(sizeof(as_hashmap *), 128);
		iobj = new_obj(generic_list_res_render, (void *)parsed_resp);
		rv = info_generic(c, ic, iobj);
	}
	else {
		// An error that should only appear during development.
		char err_msg[1024];
		snprintf(err_msg, 1023, "Unrecognized info command %s", ic->cmd);
		g_renderer->render_error(errno, err_msg, NULL);
	}

	if (iobj) {
		display_obj(iobj, NULL);
		free_obj(iobj);
	}

	if (parsed_resp) {
		as_vector_destroy(parsed_resp);
	}

	return rv;
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
info_generic(asql_config *c, info_config *ic, info_obj *iobj)
{
	int rv = 0;

	as_policy_info info_policy;
	as_policy_info_init(&info_policy);
	info_policy.timeout = c->base.timeout_ms;

	if (ic->is_ddl) {
		char* res = NULL;
		as_status status = aerospike_info_any(g_aerospike, &iobj->error, &info_policy,
											  ic->cmd, &res);
		if (status == AEROSPIKE_OK) {
			generic_cb(&iobj->error, NULL, ic->cmd, res, iobj);
			free(res);
		}
	}
	else {
		aerospike_info_foreach(g_aerospike, &iobj->error, &info_policy, ic->cmd,
		                       generic_cb, iobj);
	}

	return rv;
}

static int
display_obj(info_obj *iobj, const char *success)
{
	int rv = 0;

	if (iobj->error.code != AEROSPIKE_OK) {
		g_renderer->render_error(iobj->error.code, iobj->error.message,
								 iobj->rview);
		rv = -1;
	} else if (success) {
		g_renderer->render_ok(success, iobj->rview);
	} else {
		g_renderer->render_ok("", iobj->rview);
	}

	return rv;
}

static info_obj*
new_obj(parser_callback callback, void *udata)
{
	info_obj* iobj = (info_obj*)malloc(sizeof(info_obj));
	iobj->callback = callback;
	iobj->error.code = 0;
	iobj->rview = g_renderer->view_new(NULL);
	memset(iobj->error.message, 0, 1024);
	iobj->udata = udata;
	return iobj;
}

static void
free_obj(info_obj* iobj)
{
	g_renderer->view_destroy(iobj->rview);
	free(iobj);
}

static void
list_render(info_obj *iobj, const as_node *node, const char *req, char *res)
{
	if (iobj->error.code != AEROSPIKE_OK) {
		g_renderer->render_error(iobj->error.code, iobj->error.message, NULL);
		return;
	}

	g_renderer->view_set_node(node, iobj->rview);
	as_vector *node_result = (as_vector *)iobj->udata;

	for (int idx = 0; idx < node_result->size; idx++) {
		as_hashmap *map = as_vector_get_ptr(node_result, idx);
		g_renderer->render((as_val *)map, iobj->rview);
		as_hashmap_clear(map);
		as_hashmap_destroy(map);
	}

	g_renderer->render((as_val *)NULL, iobj->rview);

	as_vector_clear(node_result);

	return;
}

static void
bins_res_render(void *udata, const as_node *node, const char *req, char *res)
{
	info_obj* iobj = (info_obj*)udata;
	as_vector* parsed_resp = (as_vector*)iobj->udata;
	bins_res_parser(parsed_resp, node, req, res);
	list_render(iobj, node, req, res);
}

static void
udf_get_res_render(void* udata, const as_node *node, const char *req, char *res)
{
	info_obj* iobj = (info_obj*)udata;
	as_vector* parsed_resp = (as_vector*)iobj->udata;
	udf_get_res_parser(parsed_resp, node, req, res);
	list_render(iobj, node, req, res);
}

static void
list_udf_res_render(void* udata, const as_node *node, const char *req, char *res)
{
	info_obj* iobj = (info_obj*)udata;
	as_vector* parsed_resp = (as_vector*)iobj->udata;
	list_udf_parser(parsed_resp, node, req, res);
	list_render(iobj, node, req, res);
}

static void
generic_list_res_render(void *udata, const as_node *node, const char *req, char *res)
{
	info_obj* iobj = (info_obj*)udata;
	as_vector* parsed_resp = (as_vector*)iobj->udata;
	list_res_parser(parsed_resp, node, req, res);
	list_render(iobj, node, req, res);
}

static bool
render_response(info_obj * iobj, const as_error* err, const as_node *node,
				const char *req, char *res)
{
	if (!iobj->callback)
	{
		return true;
	}

	iobj->callback(iobj, node, req, res);

	return iobj->error.code ? false : true;
}

static bool
generic_cb(const as_error *err, const as_node *node, const char *req,
			char *res, void *udata)
{
	if (err->code != AEROSPIKE_OK) {
		g_renderer->render_error(err->code, err->message, NULL);
		return true;
	}

	char* resp = info_res_split(res);

	if (resp == NULL) {
		return true;
	}

	if (udata && render_response((info_obj *)udata, err,
								node, req, resp) == false) {
		return false;
	}

	return true;
}
