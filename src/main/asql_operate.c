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

//==========================================================
// Includes.
//

#include <citrusleaf/cf_b64.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_aerospike.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_map_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_vector.h>

#include <asql.h>
#include <asql_log.h>
#include <asql_operate.h>

#include <renderer.h>

//==========================================================
// Typedefs & constants.
//

#define OPERATE_MAX_CMD 47


typedef bool (* binop_fill_fn)(binop* op, as_operations* ops, as_list* params);

typedef struct {
	char* cmd;
	char* help;
	binop_fill_fn fn;
} op_map;


//=========================================================
// Forward Declarations.
//

extern void destroy_vector(as_vector* list, bool is_name);
extern int key_init(as_error* err, as_key* key, char* ns, char* set, asql_value* in_key);

static bool operations_init(operate_config* o, as_operations* ops);
static bool ops_populate(uint16_t cmd_idx, binop* op, as_operations* ops);

static bool list_append(binop* op, as_operations* ops, as_list* params);
static bool list_appenditems(binop* op, as_operations* ops, as_list* params);
static bool list_insert(binop* op, as_operations* ops, as_list* params);
static bool list_insertitems(binop* op, as_operations* ops, as_list* params);
static bool list_pop(binop* op, as_operations* ops, as_list* params);
static bool list_poprange(binop* op, as_operations* ops, as_list* params);
static bool list_remove(binop* op, as_operations* ops, as_list* params);
static bool list_removerange(binop* op, as_operations* ops, as_list* params);
static bool list_set(binop* op, as_operations* ops, as_list* params);
static bool list_trim(binop* op, as_operations* ops, as_list* params);
static bool list_clear(binop* op, as_operations* ops, as_list* params);
static bool list_increment(binop* op, as_operations* ops, as_list* params);
static bool list_size(binop* op, as_operations* ops, as_list* params);
static bool list_get(binop* op, as_operations* ops, as_list* params);
static bool list_getrange(binop* op, as_operations* ops, as_list* params);

static bool map_put(binop* op, as_operations* ops, as_list* params);
static bool map_putitems(binop* op, as_operations* ops, as_list* params);
static bool map_increment(binop* op, as_operations* ops, as_list* params);
static bool map_decrement(binop* op, as_operations* ops, as_list* params);
static bool map_clear(binop* op, as_operations* ops, as_list* params);
static bool map_settype(binop* op, as_operations* ops, as_list* params);
static bool map_removebykey(binop* op, as_operations* ops, as_list* params);
static bool map_removebykeylist(binop* op, as_operations* ops, as_list* params);
static bool map_removebykeyrange(binop* op, as_operations* ops, as_list* params);
static bool map_removebyvalue(binop* op, as_operations* ops, as_list* params);
static bool map_removebyvaluelist(binop* op, as_operations* ops, as_list* params);
static bool map_removebyvaluerange(binop* op, as_operations* ops, as_list* params);
static bool map_removebyindex(binop* op, as_operations* ops, as_list* params);
static bool map_removebyindexrange(binop* op, as_operations* ops, as_list* params);
static bool map_removebyrank(binop* op, as_operations* ops, as_list* params);
static bool map_removebyrankrange(binop* op, as_operations* ops, as_list* params);
static bool map_size(binop* op, as_operations* ops, as_list* params);
static bool map_getbykey(binop* op, as_operations* ops, as_list* params);
static bool map_getbykeyrange(binop* op, as_operations* ops, as_list* params);
static bool map_getbyvalue(binop* op, as_operations* ops, as_list* params);
static bool map_getbyvaluerange(binop* op, as_operations* ops, as_list* params);
static bool map_getbyindex(binop* op, as_operations* ops, as_list* params);
static bool map_getbyindexrange(binop* op, as_operations* ops, as_list* params);
static bool map_getbyrank(binop* op, as_operations* ops, as_list* params);
static bool map_getbyrankrange(binop* op, as_operations* ops, as_list* params);

static bool writeop(binop* op, as_operations* ops, as_list* params);
static bool readop(binop* op, as_operations* ops, as_list* params);
static bool incr(binop* op, as_operations* ops, as_list* params);
static bool prepend(binop* op, as_operations* ops, as_list* params);
static bool append(binop* op, as_operations* ops, as_list* params);
static bool touch(binop* op, as_operations* ops, as_list* params);
static bool deleteop(binop* op, as_operations* ops, as_list* params);

static bool policy_check(binop* op, bool no_policy);
static bool params_check(binop* op, as_list* params, uint16_t min_params, uint16_t max_params);

static bool get_val(binop* op, as_list* params, as_val** val);
static bool get_vallist(binop* op, as_list* params, as_val** val);
static bool get_valmap(binop* op, as_list* params, as_val** val);
static bool get_valstring(binop* op, as_list* params, as_val** val);
static bool get_valnumeric(binop* op, as_list* params, int64_t* icount, double* dcount);
static bool get_twoval(binop* op, as_list* params, as_val** begin_val, as_val** end_val);
static bool get_index(binop* op, as_list* params, int64_t* index);
static bool get_indexrange(binop* op, as_list* params, int64_t* index, int64_t* count);
static bool get_indexval(binop* op, as_list* params, int64_t* index, as_val** val);


//=========================================================
// Globals.
//

const op_map op_table[OPERATE_MAX_CMD] = {
	{ "LIST_APPEND",       "LIST_APPEND (<bin>, <val>)         ", list_append },
	{ "LIST_INSERT",       "LIST_INSERT (<bin>, <index>, <val>)", list_insert },
	{ "LIST_SET",          "LIST_SET    (<bin>, <index>, <val>)", list_set },
	{ "LIST_GET",          "LIST_GET    (<bin>, <index>)       ", list_get },
	{ "LIST_POP",          "LIST_POP    (<bin>, <index>)       ", list_pop },
	{ "LIST_REMOVE",       "LIST_REMOVE (<bin>, <index>)       ", list_remove },
	{ "LIST_APPEND_ITEMS", "LIST_APPEND_ITEMS (<bin>, <list of vals>)         ", list_appenditems },
	{ "LIST_INSERT_ITEMS", "LIST_INSERT_ITEMS (<bin>, <index>, <list of vals>)", list_insertitems },
	{ "LIST_GET_RANGE",    "LIST_GET_RANGE    (<bin>, <startindex>[, <count>])", list_getrange },
	{ "LIST_POP_RANGE",    "LIST_POP_RANGE    (<bin>, <startindex>[, <count>])", list_poprange },
	{ "LIST_REMOVE_RANGE", "LIST_REMOVE_RANGE (<bin>, <startindex>[, <count>])", list_removerange },
	{ "LIST_TRIM",         "LIST_TRIM         (<bin>, <startindex>[, <count>])", list_trim },
	{ "LIST_INCREMENT",    "LIST_INCREMENT    (<bin>, <index>, <numeric val>) ", list_increment },
	{ "LIST_CLEAR",        "LIST_CLEAR        (<bin>) ", list_clear },
	{ "LIST_SIZE",         "LIST_SIZE         (<bin>) ", list_size },
	
/*
	{ "LIST_GET_BY_INDEX", list_getbyindex },
	{ "LIST_GET_BY_VALUE", list_getbyvalue },
	{ "LIST_GET_BY_RANK", list_getbyrank },
	{ "LIST_GETALL_BY_VALUE", list_getallbyvalue },
	{ "LIST_GETALL_BY_VALUE_LIST", list_getbyvaluelist },
	{ "LIST_GET_BY_INDEX_RANGE", list_getbyindexrange },
	{ "LIST_GET_BY_VALUE_INTERVAL", list_getbyvalueininterval },
	{ "LIST_GET_BY_RANK_RANGE", list_getbyrankrange },
	
	{ "LIST_REMOVE_BY_INDEX", list_removebyindex },
	{ "LIST_REMOVE_BY_VALUE", list_removebyvalue },
	{ "LIST_REMOVE_BY_RANK", list_removebyrank },
	{ "LIST_REMOVEALL_BY_VALUE", list_removeallbyvalue },
	{ "LIST_REMOVE_BY_VALUE_LIST", list_removebyvaluelist },
	{ "LIST_REMOVE_BY_INDEX_RANGE", list_removebyindexrange },
	{ "LIST_REMOVE_BY_INTERVAL_RANGE", list_removebyvalueininterval },
	{ "LIST_REMOVE_BY_RANK_RANGE", list_getbyrankrange },
*/

	{ "MAP_PUT",             "MAP_PUT             (<bin>, <key>, <val>) [with_policy (<map policy>)]", map_put },
	{ "MAP_PUT_ITEMS",       "MAP_PUT_ITEMS       (<bin>, <map>)  [with_policy (<map policy>)]", map_putitems },
	{ "MAP_INCREMENT",       "MAP_INCREMENT       (<bin>, <key>, <numeric val>) [with_policy (<map policy>)]", map_increment },
	{ "MAP_DECREMENT",       "MAP_DECREMENT       (<bin>, <key>, <numeric val>) [with_policy (<map policy>)]", map_decrement },
	{ "MAP_GET_BY_KEY",      "MAP_GET_BY_KEY      (<bin>, <key>)  ", map_getbykey },
	{ "MAP_REMOVE_BY_KEY",   "MAP_REMOVE_BY_KEY   (<bin>, <key>)  ", map_removebykey },
	{ "MAP_GET_BY_VALUE",    "MAP_GET_BY_VALUE    (<bin>, <value>)", map_getbyvalue },
	{ "MAP_REMOVE_BY_VALUE", "MAP_REMOVE_BY_VALUE (<bin>, <value>)", map_removebyvalue },
	{ "MAP_GET_BY_INDEX",    "MAP_GET_BY_INDEX    (<bin>, <index>)", map_getbyindex },
	{ "MAP_REMOVE_BY_INDEX", "MAP_REMOVE_BY_INDEX (<bin>, <index>)", map_removebyindex },
	{ "MAP_GET_BY_RANK",     "MAP_GET_BY_RANK     (<bin>, <rank>) ", map_getbyrank },
	{ "MAP_REMOVE_BY_RANK",  "MAP_REMOVE_BY_RANK  (<bin>, <rank>) ", map_removebyrank},
	{ "MAP_REMOVE_BY_KEY_LIST",    "MAP_REMOVE_BY_KEY_LIST    (<bin>, <list of keys>)         ", map_removebykeylist },
	{ "MAP_REMOVE_BY_VALUE_LIST",  "MAP_REMOVE_BY_VALUE_LIST  (<bin>, <list of vals>)         ", map_removebyvaluelist },
	{ "MAP_GET_BY_KEY_RANGE",      "MAP_GET_BY_KEY_RANGE      (<bin>, <startkey>, <endkey>)   ", map_getbykeyrange },
	{ "MAP_REMOVE_BY_KEY_RANGE",   "MAP_REMOVEBY_RANGE        (<bin>, <startkey>, <endkey>)   ", map_removebykeyrange },
	{ "MAP_GET_BY_VALUE_RANGE",    "MAP_GET_BY_VALUE_RANGE    (<bin>, <startval>, <endval>)   ", map_getbyvaluerange },
	{ "MAP_REMOVE_BY_VALUE_RANGE", "MAP_REMOVE_BY_VALUE_RANGE (<bin>, <startval>, <endval>)   ", map_removebyvaluerange },
	{ "MAP_GET_BY_INDEX_RANGE",    "MAP_GET_BY_INDEX_RANGE    (<bin>, <startindex>[, <count>])", map_getbyindexrange },
	{ "MAP_REMOVE_BY_INDEX_RANGE", "MAP_REMOVE_BY_INDEX_RANGE (<bin>, <startindex>[, <count>])", map_removebyindexrange },
	{ "MAP_GET_BY_RANK_RANGE",     "MAP_GET_BY_RANK_RANGE     (<bin>, <startrank> [, <count>])", map_getbyrankrange },
	{ "MAP_REMOVE_BY_RANK_RANGE",  "MAP_REMOVE_BY_RANK_RANGE  (<bin>, <startrank> [, <count>])", map_removebyrankrange},
	{ "MAP_CLEAR",                 "MAP_CLEAR     (<bin>) ", map_clear },
	{ "MAP_SET_TYPE",            "MAP_SET_TYPE  (<bin>, <map type>) ", map_settype },
	{ "MAP_SIZE",                  "MAP_SIZE      (<bin>) ", map_size },


	{ "TOUCH",   "TOUCH   ()            ", touch },
	{ "READ",    "READ    (<bin>)       ", readop },
	{ "WRITE",   "WRITE   (<bin>, <val>)", writeop },
	{ "DELETE",  "DELETE  ()            ", deleteop },
	{ "PREPEND", "PREPEND (<bin>, <val>)", prepend },
	{ "APPEND",  "APPEND  (<bin>, <val>)", append },
	{ "INCR",    "INCR    (<bin>, <numeric val>)", incr }
};

//=========================================================
// Inlines & Macros.
//

#define CHECK_VAL(op, val) 		    \
	if (!(val)) {					\
		char msg[256];				\
		snprintf(msg, 255, "Operation '%s' missing parameter.", (op)->cmd); \
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);			\
		return false;              \
	}


//=========================================================
// Public API.
//

void
asql_print_ops(char* indent)
{
	fprintf(stdout, "%sOP\n", indent);
	for (uint16_t cmd_idx = 0; cmd_idx < OPERATE_MAX_CMD; cmd_idx++) {
		fprintf(stdout, "%s    %s\n", indent, op_table[cmd_idx].help);
	}
}

int
aql_operate(asql_config* c, aconfig* ac)
{
	operate_config* o = (operate_config*)ac;

	as_operations ops;
	as_operations_init(&ops, o->binops->size);

	if (!operations_init(o, &ops)) {
		as_operations_destroy(&ops);
		return 1;
	}

	as_error err;
	as_error_init(&err);

	as_policy_operate policy;
	as_policy_operate_init(&policy);

	policy.base.total_timeout = c->base.timeout_ms;
	if (c->base.socket_timeout_ms > -1) {
		// set if non-default value
		policy.base.socket_timeout = c->base.socket_timeout_ms;
	}
	policy.durable_delete = c->durable_delete;
	if (c->replica_any) {
		policy.replica = AS_POLICY_REPLICA_ANY;
	}

	if (c->linearize_read) {
		policy.read_mode_sc = AS_POLICY_READ_MODE_SC_LINEARIZE;
	}

	if (o->key.vt == ASQL_VALUE_TYPE_EDIGEST
			|| o->key.vt == ASQL_VALUE_TYPE_DIGEST) {
		policy.key = AS_POLICY_KEY_DIGEST;
	}
	else if (c->key_send) {
		policy.key = AS_POLICY_KEY_SEND;
	}

	as_key key;
	if (key_init(&err, &key, o->ns, o->set, &o->key) != 0) {
		as_operations_destroy(&ops);
		g_renderer->render_error(err.code, err.message, NULL);
		return 1;
	}

	ops.ttl = c->record_ttl_sec;

	as_record* rec = 0;
	aerospike_key_operate(g_aerospike, &err, &policy, &key, &ops, &rec);

	if (err.code == AEROSPIKE_OK) {

		if (rec) {
			print_rec(rec, NULL);
		}
		else {
			g_renderer->render_ok("1 record affected.", NULL);
		}
	}
	else {
		g_renderer->render_error(err.code, err.message, NULL);
	}

	as_record_destroy(rec);
	as_operations_destroy(&ops);

	return 0;
}


//=========================================================
// Local Helper.
//

static bool
operations_init(operate_config* o, as_operations* ops)
{
	for (uint16_t op_idx = 0; op_idx < o->binops->size; op_idx++) {

		binop* op = as_vector_get(o->binops, op_idx);

		bool found = false;

		for (uint16_t cmd_idx = 0; cmd_idx < OPERATE_MAX_CMD; cmd_idx++) {
			if (strcasecmp(op->cmd, op_table[cmd_idx].cmd)) {
				continue;
			}

			if (!ops_populate(cmd_idx, op, ops)) {
				return false;
			}
			found = true;
			break;
		}

		if (!found) {
			char msg[256];
			snprintf(msg, 255, "Operate command '%s' not found.", op->cmd);
			g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
			return false;
		}
	}
	return true;
}

static bool
ops_populate(uint16_t cmd_idx, binop* op, as_operations* ops)
{
	if (!op_table[cmd_idx].fn) {
		return false;
	}

	as_arraylist arglist;

	if (op->params) {
		as_arraylist_inita(&arglist, op->params->size);

		as_error err;
		as_error_init(&err);

		asql_set_args(&err, op->params, &arglist);

		if (err.code != AEROSPIKE_OK) {
			as_arraylist_destroy(&arglist);
			g_renderer->render_error(err.code, err.message, NULL);
			return false;
		}
	}
	else {
		as_arraylist_inita(&arglist, 0);
	}

	bool ret = op_table[cmd_idx].fn(op, ops, (as_list*)&arglist);

	as_arraylist_destroy(&arglist);

	return ret;
}


static bool
policy_check(binop* op, bool no_policy)
{
	if (no_policy && op->policy) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' does not support policy.",
				op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}
	return true;
}

static bool
params_check(binop* op, as_list* params, uint16_t min_params,
		uint16_t max_params)
{
	as_arraylist* arglist = (as_arraylist*)params;
	if ((as_arraylist_size(arglist) > max_params) ||
			(as_arraylist_size(arglist) < min_params)) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid parameter count, expected (%d to %d)",
				op->cmd, min_params, max_params);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}
	return true;
}

static bool
get_val(binop* op, as_list* params, as_val** val)
{
	if (!params_check(op, params, 1, 1)) {
		return false;
	}

	as_arraylist* arglist = (as_arraylist*)params;
	*val= as_arraylist_get(arglist, 0);
	
	CHECK_VAL(op, *val);

	return true;
}

static bool
get_valmap(binop* op, as_list* params, as_val** val)
{
	if (!params_check(op, params, 1, 1)) {
		return false;
	}

	as_arraylist* arglist = (as_arraylist*)params;
	*val = as_arraylist_get(arglist, 0);

	CHECK_VAL(op, *val);

	if (as_val_type(*val) != AS_MAP) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid non-map parameter.",
				op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}
	return true;
}

static bool
get_vallist(binop* op, as_list* params, as_val** val)
{
	if (!params_check(op, params, 1, 1)) {
		return false;
	}

	as_arraylist* arglist = (as_arraylist*)params;
	*val = as_arraylist_get(arglist, 0);

	CHECK_VAL(op, *val);

	if (as_val_type(*val) != AS_LIST) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid non-list parameter.",
				op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}
	return true;
}

static bool
get_index(binop* op, as_list* params, int64_t* index)
{
	if (!params_check(op, params, 1, 1)) {
		return false;
	}

	as_arraylist* arglist = (as_arraylist*)params;
	as_val* val = as_arraylist_get(arglist, 0);

	CHECK_VAL(op, val);

	if (as_val_type(val) != AS_INTEGER) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid index parameter.",
				op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}

	*index = as_integer_get((as_integer*)val);
	return true;
}

static bool
get_indexrange(binop* op, as_list* params, int64_t* index, int64_t* count)
{
	if (!params_check(op, params, 1, 2)) {
		return false;
	}

	as_arraylist* arglist = (as_arraylist*)params;

	as_val* val = as_arraylist_get(arglist, 0);

	CHECK_VAL(op, val);

	if (as_val_type(val) != AS_INTEGER) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid index parameter.", op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}
	*index = as_integer_get((as_integer*)val);

	// Count left as is if no in parameters.
	val = as_arraylist_get(arglist, 1);
	if (!val) {
		return true;
	}

	if (as_val_type(val) != AS_INTEGER) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid range parameter.", op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}
	*count = as_integer_get((as_integer*)val);
	
	// Count can't be negative.
	if (*count < 0) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid negative range parameter.",
				op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}
	return true;
}

static bool
get_indexval(binop* op, as_list* params, int64_t* index, as_val** out_val)
{
	if (!params_check(op, params, 2, 2)) {
		return false;
	}

	as_arraylist* arglist = (as_arraylist*)params;
	as_val* val = as_arraylist_get(arglist, 0);

	CHECK_VAL(op, val);

	if (as_val_type(val) != AS_INTEGER) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid index parameter.",
				op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}

	*index = as_integer_get((as_integer*)val);
	*out_val = as_arraylist_get(arglist, 1);
	return true;
}


static bool
get_valstring(binop* op, as_list* params, as_val** val)
{
	as_arraylist* arglist = (as_arraylist*)params;

	*val = as_arraylist_get(arglist, 0);

	CHECK_VAL(op, *val);

	if (as_val_type(*val) != AS_STRING) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid non-string parameter.",
				op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}
	return true;
}

static bool
get_valnumeric(binop* op, as_list* params, int64_t* icount, double* dcount)
{
	as_arraylist* arglist = (as_arraylist*)params;

	as_val* val = as_arraylist_get(arglist, 0);

	CHECK_VAL(op, val);

	if ((as_val_type(val) != AS_INTEGER)
			&& (as_val_type(val) != AS_DOUBLE)) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid non-numeric increment parameter.",
				op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}

	if (as_val_type(val) == AS_INTEGER) {
		*icount = as_integer_get((as_integer*)val);
	}
	else {
		*dcount = as_double_get((as_double*)val);
	}

	if (*icount == 0 && *dcount == 0) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid zero increment parameter.",
				op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}
	return true;
}

static bool
get_twoval(binop* op, as_list* params, as_val** val1, as_val** val2)
{
	if (!params_check(op, params, 2, 2)) {
		return false;
	}

	as_arraylist* arglist = (as_arraylist*)params;

	*val1 = as_arraylist_get(arglist, 0);
	CHECK_VAL(op, *val1);
	*val2 = as_arraylist_get(arglist, 1);
	CHECK_VAL(op, *val2);
	return true;
}

// TODO: allow for return type option as well
static bool
fill_policy(binop* op, as_map_policy* policy, as_map_return_type* rtype)
{
	if (!op->policy) {
		return true;
	}

	as_map_order order = AS_MAP_UNORDERED;
	as_map_write_mode mode = AS_MAP_UPDATE;

	for (uint16_t i = 0; i < op->policy->size; i++)
	{
		asql_name name = as_vector_get_ptr(op->policy, i);
		if (!strcasecmp(name, "AS_MAP_UNORDERED")) {
			order = AS_MAP_UNORDERED;
		}
		else if (!strcasecmp(name, "AS_MAP_KEY_ORDERED")) {
			order = AS_MAP_KEY_ORDERED;
		}
		else if (!strcasecmp(name, "AS_MAP_KEY_VALUE_ORDERED")) {
			order = AS_MAP_KEY_VALUE_ORDERED;
		}
		else if (!strcasecmp(name, "AS_MAP_UPDATE")) {
			mode = AS_MAP_UPDATE;
		}
		else if (!strcasecmp(name, "AS_MAP_UPDATE_ONLY")) {
			mode = AS_MAP_UPDATE_ONLY;
		}
		else if (!strcasecmp(name, "AS_MAP_CREATE_ONLY")) {
			mode = AS_MAP_CREATE_ONLY;
		}
		else {
			char msg[256];
			snprintf(msg, 255, "Operation '%s' invalid policy parameter.", op->cmd);
			g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
			return false;
		}
	}
	as_map_policy_set(policy, order, mode);
	return true;
}

static bool
list_append(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	as_val* val;
	if (!get_val(op, params, &val)) {
		return false;
	}

	as_val_reserve(val);
	as_operations_add_list_append(ops, op->bname, val);

	return true;
}

static bool
list_appenditems(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	as_val* val;
	if (!get_vallist(op, params, &val)) {
		return false;
	}

	as_val_reserve(val);
	as_operations_add_list_append_items(ops, op->bname, (as_list*)val);

	return true;
}

static bool
list_insert(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index;
	as_val* val;
	if (!get_indexval(op, params, &index, &val)) {
		return false;
	}

	as_val_reserve(val);
	as_operations_add_list_insert(ops, op->bname, index, val);
	return true;
}

static bool
list_insertitems(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index;
	as_val* val;
	if (!get_indexval(op, params, &index, &val)) {
		return false;
	}
	if (as_val_type(val) != AS_LIST) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid non-list parameter.",
				op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}

	as_val_reserve(val);
	as_operations_add_list_insert_items(ops, op->bname, index, (as_list*)val);
	return true;
}

static bool
list_pop(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index;
  	if (!get_index(op, params, &index)) {
		return false;
	}

	as_operations_add_list_pop(ops, op->bname, index);
	return true;
}

static bool
list_poprange(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index = 0;
	int64_t count = -1;

	if (!get_indexrange(op, params, &index, &count)) {
		return false;
	}

	if (count >= 0) {
		as_operations_add_list_pop_range(ops, op->bname, index, count);
	}
	else {
		as_operations_add_list_pop_range_from(ops, op->bname, index);
	}

	return true;
}

static bool
list_remove(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index;

	if (!get_index(op, params, &index)) {
		return false;
	}

	as_operations_add_list_remove(ops, op->bname, index);

	return true;
}

static bool
list_removerange(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index = 0;
	int64_t count = -1;

	if (!get_indexrange(op, params, &index, &count)) {
		return false;
	}

	if (count >= 0) {
		as_operations_add_list_remove_range(ops, op->bname, index, count);
	}
	else {
		as_operations_add_list_remove_range_from(ops, op->bname, index);
	}

	return true;
}

static bool
list_set(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index = 0;
	as_val* val;

	if (!get_indexval(op, params, &index, &val)) {
		return false;
	}
	as_val_reserve(val);
	as_operations_add_list_set(ops, op->bname, index, val);

	return true;
}

static bool
list_trim(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index = 0;
	int64_t count = -1;

	if (!get_indexrange(op, params, &index, &count)) {
		return false;
	}

	// NB: In case count is -1, it gets typecasted
	// to the huge number and hence would trim eveything
	// from index to the end of list. Though server does
	// not provide such client API aql does it that way
	// to keep the behavior similar to behavior of other
	// range ops.
	as_operations_add_list_trim(ops, op->bname, index, count);

	return true;
}

static bool
list_clear(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)
			|| !params_check(op, params, 0, 0)) {
		return false;
	}
	as_operations_add_list_clear(ops, op->bname);
	return true;
}

static bool
list_increment(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index = 0;
	as_val* val;

	if (!get_indexval(op, params, &index, &val)) {
		return false;
	}

	if (as_val_type(val) != AS_INTEGER) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid index parameter.", op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}

	as_val_reserve(val);
	as_operations_add_list_increment(ops, op->bname, index, val);

	return true;
}

static bool
list_size(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)
			|| !params_check(op, params, 0, 0)) {
		return false;
	}
	as_operations_add_list_size(ops, op->bname);
	return true;
}

static bool
list_get(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index;

	if (!get_index(op, params, &index)) {
		return false;
	}

	as_operations_add_list_get(ops, op->bname, index);

	return true;
}

static bool
list_getrange(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index = 0;
	int64_t count = -1;

	if (!get_indexrange(op, params, &index, &count)) {
		return false;
	}

	if (count >= 0) {
		as_operations_add_list_get_range(ops, op->bname, index, count);
	}
	else {
		as_operations_add_list_get_range_from(ops, op->bname, index);
	}

	return true;
}

static bool
map_put(binop* op, as_operations* ops, as_list* params)
{
	as_map_policy policy;
	as_map_policy_init(&policy);

	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;

	if (!fill_policy(op, &policy, &rtype)) {
		return false;
	}

	as_val* key;
	as_val* val;
	if (!get_twoval(op, params, &key, &val)) {
		return false;
	}

	as_val_reserve(key);
	as_val_reserve(val);
	as_operations_add_map_put(ops, op->bname, &policy, key, val);

	return true;
}

static bool
map_putitems(binop* op, as_operations* ops, as_list* params)
{
	as_map_policy policy;
	as_map_policy_init(&policy);

	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;

	if (!fill_policy(op, &policy, &rtype)) {
		return false;
	}

	as_val* val;
	if (!get_valmap(op, params, &val)) {
		return false;
	}
	as_val_reserve(val);
	as_operations_add_map_put_items(ops, op->bname, &policy, (as_map*)val);
	return true;
}

static bool
map_increment(binop* op, as_operations* ops, as_list* params)
{
	as_map_policy policy;
	as_map_policy_init(&policy);

	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;

	if (!fill_policy(op, &policy, &rtype)) {
		return false;
	}

	as_val* key;
	as_val* val;

	if (!get_twoval(op, params, &key, &val)) {
		return false;
	}

	if ((as_val_type(val) != AS_INTEGER)
			&& (as_val_type(val) != AS_DOUBLE)) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid non-numeric increment parameter.",
				op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}

	as_val_reserve(key);
	as_val_reserve(val);
	as_operations_add_map_increment(ops, op->bname, &policy, key, val);

	return true;
}

static bool
map_decrement(binop* op, as_operations* ops, as_list* params)
{
	as_map_policy policy;
	as_map_policy_init(&policy);

	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;

	if (!fill_policy(op, &policy, &rtype)) {
		return false;
	}

	as_val* key;
	as_val* val;

	if (!get_twoval(op, params, &key, &val)) {
		return false;
	}

	if ((as_val_type(val) != AS_INTEGER)
			&& (as_val_type(val) != AS_DOUBLE)) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid non-numeric decrement parameter.",
				op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}

	as_val_reserve(key);
	as_val_reserve(val);
	as_operations_add_map_decrement(ops, op->bname, &policy, key, val);

	return true;
}

static bool
map_clear(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	as_operations_add_map_clear(ops, op->bname);
	return true;
}

static bool
map_settype(binop* op, as_operations* ops, as_list* params)
{
	if (!params_check(op, params, 1, 1)) {
		return false;
	}

	as_map_policy policy;

	as_arraylist* arglist = (as_arraylist*)params;
	as_val* val = as_arraylist_get(arglist, 0);

	// NB: needed to keep using params which end up 
	// parsing quoted values as '"str"'
	asql_value value;
	asql_parse_value_as(as_val_tostring(val), &value, ASQL_VALUE_TYPE_STRING);

	if (!strcasecmp(value.u.str, "AS_MAP_UNORDERED")) {
		as_map_policy_set(&policy, AS_MAP_UNORDERED, AS_MAP_UPDATE);
	}
	else if (!strcasecmp(value.u.str, "AS_MAP_KEY_ORDERED")) {
		as_map_policy_set(&policy, AS_MAP_KEY_ORDERED, AS_MAP_UPDATE);
	}
	else if (!strcasecmp(value.u.str, "AS_MAP_KEY_VALUE_ORDERED")) {
		as_map_policy_set(&policy, AS_MAP_KEY_VALUE_ORDERED, AS_MAP_UPDATE);
	}
	else {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid policy parameter.", value.u.str);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}

	as_operations_add_map_set_policy(ops, op->bname, &policy);

	return true;
}

static bool
map_removebykey(binop* op, as_operations* ops, as_list* params)
{
	as_val* key;
	if (!get_val(op, params, &key)) {
		return false;
	}
	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_val_reserve(key);
	as_operations_add_map_remove_by_key(ops, op->bname, key, rtype);
	return true;
}

static bool
map_removebykeylist(binop* op, as_operations* ops, as_list* params)
{
	as_val* key_list;
	if (!get_vallist(op, params, &key_list)) {
		return false;
	}

	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_val_reserve(key_list);
	as_operations_add_map_remove_by_key_list(ops, op->bname,
			(as_list*)key_list, rtype);

	return true;
}

static bool
map_removebykeyrange(binop* op, as_operations* ops, as_list* params)
{
	as_val* begin_key;
	as_val* end_key;
	if (!get_twoval(op, params, &begin_key, &end_key)) {
		return false;
	}
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_val_reserve(begin_key);
	as_val_reserve(end_key);
	as_operations_add_map_remove_by_key_range(ops, op->bname, begin_key,
			end_key, rtype);

	return true;
}

static bool
map_removebyvalue(binop* op, as_operations* ops, as_list* params)
{
	as_val* val;
	if (!get_val(op, params, &val)) {
		return false;
	}

	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_val_reserve(val);
	as_operations_add_map_remove_by_value(ops, op->bname, val, rtype);
	return true;
}

static bool
map_removebyvaluelist(binop* op, as_operations* ops, as_list* params)
{
	as_val* val;
	if (!get_vallist(op, params, &val)) {
		return false;
	}

	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_val_reserve(val);
	as_operations_add_map_remove_by_value_list(ops, op->bname, (as_list*)val,
			rtype);

	return true;
}

static bool
map_removebyvaluerange(binop* op, as_operations* ops, as_list* params)
{
	as_val* begin_val;
	as_val* end_val;

	if (!get_twoval(op, params, &begin_val, &end_val)) {
		return false;
	}

	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_val_reserve(begin_val);
	as_val_reserve(end_val);
	as_operations_add_map_remove_by_value_range(ops, op->bname, begin_val,
			end_val, rtype);

	return true;
}

static bool
map_removebyindex(binop* op, as_operations* ops, as_list* params)
{
	int64_t index;
  	if (!get_index(op, params, &index)) {
		return false;
	}

	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_operations_add_map_remove_by_index(ops, op->bname, index, rtype);
	return true;
}

static bool
map_removebyindexrange(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index = 0;
	int64_t count = -1;

	if (!get_indexrange(op, params, &index, &count)) {
		return false;
	}

	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	if (count >= 0) {
		as_operations_add_map_remove_by_index_range(ops, op->bname, index,
				count, rtype);
	}
	else {
		as_operations_add_map_remove_by_index_range_to_end(ops, op->bname,
				index, rtype);
	}

	return true;
}

static bool
map_removebyrank(binop* op, as_operations* ops, as_list* params)
{
	int64_t rank;
  	if (!get_index(op, params, &rank)) {
		return false;
	}

	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_operations_add_map_remove_by_rank(ops, op->bname, rank, rtype);
	return true;
}

static bool
map_removebyrankrange(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t rank = 0;
	int64_t count = -1;

	if (!get_indexrange(op, params, &rank, &count)) {
		return false;
	}

	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	if (count >= 0) {
		as_operations_add_map_remove_by_rank_range(ops, op->bname, rank,
				count, rtype);
	}
	else {
		as_operations_add_map_remove_by_rank_range_to_end(ops, op->bname,
				rank, rtype);
	}
	return true;
}

static bool
map_size(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)
			|| !params_check(op, params, 0, 0)) {
		return false;
	}
	as_operations_add_map_size(ops, op->bname);
	return true;
}

static bool
map_getbykey(binop* op, as_operations* ops, as_list* params)
{
	as_val* key;
	if (!get_val(op, params, &key)) {
		return false;
	}
	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_val_reserve(key);
	as_operations_add_map_get_by_key(ops, op->bname, key, rtype);
	return true;
}

static bool
map_getbykeyrange(binop* op, as_operations* ops, as_list* params)
{
	as_val* begin_key;
	as_val* end_key;
	if (!get_twoval(op, params, &begin_key, &end_key)) {
		return false;
	}
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_val_reserve(begin_key);
	as_val_reserve(end_key);
	as_operations_add_map_get_by_key_range(ops, op->bname, begin_key,
			end_key, rtype);
	return true;
}

static bool
map_getbyvalue(binop* op, as_operations* ops, as_list* params)
{
	as_val* val;
	if (!get_val(op, params, &val)) {
		return false;
	}

	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_val_reserve(val);
	as_operations_add_map_get_by_value(ops, op->bname, val, rtype);
	return true;
}

static bool
map_getbyvaluerange(binop* op, as_operations* ops, as_list* params)
{
	as_val* begin_val;
	as_val* end_val;

	if (!get_twoval(op, params, &begin_val, &end_val)) {
		return false;
	}

	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_val_reserve(begin_val);
	as_val_reserve(end_val);
	as_operations_add_map_get_by_value_range(ops, op->bname, begin_val,
			end_val, rtype);

	return true;
}

static bool
map_getbyindex(binop* op, as_operations* ops, as_list* params)
{
	int64_t index;
  	if (!get_index(op, params, &index)) {
		return false;
	}

	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_operations_add_map_get_by_index(ops, op->bname, index, rtype);
	return true;
}

static bool
map_getbyindexrange(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t index = 0;
	int64_t count = -1;

	if (!get_indexrange(op, params, &index, &count)) {
		return false;
	}

	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	if (count >= 0) {
		as_operations_add_map_get_by_index_range(ops, op->bname, index,
				count, rtype);
	}
	else {
		as_operations_add_map_get_by_index_range_to_end(ops, op->bname,
				index, rtype);
	}
	return true;
}

static bool
map_getbyrank(binop* op, as_operations* ops, as_list* params)
{
	int64_t rank;
  	if (!get_index(op, params, &rank)) {
		return false;
	}

	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	as_operations_add_map_get_by_rank(ops, op->bname, rank, rtype);
	return true;
}

static bool
map_getbyrankrange(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t rank = 0;
	int64_t count = -1;

	if (!get_indexrange(op, params, &rank, &count)) {
		return false;
	}

	// TODO: parametrize it
	as_map_return_type rtype = AS_MAP_RETURN_KEY_VALUE;
	if (count >= 0) {
		as_operations_add_map_get_by_rank_range(ops, op->bname, rank,
				count, rtype);
	}
	else {
		as_operations_add_map_get_by_rank_range_to_end(ops, op->bname,
				rank, rtype);
	}
	return true;
}

static bool
readop(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)
			|| !params_check(op, params, 0, 0)) {
		return false;
	}
	as_operations_add_read(ops, op->bname);
	return true;
}

static bool
writeop(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	as_val* val;
	if (!get_val(op, params, &val)) {

	}

	as_val_reserve(val);
	as_operations_add_write(ops, op->bname, (as_bin_value*)val);
	return true;
}

static bool
incr(binop* op, as_operations* ops, as_list* params)
{
	if (!policy_check(op, true)) {
		return false;
	}

	int64_t icount = 0;
	double  dcount = 0;
	if (!get_valnumeric(op, params, &icount, &dcount)) {
		return false;
	}

	if (icount) {
		as_operations_add_incr(ops, op->bname, icount);
	}
	else {
		as_operations_add_incr_double(ops, op->bname, dcount);
	}
	return true;
}

static bool
prepend(binop* op, as_operations* ops, as_list* params)
{
	if (!params_check(op, params, 1, 1)
			|| !policy_check(op, true)) {
		return false;
	}

	as_val* val;
	if (!get_valstring(op, params, &val)) {
		return false;
	}

	as_operations_add_prepend_strp(ops, op->bname,
			strdup(as_string_get((as_string*)val)), true);

	return true;
}

static bool
append(binop* op, as_operations* ops, as_list* params)
{
	if (!params_check(op, params, 1, 1)
			|| !policy_check(op, true)) {
		return false;
	}

	as_val* val;
	if (!get_valstring(op, params, &val)) {
		return false;
	}

	as_operations_add_append_strp(ops, op->bname,
			strdup(as_string_get((as_string*)val)), true);

	return true;
}

static bool
touch(binop* op, as_operations* ops, as_list* params)
{
	if (op->bname) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid parameter.", op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}

	if (!params_check(op, params, 0, 0)
			|| !policy_check(op, true)) {
		return false;
	}

	as_operations_add_touch(ops);

	return true;
}

static bool
deleteop(binop* op, as_operations* ops, as_list* params)
{
	if (op->bname) {
		char msg[256];
		snprintf(msg, 255, "Operation '%s' invalid parameter.", op->cmd);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, msg, NULL);
		return false;
	}

	if (!params_check(op, params, 0, 0)
			|| !policy_check(op, true)) {
		return false;
	}

	as_operations_add_delete(ops);

	return true;
}
