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


#include <aerospike/aerospike.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/aerospike_info.h>
#include <aerospike/aerospike_stats.h>
#include <aerospike/as_aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_query.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_record.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_list.h>

#include <renderer.h>
#include <json.h>
#include <asql.h>
#include <asql_query.h>
#include <asql_info.h>
#include <asql_info_parser.h>
#include <asql_log.h>
#include <stdatomic.h>


//==========================================================
// Typedefs & constants.
//

typedef struct {
	char name[64];
	void* rview;
	uint64_t start;
} asql_query_data;

typedef struct {
	void* rview;
	bool limit_set;
	atomic_int record_limit;
} query_cb_udata;


//==========================================================
// Forward Declarations.
//

extern void asql_record_set_renderer(as_record* rec, as_hashmap* m, char* bin_name, as_val* val);
static bool cardinality_callback(const as_error *err, const as_node *node, const char *req, char *res, void *udata);
static uint32_t get_bin_cardinality(as_error* err, asql_name ns, asql_name index_name);
static int compare_bin_cardinality(const asql_name ns, const asql_name set, asql_name ibname, asql_name ibname2, bool* exists, as_error *err);
static void populate_filter_exp(as_exp **filter, asql_where *where, as_error *err);
static int populate_where(as_query* query, as_policy_query* policy, sk_config* s, as_error *err);
static bool query_callback(const as_val* val, void* udata);
static int query_select(asql_config* c, sk_config* s);
static int query_execute(asql_config* c, sk_config* s);
static bool query_agg_renderer(const as_val* val, void* udata);


//==========================================================
// Public API.
//

int
asql_query_aggregate(asql_config* c, sk_config* s)
{
	as_error err;
	as_error_init(&err);

	as_policy_query query_policy;
	as_policy_query_init(&query_policy);
	query_policy.base.total_timeout = c->base.timeout_ms;
	if (c->base.socket_timeout_ms > -1) {
		// set if non-default value
		query_policy.base.socket_timeout = c->base.socket_timeout_ms;
	}

	if (strlen(s->ns) >= AS_NAMESPACE_MAX_SIZE) {
		char err_msg[1024];
		snprintf(err_msg, 1023, "Namespace name is too long: '%s'", s->ns);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, err_msg, NULL);
		return 1;
	}

	if (s->set && (strlen(s->set) >= AS_SET_MAX_SIZE)) {
		char err_msg[1024];
		snprintf(err_msg, 1023, "Set name is too long: '%s'", s->set);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, err_msg, NULL);
		return 1;
	}

	as_query query;
	as_query_init(&query, s->ns, s->set);

	as_arraylist arglist;
	if (!s->u.params) {
		as_arraylist_inita(&arglist, 0);
	}
	else {
		as_arraylist_inita(&arglist, s->u.params->size);
		asql_set_args(&err, s->u.params, &arglist);
	}

	if (err.code == AEROSPIKE_OK) {
		if (s->type == SECONDARY_INDEX_OP) {
			// NB: There is no scan API for aggregation in C client, query
			// aggregation without where clause is SCAN aggregation.
			// So populate where clause only for sindex query aggregation
			as_query_where_inita(&query, 1);
			populate_where(&query, NULL, s, &err);
		}
	}

	void* rview = g_renderer->view_new(CLUSTER);

	if (err.code == AEROSPIKE_OK) {
		// NB: query object consumes arglist no need for
		// destroy on arglist
		as_query_apply(&query, s->u.udfpkg, s->u.udfname, (as_list*)&arglist);

		asql_query_data data = { .name = { '\0' }, .rview = NULL };

		strncpy(data.name, s->u.udfname, AS_BIN_NAME_MAX_LEN);
		if (strlen(s->u.udfname) > AS_BIN_NAME_MAX_LEN) {
			data.name[AS_BIN_NAME_MAX_LEN - 1] = '.';
			data.name[AS_BIN_NAME_MAX_LEN - 2] = '.';
			data.name[AS_BIN_NAME_MAX_LEN - 3] = '.';
		}
		data.name[AS_BIN_NAME_MAX_LEN] = '\0';
		data.rview = rview;
		data.start = cf_getms();

		aerospike_query_foreach(g_aerospike, &err, &query_policy, &query,
				query_agg_renderer, &data);
	}

	if (err.code == AEROSPIKE_OK) {
		g_renderer->render_ok("", rview);
	} else {
		g_renderer->render_error(err.code, err.message, rview);
	}

	g_renderer->view_destroy(rview);
	as_query_destroy(&query);

	return 0;
}

int
asql_query(asql_config* c, aconfig* ac)
{
	sk_config* s = (sk_config*)ac;

	switch (s->optype) {
		case ASQL_OP_SELECT:
			return query_select(c, s);
		case ASQL_OP_AGGREGATE:
			return asql_query_aggregate(c, s);
		case ASQL_OP_EXECUTE:
			return query_execute(c, s);
		default:
			return 0;
	}
}


//==========================================================
// Local Helpers.
//

static bool cardinality_callback(const as_error *err, const as_node *node, const char *req, char *res, void *udata)
{
	if (err->code != AEROSPIKE_OK) {
		return false;
	}

	char* resp = strdup(info_res_split(res));

	if (resp == NULL) {
		return false;
	}

	double* total_ebp = (double*)udata;
	as_vector* parsed_result = as_vector_create(sizeof(as_hashmap*), 1);

	list_res_parser(parsed_result, node, req, resp);

	as_string key_epb;
	as_string_init(&key_epb, "entries_per_bval", false);
	double ebp;

	as_hashmap* map = as_vector_get_ptr(parsed_result, 0);
	const char* val_epb = as_string_get(as_string_fromval(as_hashmap_get(map, as_string_toval(&key_epb))));

	if (val_epb == NULL) {
		as_string key_entries;
		as_string_init(&key_entries, "entries", false);
		as_string key_keys;
		as_string_init(&key_keys, "keys", false);

		const char* val_keys = as_string_get(as_string_fromval(as_hashmap_get(map, as_string_toval(&key_keys))));

		if (val_keys == NULL) {
			// Unable to determine cardinality. Likely server 6.0 or much older
			as_hashmap_destroy(map);
			as_vector_destroy(parsed_result);
			return true;
		}

		const char* val_entries = as_string_get(as_string_fromval(as_hashmap_get(map, as_string_toval(&key_entries))));

		if (val_entries == NULL) {
			// I think 'entries' has always existed so not sure this is possible.
			as_hashmap_destroy(map);
			as_vector_destroy(parsed_result);
			return true;
		}

		double keys = strtod(val_keys, NULL);
		double entries = strtod(val_entries, NULL);

		ebp = entries / keys;
	}
	else {
		ebp = strtod(val_epb, NULL);
	}
	
	*total_ebp += ebp;

	as_hashmap_destroy(map);
	as_vector_destroy(parsed_result);
	return true;
}

static uint32_t get_bin_cardinality(as_error* err, asql_name ns, asql_name index_name) {
	char *req = malloc(sizeof(char *) * 7 + strlen(ns) + 1 + strlen(index_name) + 1);
	double cardinality = 0.0f;
	double total_epb = 0.0f;
	sprintf(req, "sindex/%s/%s", ns, index_name);

	if (aerospike_info_foreach(g_aerospike, err, NULL, req, cardinality_callback, &total_epb) != AEROSPIKE_OK) {
		return 0;
	}

	as_cluster_stats stats;
	aerospike_stats(g_aerospike, &stats);

	if (stats.nodes_size != 0) {
		cardinality = total_epb / (double)stats.nodes_size;
	}

	aerospike_stats_destroy(&stats);
	free(req);

	return cardinality;
}

static int
compare_bin_cardinality(const asql_name ns, const asql_name set, asql_name ibname, asql_name ibname2, bool* exists, as_error *err)
{
	// returns positive ibname > ibname2
	// returns negative ibname < ibname2
	int rv = 0;
	*exists = true;
	char *req = malloc(sizeof(char *) * 7 + strlen(ns) + 1);
	char* res = NULL;
	sprintf(req, "sindex/%s", ns);
	as_vector* responses = as_vector_create(sizeof(as_hashmap*), 128);

	if (aerospike_info_any(g_aerospike, err, NULL, req, &res) != AEROSPIKE_OK) {
		return 0;
	}

	asql_name ibname_index = NULL;
	asql_name ibname2_index = NULL;
	double ibname_card = 0;
	double ibname2_card = 0;
	as_string bin_key;
	as_string set_key;
	as_string index_key;
	as_string_init(&bin_key, "bin", false);
	as_string_init(&set_key, "set", false);
	as_string_init(&index_key, "indexname", false);

	const char* resp = info_res_split(res);
	if (resp == NULL) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Error: Unable to parse info response: %s", res);
		goto cleanup;
	}

	list_res_parser(responses, NULL, req, resp);

	for (int idx = 0; idx < responses->size; idx++) {
		as_hashmap* map = as_vector_get_ptr(responses, idx);
		char* binVal = as_string_get(as_string_fromval(as_hashmap_get(map, (as_val*)&bin_key)));

		if (binVal == NULL) {
			continue;
		}

		if (!strcmp(binVal, ibname)) {
			char *set_val = as_string_get(as_string_fromval(as_hashmap_get(map, as_string_toval(&set_key))));
			if (set_val != NULL && !strcmp(set_val, set)) {
				ibname_index = as_string_get(as_string_fromval(as_hashmap_get(map, as_string_toval(&index_key))));
			}
		}
		else if (!strcmp(binVal, ibname2)) {
			char *set_val = as_string_get(as_string_fromval(as_hashmap_get(map, as_string_toval(&set_key))));
			if (set_val != NULL && !strcmp(set_val, set)) {
				ibname2_index = as_string_get(as_string_fromval(as_hashmap_get(map, as_string_toval(&index_key))));
			}
		}
	}

	if (ibname_index == NULL && ibname2_index == NULL) {
		*exists = false;
		rv = 0;
		goto cleanup;
	}

	if (ibname_index == NULL && ibname2_index != NULL) {
		rv = 1; 
		goto cleanup;
	}
	
	if (ibname_index != NULL && ibname2_index == NULL) {
		rv = -1; 
		goto cleanup;
	}

	ibname_card = get_bin_cardinality(err, ns, ibname_index);
	ibname2_card = get_bin_cardinality(err, ns, ibname2_index);
	
	if (ibname_card > ibname2_card) {
		rv = 1;
		goto cleanup;
	}

	if (ibname_card < ibname2_card) {
		rv = -1;
		goto cleanup;
	}

	rv = 0;

cleanup:
	for (int idx = 0; idx < responses->size; idx++) {
		as_hashmap* map = as_vector_get_ptr(responses, idx);
		as_hashmap_destroy(map);
	}

	as_vector_destroy(responses);
	return rv;

}

static void
populate_filter_exp(as_exp **filter, asql_where *where, as_error *err) {
	asql_name ibname = where->ibname;
	asql_value* beg = &where->beg;
	as_val_t bin_type = where->beg.type;

	if (bin_type == AS_INTEGER) { // Integer Range
		as_exp_build(tmp, as_exp_cmp_eq(as_exp_bin_int(ibname), as_exp_int(beg->u.i64)));
		*filter = tmp;
	}
	else if (bin_type == AS_STRING) { // String Equality
		as_exp_build(tmp, as_exp_cmp_eq(as_exp_bin_str(ibname), as_exp_str(beg->u.str)));
		*filter = tmp;
	}
	else {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
							"Error: Equality match is only available for int and string bins", bin_type);
	}
}

static int
populate_where(as_query *query, as_policy_query *policy, sk_config *s, as_error *err)
{
	asql_where *chosen_where = NULL;

	if (s->where2) {
		if (policy == NULL) {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
										"Error: Double where clause not supported for this operation");
		}

		char* bin1 = s->where.ibname;
		char* bin2 = s->where2->ibname;
		bool both_exist = true;
		int rv = compare_bin_cardinality(s->ns, s->set, bin1, bin2, &both_exist, err);

		if (err->code != AEROSPIKE_OK) {
			as_error_append(err, "Unable to determine cardinality");
			return err->code;
		}

		if (rv < 0) {
			// use bin1
			chosen_where = &s->where;
			populate_filter_exp(&policy->base.filter_exp, s->where2, err);
		}
		else if (rv > 0) {
			// use bin2
			chosen_where = s->where2;
			populate_filter_exp(&policy->base.filter_exp, &s->where, err);
		}
		else if (rv == 0 && both_exist) {
			// pick bin1 or bin2. we'll arbitrarily pick bin1
			chosen_where = &s->where;
			populate_filter_exp(&policy->base.filter_exp, &s->where, err);
		} 
		else {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
									   "Error: at least one bin needs a secondary index defined");
		}
	} else {
		chosen_where = &s->where;
	}

	asql_name ibname = chosen_where->ibname;
	asql_value *beg = &chosen_where->beg;
	asql_value *end = &chosen_where->end;
	as_val_t bin_type = chosen_where->beg.type;
	asql_query_type_t query_type = s->where.qtype;

	if (s->itype) {
		if (chosen_where->beg.type == AS_INTEGER) { // Integer Range
			if (!strcasecmp(s->itype, "LIST")) {
				as_query_where(query, ibname,
						as_range(LIST, NUMERIC, beg->u.i64, end->u.i64));
			}
			else if (!strcasecmp(s->itype, "MAPKEYS")) {
				as_query_where(query, ibname,
						as_range(MAPKEYS, NUMERIC, beg->u.i64, end->u.i64));
			}
			else if (!strcasecmp(s->itype, "MAPVALUES")) {
				as_query_where(query, ibname,
						as_range(MAPVALUES, NUMERIC, beg->u.i64, end->u.i64));
			}
		}
		else if (bin_type == AS_STRING) { // String Equality
			if (!strcasecmp(s->itype, "LIST")) {
				as_query_where(query, ibname,
						as_contains(LIST, STRING, beg->u.str));
			}
			else if (!strcasecmp(s->itype, "MAPKEYS")) {
				as_query_where(query, ibname,
						as_contains(MAPKEYS, STRING, beg->u.str));
			}
			else if (!strcasecmp(s->itype, "MAPVALUES")) {
				as_query_where(query, ibname,
						as_contains(MAPVALUES, STRING, beg->u.str));
			}
		}
		else if (bin_type == AS_GEOJSON) { // Geospatial Lookup
			if (!strcasecmp(s->itype, "LIST")) {
				as_query_where(query, ibname,
						as_range(LIST, GEO2DSPHERE, beg->u.str, 0));
			}
			else if (!strcasecmp(s->itype, "MAPKEYS")) {
				as_query_where(query, ibname,
						as_range(MAPKEYS, GEO2DSPHERE, beg->u.str, 0));
			}
			else if (!strcasecmp(s->itype, "MAPVALUES")) {
				as_query_where(query, ibname,
						as_range(MAPVALUES, GEO2DSPHERE, beg->u.str, 0));
			}
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
									   "Error: Unknown query data type: %d",
									   bin_type);
		}
	}
	else {
		if (bin_type == AS_INTEGER) { // Integer Range
			as_query_where(query, ibname,
					as_integer_range(beg->u.i64, end->u.i64));
		}
		else if (bin_type == AS_STRING) { // String Equality
			as_query_where(query, ibname, as_string_equals(beg->u.str));
		}
		else if (bin_type == AS_GEOJSON) {
			if (query_type == ASQL_QUERY_TYPE_WITHIN) {
				as_query_where(query, ibname,
						as_geo_within(beg->u.str));
			}
			else if (query_type == ASQL_QUERY_TYPE_CONTAINS) {
				as_query_where(query, ibname,
						as_geo_contains(beg->u.str));
			}
			else {
				return as_error_update(err, AEROSPIKE_ERR_CLIENT,
									"Error: Unknown GeoJSON query type: %d",
									query_type);
			}
		} else {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
								"Error: Unsupported query data type for bin: %s", ibname);
		}
	}
	return 0;
}

static void 
new_query_cb_udata(query_cb_udata *query_udata, void* rview, int64_t record_limit) {
	query_udata->rview = rview;

	if (record_limit == -1) {
		query_udata->limit_set = false;
	} else {
		query_udata->limit_set = true;
	}

	atomic_init(&query_udata->record_limit, record_limit);
}

static bool query_callback(const as_val* val, void* udata) {
	query_cb_udata* query_udata = (query_cb_udata*)udata;

	if (!val) {
		// render must be called with NULL to render table.
		g_renderer->render(val, query_udata->rview);
		return false;
	}

	/*
	 * query.max_records is only supported on servers newer than 6.0.
	 * Older servers require that we set the limit on the client side.
	 */
	if ((query_udata->limit_set && (atomic_fetch_sub(&query_udata->record_limit, 1) < 1)) 
		|| !g_renderer->render(val, query_udata->rview)) {
		// Causes next call to query_callback where val == NULL
		return false;
	}

	return true;
}

// Select query to return : all bins, bin/PK = value/range
static int
query_select(asql_config* c, sk_config* s)
{
	as_error err;
	as_error_init(&err);

	as_policy_query query_policy;
	as_policy_query_init(&query_policy);
	query_policy.base.total_timeout = c->base.timeout_ms;
	if (c->base.socket_timeout_ms > -1) {
		// set if non-default value
		query_policy.base.socket_timeout = c->base.socket_timeout_ms;
	}

	if (strlen(s->ns) >= AS_NAMESPACE_MAX_SIZE) {
		char err_msg[1024];
		snprintf(err_msg, 1023, "Namespace name is too long: '%s'", s->ns);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, err_msg, NULL);
		return 1;
	}

	if (s->set && (strlen(s->set) >= AS_SET_MAX_SIZE)) {
		char err_msg[1024];
		snprintf(err_msg, 1023, "Set name is too long: '%s'", s->set);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, err_msg, NULL);
		return 1;
	}

	as_query query;
	as_query_init(&query, s->ns, s->set);
	query.no_bins = c->no_bins;
	bool select_all = false;

	if (!s->s.bnames) {
		select_all = true;
	}
	else {
		select_all = false;
		as_query_select_inita(&query, s->s.bnames->size);

		for (int i = 0; i < s->s.bnames->size; i++) {
			char* bname = as_vector_get_ptr(s->s.bnames, i);

			if (strlen(bname) > AS_BIN_NAME_MAX_LEN) {
				as_error_update(&err, AEROSPIKE_ERR_CLIENT,
				                "Bin name is too long: '%s'", bname);
				break;
			}
			as_query_select(&query, bname);
		}
	}

	if (err.code == AEROSPIKE_OK) {
		as_query_where_inita(&query, 1);
		populate_where(&query, &query_policy, s, &err);
	}

	if (s->limit) {
		query.max_records = s->limit->u.i64;
	}

	// For each record obtained from the query, invoke the callback function in renderer
	void* rview = g_renderer->view_new(CLUSTER);

	if (err.code == AEROSPIKE_OK) {
		if (! select_all) {
			g_renderer->view_set_cols(s->s.bnames, rview);
		}
		
		query_cb_udata query_udata;
		int64_t max_records = -1;

		if (s->limit) {
			max_records = s->limit->u.i64;
		}

		new_query_cb_udata(&query_udata, rview, max_records);
		aerospike_query_foreach(g_aerospike, &err, &query_policy, &query,
		                        query_callback, &query_udata);
	}

	if (err.code == AEROSPIKE_OK) {
		g_renderer->render_ok("", rview);
	} else if (err.code == AEROSPIKE_ERR_INDEX_NOT_FOUND) {
		as_error_append(&err, "\nMake sure a sindex is created and that strings are enclosed in quotes");
		g_renderer->render_error(err.code, err.message, rview);
	} else {
		g_renderer->render_error(err.code, err.message, rview);
	}

	g_renderer->view_destroy(rview);
	as_query_destroy(&query);
	as_exp_destroy(query_policy.base.filter_exp); // created in populate_where

	return 0;
}

// Execute udf on : all records in a ns.set (or) <rec-PK> = <value>
static int
query_execute(asql_config* c, sk_config* s)
{
	as_error err;
	as_error_init(&err);

	as_policy_write write_policy;
	as_policy_write_init(&write_policy);
	write_policy.base.total_timeout = c->base.timeout_ms;
	write_policy.durable_delete = c->durable_delete;

	if (c->base.socket_timeout_ms > -1) {
		// set if non-default value
		write_policy.base.socket_timeout = c->base.socket_timeout_ms;
	}

	if (strlen(s->ns) >= AS_NAMESPACE_MAX_SIZE) {
		char err_msg[1024];
		snprintf(err_msg, 1023, "Namespace name is too long: '%s'", s->ns);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, err_msg, NULL);
		return 1;
	}

	if (s->set && (strlen(s->set) >= AS_SET_MAX_SIZE)) {
		char err_msg[1024];
		snprintf(err_msg, 1023, "Set name is too long: '%s'", s->set);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, err_msg, NULL);
		return 1;
	}

	as_query query;
	as_query_init(&query, s->ns, s->set);

	as_arraylist arglist;
	if (!s->u.params) {
		as_arraylist_inita(&arglist, 0);
	}
	else {
		as_arraylist_inita(&arglist, s->u.params->size);
		asql_set_args(&err, s->u.params, &arglist);
	}

	uint64_t query_id = 0;
	if (err.code == AEROSPIKE_OK) {
		as_query_where_inita(&query, 1);
		populate_where(&query, NULL, s, &err);
	}

	if (err.code == AEROSPIKE_OK) {
		// NB: query object consumes arglist no need for
		// destroy on arglist
		as_query_apply(&query, s->u.udfpkg, s->u.udfname, (as_list*)&arglist);
		aerospike_query_background(g_aerospike, &err, &write_policy, &query,
				&query_id);
	}

	if (err.code == AEROSPIKE_OK) {
		char ok_msg[1024];
		snprintf(ok_msg, 1023, "Query job (%"PRIu64") created.", query_id);
		g_renderer->render_ok(ok_msg, NULL);
	}
	else {
		g_renderer->render_error(err.code, err.message, NULL);
	}

	as_query_destroy(&query);

	return 0;
}

// Records returned from a query by aql rendered as a table
static bool
query_agg_renderer(const as_val* val, void* udata)
{
	asql_query_data* data = (asql_query_data*)udata;

	if (val) {
		as_record rec;
		as_record_inita(&rec, 1);
		as_hashmap m;
		as_hashmap_init(&m, 2);
		as_val_reserve(val);
		// Consumes val
		asql_record_set_renderer(&rec, &m, data->name, (as_val*)val);

		g_renderer->render((as_val*)&rec, data->rview);

		as_record_destroy(&rec);
		as_hashmap_destroy(&m);
	}
	else {
		g_renderer->render((as_val*) NULL, data->rview);
	}

	return true;
}
