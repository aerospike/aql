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
#include <aerospike/as_aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_query.h>
#include <aerospike/as_record.h>

#include <aerospike/as_arraylist.h>
#include <aerospike/as_list.h>

#include <renderer.h>
#include <json.h>
#include <asql.h>
#include <asql_query.h>


//==========================================================
// Typedefs & constants.
//

typedef struct {
	char name[64];
	void* rview;
	uint64_t start;
} asql_query_data;


//==========================================================
// Forward Declarations.
//

extern void asql_record_set_renderer(as_record* rec, as_hashmap* m, char* bin_name, as_val* val);

static int populate_where(as_query* query, sk_config* s, as_error *err);
static int query_select(asql_config* c, sk_config* s);
static int query_execute(asql_config* c, sk_config* s);
static bool query_renderer(const as_val* val, void* udata);


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
			populate_where(&query, s, &err);
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
				query_renderer, &data);
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

static int
populate_where(as_query* query, sk_config* s, as_error *err)
{
	asql_value *beg = &s->where.beg;
	asql_value *end = &s->where.end;
	as_val_t type = s->where.beg.type;

	if (s->itype) {
		if (type == AS_INTEGER) { // Integer Range
			if (!strcasecmp(s->itype, "LIST")) {
				as_query_where(query, s->ibname,
						as_range(LIST, NUMERIC, beg->u.i64, end->u.i64));
			}
			else if (!strcasecmp(s->itype, "MAPKEYS")) {
				as_query_where(query, s->ibname,
						as_range(MAPKEYS, NUMERIC, beg->u.i64, end->u.i64));
			}
			else if (!strcasecmp(s->itype, "MAPVALUES")) {
				as_query_where(query, s->ibname,
						as_range(MAPVALUES, NUMERIC, beg->u.i64, end->u.i64));
			}
		}
		else if (type == AS_STRING) { // String Equality
			if (!strcasecmp(s->itype, "LIST")) {
				as_query_where(query, s->ibname,
						as_contains(LIST, STRING, beg->u.str));
			}
			else if (!strcasecmp(s->itype, "MAPKEYS")) {
				as_query_where(query, s->ibname,
						as_contains(MAPKEYS, STRING, beg->u.str));
			}
			else if (!strcasecmp(s->itype, "MAPVALUES")) {
				as_query_where(query, s->ibname,
						as_contains(MAPVALUES, STRING, beg->u.str));
			}
		}
		else if (type == AS_GEOJSON) { // Geospatial Lookup
			if (!strcasecmp(s->itype, "LIST")) {
				as_query_where(query, s->ibname,
						as_range(LIST, GEO2DSPHERE, beg->u.str, 0));
			}
			else if (!strcasecmp(s->itype, "MAPKEYS")) {
				as_query_where(query, s->ibname,
						as_range(MAPKEYS, GEO2DSPHERE, beg->u.str, 0));
			}
			else if (!strcasecmp(s->itype, "MAPVALUES")) {
				as_query_where(query, s->ibname,
						as_range(MAPVALUES, GEO2DSPHERE, beg->u.str, 0));
			}
		}
		else {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
									   "Error: Unknown query data type: %d",
									   type);
		}
	}
	else {
		if (beg->type == AS_INTEGER) { // Integer Range
			as_query_where(query, s->ibname,
					as_integer_range(beg->u.i64, end->u.i64));
		}
		else if (beg->type == AS_STRING) { // String Equality
			as_query_where(query, s->ibname, as_string_equals(beg->u.str));
		}
		else if (beg->type == AS_GEOJSON) {
			if (s->where.qtype == ASQL_QUERY_TYPE_WITHIN) {
				as_query_where(query, s->ibname,
						as_geo_within(beg->u.str));
			}
			else if (s->where.qtype == ASQL_QUERY_TYPE_CONTAINS) {
				as_query_where(query, s->ibname,
						as_geo_contains(beg->u.str));
			}
			else {
				return as_error_update(err, AEROSPIKE_ERR_CLIENT,
									"Error: Unknown GeoJSON query type: %d",
									s->where.qtype);
			}
		} else {
			return as_error_update(err, AEROSPIKE_ERR_CLIENT,
								"Error: Unknown query data type: %d", type);
		}
	}
	return 0;
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
		populate_where(&query, s, &err);
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
		aerospike_query_foreach(g_aerospike, &err, &query_policy, &query,
		                        g_renderer->render, rview);
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
		populate_where(&query, s, &err);
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
query_renderer(const as_val* val, void* udata)
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
