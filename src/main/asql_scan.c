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
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_scan.h>

#include <aerospike/as_arraylist.h>
#include <aerospike/as_list.h>

#include <renderer.h>
#include <asql.h>
#include <asql_scan.h>

//=========================================================
// Forward Declarations.
//

extern int asql_query_aggregate(asql_config* c, scan_config* s);

static int scan_select(asql_config* c, scan_config* s);
static int scan_execute(asql_config* c, scan_config* s);


//=========================================================
// Public API.
//

int
asql_scan(asql_config* c, aconfig* ac)
{
	scan_config* s = (scan_config*)ac;

	switch (s->optype) {
		case ASQL_OP_SELECT:
			return scan_select(c, s);
		case ASQL_OP_EXECUTE:
			return scan_execute(c, s);
		case ASQL_OP_AGGREGATE:
			return asql_query_aggregate(c, s);
		default:
			return 0;
	}
}


//=========================================================
// Local Helpers.
//


static int
scan_select(asql_config* c, scan_config* s)
{
	as_error err;
	as_error_init(&err);

	as_policy_scan scan_policy;
	as_policy_scan_init(&scan_policy);
	scan_policy.base.total_timeout = c->base.timeout_ms;
	if (c->base.socket_timeout_ms > -1) {
		// set if non-default value
		scan_policy.base.socket_timeout = c->base.socket_timeout_ms;
	}
	scan_policy.durable_delete = c->durable_delete;
	scan_policy.records_per_second = (uint32_t)c->scan_records_per_second;

	if (s->limit) {
		scan_policy.max_records = s->limit->u.i64;
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

	as_scan scan;
	as_scan_init(&scan, s->ns, s->set);
	scan.no_bins = c->no_bins;
	bool select_all = false;

	if (!s->s.bnames) {
		// select all bins
		select_all = true;
	}
	else {
		// select specific bins
		select_all = false;
		as_scan_select_inita(&scan, s->s.bnames->size);
		for (int i = 0; i < s->s.bnames->size; i++) {

			char* bname = as_vector_get_ptr(s->s.bnames, i);

			if (strlen(bname) > AS_BIN_NAME_MAX_LEN) {
				as_error_update(&err, AEROSPIKE_ERR_CLIENT,
				                "Bin name is too long: '%s'", bname);
				break;
			}
			as_scan_select(&scan, bname);
		}
	}

	void* rview = g_renderer->view_new(CLUSTER);

	if (err.code == AEROSPIKE_OK) {
		if (! select_all) {
			g_renderer->view_set_cols(s->s.bnames, rview);
		}
		aerospike_scan_foreach(g_aerospike, &err, &scan_policy, &scan,
		                       g_renderer->render, rview);
	}

	if (err.code == AEROSPIKE_OK) {
		g_renderer->render_ok("", rview);
	} else {
		g_renderer->render_error(err.code, err.message, rview);
	}

	g_renderer->view_destroy(rview);
	as_scan_destroy(&scan);

	return 0;
}

static int
scan_execute(asql_config* c, scan_config* s)
{
	as_error err;
	as_error_init(&err);

	as_policy_scan scan_policy;
	as_policy_scan_init(&scan_policy);
	scan_policy.base.total_timeout = c->base.timeout_ms;
	if (c->base.socket_timeout_ms > -1) {
		// set if non-default value
		scan_policy.base.socket_timeout = c->base.socket_timeout_ms;
	}
	scan_policy.records_per_second = (uint32_t)c->scan_records_per_second;

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

	as_scan scan;
	as_scan_init(&scan, s->ns, s->set);

	as_arraylist arglist;

	if (s->u.params) {
		as_arraylist_inita(&arglist, s->u.params->size);

		asql_set_args(&err, s->u.params, &arglist);

		if (err.code != AEROSPIKE_OK) {
			g_renderer->render_error(err.code, err.message, NULL);
			return 1;
		}
	}
	else {
		as_arraylist_inita(&arglist, 0);
	}

	// NB: scan object consumes arglist no need for
	// destroy on arglist
	as_scan_apply_each(&scan, s->u.udfpkg, s->u.udfname,
	                   (as_list*)&arglist);

	uint64_t scanid = 0;
	aerospike_scan_background(g_aerospike, &err, &scan_policy, &scan, &scanid);

	if (err.code == AEROSPIKE_OK) {
		char ok_msg[1024];
		snprintf(ok_msg, 1023, "Scan job (%"PRIu64") created.", scanid);
		g_renderer->render_ok(ok_msg, NULL);
	}
	else {
		g_renderer->render_error(err.code, err.message, NULL);
	}

	as_scan_destroy(&scan);

	return 0;
}
