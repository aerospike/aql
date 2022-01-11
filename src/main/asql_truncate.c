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
#include <asql_truncate.h>

//=========================================================
// Public API.
//

int
asql_truncate(asql_config* c, aconfig* ac)
{
	as_error err;

	truncate_config* tc = (truncate_config*)ac;

	as_policy_info info_policy;
	as_policy_info_init(&info_policy);
	info_policy.timeout = c->base.timeout_ms;

	// Execute truncate command in the database cluster.
	as_status status = aerospike_truncate(g_aerospike, &err, &info_policy, tc->ns, tc->set, tc->lut);

	int rv = 0;
	if (status == AEROSPIKE_OK) {
		g_renderer->render_ok("", NULL);
	}
	else {
		rv = -1;
		g_renderer->render_error(err.code, err.message, NULL);
	}

	return rv;
}
