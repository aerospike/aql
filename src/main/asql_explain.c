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


#include <aerospike/aerospike_key.h>
#include <aerospike/as_aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_key.h>
#include <aerospike/as_rec.h>
#include <aerospike/as_stringmap.h>
#include <aerospike/as_shm_cluster.h>

#include <aerospike/as_partition.h>
#include <aerospike/as_cluster.h>

#include <aerospike/as_arraylist.h>
#include <aerospike/as_list.h>

#include <citrusleaf/cf_digest.h>

#include <renderer.h>
#include <json.h>
#include <asql.h>
#include <asql_key.h>


//==========================================================
// Forward Declarations.
//



//==========================================================
// Public API.
//

int
asql_key_select_explain(asql_config* c, pk_config* p, as_key* key,
                        as_error* err)
{
	as_hashmap m;
	as_hashmap_init(&m, 12);

	as_bytes b;
	as_bytes_init_wrap(&b, key->digest.value, CF_DIGEST_KEY_SZ, false);
	as_hashmap_set(&m, (as_val*)as_string_new_strdup("DIGEST"),
	        (as_val*)as_string_new(removespaces(as_val_tostring(&b)),
	                                true));

	if (p->u.udfpkg) {
		as_hashmap_set(&m, (as_val*)as_string_new_strdup("UDF"),
		               (as_val*)as_string_new_strdup("TRUE"));
	}
	else {
		as_hashmap_set(&m, (as_val*)as_string_new_strdup("UDF"),
		               (as_val*)as_string_new_strdup("FALSE"));
	}

	as_hashmap_set(&m, (as_val*)as_string_new_strdup("NAMESPACE"),
	               (as_val*)as_string_new(key->ns, false));
	as_hashmap_set(&m, (as_val*)as_string_new_strdup("SET"),
	               (as_val*)as_string_new(key->set, false));

	as_hashmap_set(&m, (as_val*)as_string_new_strdup("STATUS"),
	               (as_val*)as_string_new(err->message, false));

	uint32_t partition_id = as_partition_getid(key->digest.value, 4096);
	as_hashmap_set(&m, (as_val*)as_string_new_strdup("PARTITION"),
	               (as_val*)as_integer_new(partition_id));


	as_node *prole = NULL;
	as_node *master = NULL;

	if (g_aerospike->cluster->shm_info) {
		as_cluster_shm* cluster_shm = g_aerospike->cluster->shm_info->cluster_shm;
		as_partition_table_shm* pptable = as_shm_find_partition_table(cluster_shm, key->ns);

		if (!pptable) {
			g_renderer->render_error(AEROSPIKE_ERR_CLIENT, "Error getting partition table", NULL);
			as_hashmap_destroy(&m);
			return 1;
		}

		as_partition_shm* asp = &pptable->partitions[partition_id];
		uint32_t master_index = as_load_uint32(&asp->nodes[0]);
		uint32_t prole_index = as_load_uint32(&asp->nodes[1]);

		as_node** local_nodes = g_aerospike->cluster->shm_info->local_nodes;

		master = (as_node*)as_load_ptr(&local_nodes[master_index-1]);
		if (prole_index) {
			prole = (as_node*)as_load_ptr(&local_nodes[prole_index-1]);
		}

	} else {
		as_partition_tables* pptables = &g_aerospike->cluster->partition_tables;
		as_partition_table* pptable = as_partition_tables_get(pptables, key->ns);

		if (!pptable) {
			g_renderer->render_error(AEROSPIKE_ERR_CLIENT, "Error getting partition table", NULL);
			as_hashmap_destroy(&m);
			return 1;
		}

		as_partition *asp = &pptable->partitions[partition_id];
		master = (as_node *)as_load_ptr(&asp->nodes[0]);
		prole = (as_node *)as_load_ptr(&asp->nodes[1]);
	}

	as_hashmap_set(&m, (as_val *)as_string_new_strdup("MASTER NODE"),
				   (as_val *)as_string_new(master->name, false));
	if (prole) {
		as_hashmap_set(&m, (as_val *)as_string_new_strdup("REPLICA NODE"),
					   (as_val *)as_string_new(prole->name, false));
	}

	as_hashmap_set(&m, (as_val*)as_string_new_strdup("KEY_TYPE"),
	        (as_val*)as_string_new(
	                strdup(p->key.vt == ASQL_VALUE_TYPE_DIGEST ?
	                        "DIGEST":
	                        (p->key.vt == ASQL_VALUE_TYPE_EDIGEST ?
	                                "BASE64_DIGEST":
	                                (p->key.type == AS_STRING ?
	                                        "STRING":
	                                        (p->key.type == AS_INTEGER ?
	                                                "INTEGER": "UNKNOWN")))),
	                true));

	as_hashmap_set(&m, (as_val*)as_string_new_strdup("POLICY_KEY"),
	        (as_val*)as_string_new(
	                strdup((p->key.vt == ASQL_VALUE_TYPE_EDIGEST ||
							p->key.vt == ASQL_VALUE_TYPE_DIGEST) ?
	                        "AS_POLICY_KEY_DIGEST":
	                        (c->key_send ?
	                                "AS_POLICY_KEY_SEND":
	                                "AS_POLICY_KEY_DEFAULT")),
	                true));

	as_hashmap_set(&m, (as_val*)as_string_new_strdup("TIMEOUT"),
	               (as_val*)as_integer_new(c->base.timeout_ms));

	void* rview = g_renderer->view_new(CLUSTER);
	g_renderer->render((as_val*)&m, rview);
	g_renderer->render((as_val*) NULL, rview);

	if (err->code == AEROSPIKE_OK) {
		g_renderer->render_ok("", rview);
	}
	else {
		g_renderer->render_error(err->code, err->message, rview);
	}

	g_renderer->view_destroy(rview);
	as_hashmap_destroy(&m);
	return 0;
}
