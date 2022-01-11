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

#include <stdlib.h>

#include <aerospike/as_admin.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_cluster.h>
#include <aerospike/as_pair.h>
#include <aerospike/as_string.h>
#include <aerospike/as_string_builder.h>

#include <asql.h>
#include <asql_admin.h>

#include "renderer/table.h"


//=========================================================
// Forward Declarations.
//


static const char* privilege_code_tostring(as_privilege_code code);
static uint32_t roles_string_sz(as_user* user);
static void roles_string(as_user* user, char* output);
static int display_users(as_user** users, int size);
static as_status query_user(as_error* err, as_policy_admin* policy, const char* user_name);
static as_status query_users(as_error* err, as_policy_admin* policy);
static void privilege_string(as_privilege* priv, as_string_builder* sb);
static void privileges_string(as_role* role, as_string_builder* sb);
static int display_roles(as_role** roles, int size);
static as_status query_role(as_error* err, as_policy_admin* policy, const char* role_name);
static as_status query_roles(as_error* err, as_policy_admin* policy);


//=========================================================
// Public API.
//

admin_config*
aql_admin_config_create()
{
	admin_config* cfg = malloc(sizeof(admin_config));
	memset(cfg, 0, sizeof(admin_config));
	cfg->type = ADMIN_OP;
	return cfg;
}

int
aql_admin(asql_config* c, aconfig* ac)
{
	admin_config* cfg = (admin_config*)ac;
	as_error err;

	as_policy_admin policy;
	as_policy_admin_init(&policy);
	policy.timeout = c->base.timeout_ms;

	as_status status;

	bool is_query = false;

	switch (cfg->cmd) {
		case AQL_CREATE_USER:
			status = aerospike_create_user(g_aerospike, &err, &policy,
			                               cfg->user, cfg->password,
			                               cfg->list->list, cfg->list->size);
			break;

		case AQL_DROP_USER:
			status = aerospike_drop_user(g_aerospike, &err, &policy, cfg->user);
			break;

		case AQL_SET_PASSWORD: {
			char* user = g_aerospike->cluster->user;

			if (!user){
				// Security not enabled
				status = as_error_update(&err, AEROSPIKE_ERR_CLIENT, "Aerospike security not enabled");
			}
			else if (cfg->user == 0 || *(cfg->user) == 0
			        || (user && strcmp(user, cfg->user) == 0)) {
				// Change own password.
				status = aerospike_change_password(g_aerospike, &err, &policy,
				                                   cfg->user, cfg->password);
			}
			else {
				// Change other user's password by user admin.
				status = aerospike_set_password(g_aerospike, &err, &policy,
				                                cfg->user, cfg->password);
			}
			break;
		}

		case AQL_GRANT_ROLES:
			status = aerospike_grant_roles(g_aerospike, &err, &policy,
			                               cfg->user, cfg->list->list,
			                               cfg->list->size);
			break;

		case AQL_REVOKE_ROLES:
			status = aerospike_revoke_roles(g_aerospike, &err, &policy,
			                                cfg->user, cfg->list->list,
			                                cfg->list->size);
			break;

		case AQL_CREATE_ROLE:
			status = aerospike_create_role_whitelist(g_aerospike, &err, &policy,
			                               cfg->role, cfg->list->list,
			                               cfg->list->size, cfg->whitelist->list, cfg->whitelist->size);
			break;

		case AQL_DROP_ROLE:
			status = aerospike_drop_role(g_aerospike, &err, &policy, cfg->role);
			break;

		case AQL_GRANT_PRIVILEGES:
			status = aerospike_grant_privileges(g_aerospike, &err, &policy,
			                                    cfg->role, cfg->list->list,
			                                    cfg->list->size);
			break;

		case AQL_REVOKE_PRIVILEGES:
			status = aerospike_revoke_privileges(g_aerospike, &err, &policy,
			                                     cfg->role, cfg->list->list,
			                                     cfg->list->size);
			break;

		case AQL_SET_WHITELIST:
			status = aerospike_set_whitelist(g_aerospike, &err, &policy,
											 cfg->role, cfg->whitelist->list, cfg->whitelist->size);
			break;

		case AQL_SHOW_USER:
			status = query_user(&err, &policy, cfg->user);
			is_query = true;
			break;

		case AQL_SHOW_USERS:
			status = query_users(&err, &policy);
			is_query = true;
			break;

		case AQL_SHOW_ROLE:
			status = query_role(&err, &policy, cfg->role);
			is_query = true;
			break;

		case AQL_SHOW_ROLES:
			is_query = true;
			status = query_roles(&err, &policy);
			break;

		default:
			status = as_error_update(&err, AEROSPIKE_ERR_PARAM,
			                         "Invalid command: %d", cfg->cmd);
			break;
	}

	if (status == 0) {
		if (!is_query) {
			g_renderer->render_ok("", NULL);
		}
	}
	else if (err.message[0]) {
		g_renderer->render_error(status, err.message, NULL);
	}
	else {
		g_renderer->render_error(status, as_error_string(status), NULL);
	}
	return status;
}


//=========================================================
// Local Helpers.
//

static const char*
privilege_code_tostring(as_privilege_code code)
{
	switch (code) {
		case AS_PRIVILEGE_USER_ADMIN:
			return "user-admin";

		case AS_PRIVILEGE_SYS_ADMIN:
			return "sys-admin";

		case AS_PRIVILEGE_DATA_ADMIN:
			return "data-admin";

		case AS_PRIVILEGE_READ:
			return "read";

		case AS_PRIVILEGE_READ_WRITE:
			return "read-write";

		case AS_PRIVILEGE_READ_WRITE_UDF:
			return "read-write-udf";

		case AS_PRIVILEGE_WRITE:
			return "write";

		default:
			return "unknown";
	}
}

static uint32_t
roles_string_sz(as_user* user)
{
	uint32_t size = 0;
	for (uint32_t i = 0; i < user->roles_size; i++) {
		size += strlen(user->roles[i]) + 2;
	}
	return size;
}

static void
roles_string(as_user* user, char* output)
{
	char* p = output;

	for (uint32_t i = 0; i < user->roles_size; i++) {
		if (i > 0) {
			p = stpcpy(p, ", ");
		}
		p = stpcpy(p, user->roles[i]);
	}
	*p = 0;
}

static int
display_users(as_user** users, int size)
{
	as_arraylist list;
	as_arraylist_init(&list, size, sizeof(as_pair));
	as_pair user_pair;
	as_pair role_pair;
	void* rview = g_renderer->view_new(NULL);

	for (uint32_t i = 0; i < size; i++) {
		as_user* user = users[i];

		as_pair_init(&user_pair, (as_val*)as_string_new("user", false),
		             (as_val*)as_string_new(user->name, false));
		as_arraylist_append(&list, (as_val*)&user_pair);

		char roles_buf[roles_string_sz(user)];
		roles_string(user, roles_buf);
		as_pair_init(&role_pair, (as_val*)as_string_new("roles", false),
		             (as_val*)as_string_new(roles_buf, false));
		as_arraylist_append(&list, (as_val*)&role_pair);

		g_renderer->render((as_val*)&list, rview);
		as_arraylist_trim(&list, 0);
	}
	g_renderer->render((as_val*) NULL, rview);
	g_renderer->view_destroy(rview);
	as_arraylist_destroy(&list);
	return 0;
}

static as_status
query_user(as_error* err, as_policy_admin* policy, const char* user_name)
{
	as_user* user;
	as_status ret = aerospike_query_user(g_aerospike, err, policy, user_name,
	                                     &user);

	if (ret != 0) {
		return ret;
	}
	display_users(&user, 1);
	as_user_destroy(user);
	return ret;
}

static as_status
query_users(as_error* err, as_policy_admin* policy)
{
	as_user** users;
	int size;
	as_status ret = aerospike_query_users(g_aerospike, err, policy, &users,
	                                      &size);

	if (ret != 0) {
		return ret;
	}
	display_users(users, size);
	as_users_destroy(users, size);
	return ret;
}

static void
privilege_string(as_privilege* priv, as_string_builder* sb)
{
	as_string_builder_append(sb, privilege_code_tostring(priv->code));

	if (priv->ns[0]) {
		as_string_builder_append_char(sb, '.');
		as_string_builder_append(sb, priv->ns);

		if (priv->set[0]) {
			as_string_builder_append_char(sb, '.');
			as_string_builder_append(sb, priv->set);
		}
	}
}

static void
privileges_string(as_role* role, as_string_builder* sb)
{
	as_string_builder_reset(sb);

	for (int i = 0; i < role->privileges_size; i++) {
		as_privilege* priv = &role->privileges[i];
		if (i > 0) {
			as_string_builder_append(sb, ", ");
		}
		privilege_string(priv, sb);
	}
}

static void
whitelist_string(as_role* role, as_string_builder* sb)
{
	as_string_builder_reset(sb);

	for (int i = 0; i < role->whitelist_size; i++) {
		if (i > 0) {
			as_string_builder_append(sb, ", ");
		}
		as_string_builder_append(sb, role->whitelist[i]);
	}
}

static int
display_roles(as_role** roles, int size)
{
	as_string_builder sb1;
	as_string_builder_inita(&sb1, 1024, true);

	as_string_builder sb2;
	as_string_builder_inita(&sb2, 1024, false);

	as_arraylist list;
	as_arraylist_init(&list, 3, sizeof(as_pair));
	as_pair role_pair;
	as_pair priv_pair;
	as_pair wl_pair;
	void* rview = g_renderer->view_new(NULL);

	for (uint32_t i = 0; i < size; i++) {
		as_role* role = roles[i];

		as_pair_init(&role_pair, (as_val*)as_string_new("role", false),
		             (as_val*)as_string_new(role->name, false));
		as_arraylist_append(&list, (as_val*)&role_pair);

		privileges_string(role, &sb1);
		as_pair_init(&priv_pair, (as_val*)as_string_new("privileges", false),
		             (as_val*)as_string_new(sb1.data, false));
		as_arraylist_append(&list, (as_val*)&priv_pair);

		whitelist_string(role, &sb2);
		as_pair_init(&wl_pair, (as_val*)as_string_new("whitelist", false),
		             (as_val*)as_string_new(sb2.data, false));
		as_arraylist_append(&list, (as_val*)&wl_pair);

		g_renderer->render((as_val*)&list, rview);
		as_arraylist_trim(&list, 0);
	}

	as_string_builder_destroy(&sb1);
	g_renderer->render((as_val*) NULL, rview);
	g_renderer->view_destroy(rview);
	as_arraylist_destroy(&list);
	return 0;
}

static as_status
query_role(as_error* err, as_policy_admin* policy, const char* role_name)
{
	as_role* role;
	as_status ret = aerospike_query_role(g_aerospike, err, policy, role_name,
	                                     &role);

	if (ret != 0) {
		return ret;
	}
	display_roles(&role, 1);
	as_role_destroy(role);
	return ret;
}

static as_status
query_roles(as_error* err, as_policy_admin* policy)
{
	as_role** roles;
	int size;
	as_status ret = aerospike_query_roles(g_aerospike, err, policy, &roles,
	                                      &size);

	if (ret != 0) {
		return ret;
	}
	display_roles(roles, size);
	as_roles_destroy(roles, size);
	return ret;
}
