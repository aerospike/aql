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

#pragma once

//==========================================================
// Typedefs & constants.
//

typedef enum {
	AQL_CREATE_USER,
	AQL_DROP_USER,
	AQL_SET_PASSWORD,
	AQL_GRANT_ROLES,
	AQL_REVOKE_ROLES,
	AQL_CREATE_ROLE,
	AQL_DROP_ROLE,
	AQL_GRANT_PRIVILEGES,
	AQL_REVOKE_PRIVILEGES,
	AQL_SET_WHITELIST,
	AQL_SHOW_USER,
	AQL_SHOW_USERS,
	AQL_SHOW_ROLE,
	AQL_SHOW_ROLES
} aql_admin_command;

typedef struct admin_config {
	atype type;
	asql_optype optype;

	aql_admin_command cmd;

	asql_name user;
	asql_name password;
	asql_name role;

	as_vector* whitelist;
	as_vector* list;
} admin_config;

//==========================================================
// Public API.
//

int aql_admin(asql_config* c, aconfig* ac);

admin_config* aql_admin_config_create();
void aql_admin_config_destroy(aconfig* ac);
