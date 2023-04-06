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
// Includes.
//

#include <aerospike/as_error.h>
#include <aerospike/as_node.h>

//==========================================================
// Typedefs & Constants.
//


//=========================================================
// Public API.
//

bool bins_res_parser(as_vector* result, const as_node* node, const char* req, const char* res);
bool udf_get_res_parser(as_vector* result, const as_node* node, const char* req, const char* res);
bool list_udf_parser(as_vector* result, const as_node* node, const char* req, const char* res);
bool list_res_parser(as_vector* result, const as_node* node, const char* req, const char* res);
char* info_res_split(const char* res);