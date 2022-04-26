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

#include <aerospike/as_bin.h>
#include <aerospike/as_record.h>
#include <aerospike/as_types.h>

#include <asql_value.h>


//==========================================================
// Public API.
//

int as_json_print(const as_val*);
as_val* as_json_arg(char*, asql_value_type_t);
void as_json_print_as_val(const as_val* val, int indent, bool metadata, bool no_bins);
