/*
 * Copyright 2026 Aerospike, Inc.
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

#include <stdbool.h>
#include <stdint.h>

#include <aerospike/as_error.h>
#include <aerospike/as_key.h>


//=========================================================
// Public API.
//

bool asql_digest_from_hex(as_error* err, as_digest_value dig, const char* in);
bool asql_digest_from_b64(as_error* err, as_digest_value dig, const char* in);
char* asql_b64_decode_str(const char* in, uint32_t* out_size);
