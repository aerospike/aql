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

#include <aerospike/as_val.h>
#include <aerospike/as_node.h>
#include <aerospike/as_record.h>

#include <citrusleaf/cf_digest.h>

//==========================================================
// Typedefs & constants.
//

#define COL_NAME_PK				"PK"
#define COL_NAME_SETNAME		"{set}"
#define COL_NAME_META_DIGEST	"{digest}"
#define COL_NAME_META_EDIGEST	"{edigest}"
#define COL_NAME_META_TTL_NAME	"{ttl}"
#define COL_NAME_META_GEN_NAME	"{gen}"

typedef struct {
	void* (* view_new)(const as_node* node);
	void (* view_destroy)(void* view);
	bool (* render)(const as_val* val, void* view);
	void (* render_error)(const int32_t code, const char* msg, void* view);
	void (* render_ok)(const char* msg, void* view);
	void (* view_set_cols)(as_vector* bnames, void* view);
	void (* view_set_node)(const as_node* node, void* view);
} renderer;

//=========================================================
// Globals.
//

extern renderer* g_renderer;
extern as_node* CLUSTER;

static inline bool
rec_has_digest(as_record *p_rec)
{
	for (int d = 0; d < sizeof(as_digest_value); d++) {
		if (p_rec->key.digest.value[d] != 0) {
			return true;
		}
	}
	return false;
}

static inline char*
removespaces(char* input)
{
	char* src = input;
	char* dst = input;

	while (*src != 0) {

		if (*src == ' ') {
			src++;
			continue;
		}
		*dst++ = *src++;
	}
	*dst = 0;
	return input;
}

static inline void
print_rec(as_record* rec, as_vector* bnames)
{
	void* rview = g_renderer->view_new(CLUSTER);
	if (bnames) {
		g_renderer->view_set_cols(bnames, rview);
	}
	g_renderer->render((as_val*)rec, rview);
	g_renderer->render((as_val*)NULL, rview);
	g_renderer->render_ok("", rview);
	g_renderer->view_destroy(rview);
}
