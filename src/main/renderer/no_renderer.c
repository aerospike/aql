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

#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

#include <aerospike/as_val.h>

#include "no_renderer.h"
#include "renderer.h"
#include "asql.h"


//==========================================================
// Typedefs & constants.
//

typedef struct mute_s {
	uint64_t start;
	uint32_t rows_total;
	as_node* node;

	pthread_mutex_t l;
} mute;


//=========================================================
// Forward Declarations.
//

extern uint64_t cf_getms();

static void* view_new(const as_node* node);
static void view_destroy(void* self);
static void view_set_node(const as_node* node, void* view);
static void view_set_cols(as_vector* bnames, void* view);
static bool render(const as_val* val, void* view);
static void render_error(const int32_t code, const char* msg, void* view);
static void render_ok(const char* msg, void* view);


//=========================================================
// Function Table.
//

renderer no_renderer = {
	.view_new = view_new,
	.view_destroy = view_destroy,
	.render = render,
	.render_error = render_error,
	.render_ok = render_ok,
	.view_set_node = view_set_node,
	.view_set_cols = view_set_cols
};


//=========================================================
// Local Helpers.
//

static void*
view_new(const as_node* node)
{
	mute* self = (mute*)malloc(sizeof(mute));
	if (!self) {
		return NULL;
	}

	self->start = cf_getms();
	self->rows_total = 0;
	self->node = (as_node*)node;
	pthread_mutex_init(&self->l, 0);

	return self;
}

static void
view_destroy(void* self)
{
	if (self) {
		free(self);
	}
}

static void
view_set_node(const as_node* node, void* view)
{
	mute* self = (mute*)view;
	self->rows_total = 0;
	self->node = (as_node*)node;
}

static void
view_set_cols(as_vector* bnames, void* view)
{
	// No-Op
	return;
}


static bool
render(const as_val* val, void* view)
{
	mute* self = (mute*)view;
	if (!self) {
		return false;
	}

	pthread_mutex_lock(&self->l);
	if (!val) {

		// if NULL, then we have exhausted the dataset, so we
		// will render the footer

		uint32_t rows = self->rows_total;
		uint64_t start = self->start;
		uint64_t stop = cf_getms();
		uint64_t diff = stop - start;
		long double secs = (long double)diff / 1000;

		// print summary
		if (self->node && self->node != CLUSTER) {
			fprintf(stdout, "[%s] %d %s in set (%.3LF %s)\n\n",
			        as_node_get_address_string(self->node), rows,
			        rows == 1 ? "row": "rows", secs,
			        diff == 1000 ? "sec": "secs");
		}
		else {
			fprintf(stdout, "%d %s in set (%.3LF %s)\n\n", rows,
			        rows == 1 ? "row": "rows", secs,
			        diff == 1000 ? "sec": "secs");

		}
	}
	else {
		self->rows_total++;
	}

	pthread_mutex_unlock(&self->l);
	return true;
}

static void
render_error(const int32_t code, const char* msg, void* view)
{
	if (msg && strlen(msg) != 0) {
		fprintf(stdout, "Error: (%d) %s\n\n", code, msg);
	}
	else {
		fprintf(stdout, "Error: (%d)\n\n", code);
	}
}

static void
render_ok(const char* msg, void* view)
{
	if (msg && strlen(msg) != 0) {
		fprintf(stdout, "OK, %s\n\n", msg);
	}
	else {
		fprintf(stdout, "OK\n\n");
	}
}
