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

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/as_val.h>

#include "json.h"
#include "json_renderer.h"
#include "renderer.h"
#include "asql.h"


//==========================================================
// Typedefs & constants.
//


typedef struct json_s {
	uint32_t entries;
	uint64_t start_ms;
	as_node* node;  //If node is null it is 
	pthread_mutex_t l;
	uint8_t indent;
} json;



//=========================================================
// Globals.
//

char* SPACE = "                ";
uint8_t BUF_SPACE_LEN = 16;
#define  TABSIZE 4

static as_node cluster_node;
as_node* CLUSTER = &cluster_node;

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
static void render_nodeid(json* self);
static char* spaces(uint8_t indent);


//=========================================================
// Function Table.
//

renderer json_renderer = {
	.view_new = view_new,
	.view_destroy = view_destroy,
	.render = render,
	.render_error = render_error,
	.render_ok = render_ok,
	.view_set_node = view_set_node,
	.view_set_cols = view_set_cols
};

//=========================================================
// Function Table.
//

#define BEGIN_MAIN(self) fprintf(stdout, "\n["); (self)->indent++;
#define END_MAIN(self) (self)->indent--; fprintf(stdout, "\n]\n\n");
#define BEGIN_NODE(self) fprintf(stdout, "\n%s[", spaces((self)->indent)); (self)->indent++;
#define END_NODE(self) (self)->indent--;  fprintf(stdout, "\n%s]", spaces((self)->indent));


//=========================================================
// Local Helpers.
//

static void*
view_new(const as_node* node)
{
	json* self = (json*)malloc(sizeof(json));
	if (! self) {
		return NULL;
	}
	memset(self, 0, sizeof(json));
	self->start_ms = cf_getms();
	pthread_mutex_init(&self->l, 0);

	BEGIN_MAIN(self);

	view_set_node(node, (void*) self);

	return self;
}

static void
view_destroy(void* view)
{
	json* self = (json*)view;
	if (self) {

		if (self->node) {
			END_NODE(self);
		}

		END_MAIN(self);

		free(self);
	}
}

static void
view_set_node(const as_node* node, void* view)
{
	json* self = (json*)view;
	if (! self) {
		return;
	}

	if (self->node) { 
		END_NODE(self);
		fprintf(stdout, ",");
	}


	self->entries = 0;
	self->node = (as_node*)node;

	if (self->node) {
		BEGIN_NODE(self);
	}
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
	json* self = (json*)view;

	pthread_mutex_lock(&self->l);

	if (! val) {
		if (self->node && self->node != CLUSTER) {
			render_nodeid(self);
		}
		pthread_mutex_unlock(&self->l);
		return true;
	}

	if (self->entries > 0) {
		fprintf(stdout, ",");
	}

	as_json_print_as_val(val, self->node ? self->indent + 2 : self->indent + 1, 
			g_config->record_print_metadata,
			g_config->no_bins);

	self->entries++;
	pthread_mutex_unlock(&self->l);
	return true;
}

static void
render_status(as_val* val, void* self)
{
	json* view = self ? (json*) self : view_new(NULL);

	// Status are always at cluster level.
	view_set_node((as_node*)CLUSTER, view);

	g_renderer->render(val, view);
	g_renderer->render((as_val*) NULL, view);

	if (! self) {
		view_destroy(view);
	}
}

static void
render_error(const int32_t code, const char* msg, void* self)
{
	as_hashmap map;

	as_hashmap_init(&map, 2);

	as_string status;
	as_integer status_code;
	as_string_init(&status, "Status", false);
	as_integer_init(&status_code, code);
	as_hashmap_set(&map, (as_val*)&status, (as_val*)&status_code);

	as_string message;
	as_string msg_value;
	if (msg && strlen(msg) != 0) {
		as_string_init(&message, "Message", false);
		as_string_init(&msg_value, (char *)msg, false);
		as_hashmap_set(&map, (as_val*)&message, (as_val*)&msg_value);
	}
	
	render_status((as_val*)&map, self);

	as_hashmap_destroy(&map);

	return;
}

static void
render_ok(const char* msg, void* self)
{
	as_hashmap map;

	as_hashmap_init(&map, 2);

	as_string status;
	as_integer status_code;
	as_string_init(&status, "Status", false);
	as_integer_init(&status_code, 0);
	as_hashmap_set(&map, (as_val*)&status, (as_val*)&status_code);

	as_string message;
	as_string msg_value;
	if (msg && strlen(msg) != 0) {
		as_string_init(&message, "Message", false);
		as_string_init(&msg_value, (char *)msg, false);
		as_hashmap_set(&map, (as_val*)&message, (as_val*)&msg_value);
	}
	
	render_status((as_val*)&map, self);

	as_hashmap_destroy(&map);

	return;
}

static void
render_nodeid(json* self)
{
	fprintf(stdout, ",\n%s", spaces(self->indent));

	fprintf(stdout, "{\n%s\"node\": \"%s\"", spaces(self->indent + 1),
			as_node_get_address_string(self->node));

	fprintf(stdout, "\n%s}", spaces(self->indent));
}

static char*
spaces(uint8_t indent) 
{
	return &SPACE[BUF_SPACE_LEN - (indent * TABSIZE)];
}
