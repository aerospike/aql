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

#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>

#include <aerospike/as_val.h>
#include <aerospike/as_rec.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_string.h>
#include <aerospike/as_record.h>
#include <aerospike/as_key.h>
#include <aerospike/as_pair.h>
#include <citrusleaf/cf_b64.h>

#include "raw_renderer.h"
#include "renderer.h"
#include "asql.h"


//==========================================================
// Typedefs & constants.
//

typedef struct {
	uint64_t start;
	uint32_t rows_total;
	as_node* node;
	pthread_mutex_t l;
} raw;


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

static bool each_bin(const char* name, const as_val* val, void* udata);
static bool each_map_entry(const as_val* key, const as_val* val, void* udata);
static bool each_list_entry(as_val* val, void* udata);


//=========================================================
// Function Table.
//

renderer raw_renderer = {
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
	raw* self = (raw*)malloc(sizeof(raw));
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
	raw* self = (raw*)view;
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
	raw* self = (raw*)view;
	if (!self) {
		return false;
	}

	if (!val) {

		// if NULL, then we have exhausted the dataset, so we
		// will render the footer
		pthread_mutex_lock(&self->l);

		uint32_t rows = self->rows_total;
		uint64_t start = self->start;
		uint64_t stop = cf_getms();
		uint64_t diff = stop - start;
		long double secs = (long double)diff / 1000;

		// print summary
		if (self->node && self->node != CLUSTER) {
			fprintf(stdout, "\n[%s] %d %s in set (%.3LF %s)\n\n",
			        as_node_get_address_string(self->node), rows,
			        rows == 1 ? "row": "rows", secs,
			        diff == 1000 ? "sec": "secs");
		}
		else {
			fprintf(stdout, "\n%d %s in set (%.3LF %s)\n\n", rows,
			        rows == 1 ? "row": "rows", secs,
			        diff == 1000 ? "sec": "secs");

		}
		pthread_mutex_unlock(&self->l);
	}
	else {

		switch (as_val_type(val)) {
			case AS_REC: {
				as_rec* rec = as_rec_fromval(val);

				// If we find a key-string coming back for
				// the record, then we will add key as a new
				// column

				pthread_mutex_lock(&self->l);

				as_record* p_rec = as_record_fromval(val);

				if (p_rec->key.valuep) {
					each_bin(COL_NAME_PK, (as_val*)p_rec->key.valuep, self);
				}

				uint16_t nbins = as_rec_numbins(rec);
				if (nbins > 0) {

					fprintf(stdout, "*************************** %d. row ***************************\n",
							self->rows_total + 1);
					as_rec_foreach(rec, each_bin, self);

					if (g_config->record_print_metadata) {
						// need to notice single-record calls doesn't have digest, and print N/A
						if (rec_has_digest(p_rec)) {
							uint32_t digest_len = cf_b64_encoded_len(
							        sizeof(as_digest_value));
							char* digest64 = alloca(digest_len + 1);
							cf_b64_encode((uint8_t*)p_rec->key.digest.value,
							              sizeof(as_digest_value), digest64);
							digest64[digest_len] = 0;

							as_string digest;
							as_string_init(&digest, digest64, false);
							each_bin(COL_NAME_META_EDIGEST, (as_val*)&digest,
							         self);
							as_string_destroy(&digest);
						}

						// print setname if it exists
						if (p_rec->key.set[0]) {
							as_string setname;
							as_string_init(&setname, p_rec->key.set, false);
							each_bin(COL_NAME_SETNAME, (as_val*)&setname,
							         self);
							as_string_destroy(&setname);
						}

						as_integer temp_int;
						if ((int32_t)p_rec->ttl == -1) {
							// -1 is special and is treated as never expire
							as_integer_init(&temp_int, -1);
						}
						else {
							as_integer_init(&temp_int, p_rec->ttl);
						}
						each_bin(COL_NAME_META_TTL_NAME, (as_val*)&temp_int,
						         self);
						as_integer_init(&temp_int, p_rec->gen);
						each_bin(COL_NAME_META_GEN_NAME, (as_val*)&temp_int,
						         self);
					}

					self->rows_total++;
				} else {
					// In case of no_bin print digest
					// need to notice single-record calls doesn't have digest, and print N/A
					if (g_config->no_bins
							&& rec_has_digest(p_rec)) {

						as_bytes b;
						as_bytes_init_wrap(&b, p_rec->key.digest.value,
								CF_DIGEST_KEY_SZ, false);
						as_string digest;
						as_string_init(&digest,
								removespaces(as_val_tostring(&b)), false);
						each_bin(COL_NAME_META_DIGEST, (as_val*)&digest,
									self);
						as_string_destroy(&digest);
						self->rows_total++;
					}
				}



				pthread_mutex_unlock(&self->l);
			}
				break;

			case AS_MAP: {
				as_map* map = as_map_fromval(val);

				pthread_mutex_lock(&self->l);
				uint16_t size = as_map_size(map);
				if (size > 0) {

					fprintf(stdout, "*************************** %d. row ***************************\n",
							self->rows_total + 1);
					as_map_foreach(map, each_map_entry, self);
					self->rows_total++;
				}

				pthread_mutex_unlock(&self->l);
			}
				break;

			case AS_LIST: {
				as_list* list = as_list_fromval((as_val*)val);

				pthread_mutex_lock(&self->l);
				uint16_t size = as_list_size(list);
				if (size > 0) {
					fprintf(stdout, "*************************** %d. row ***************************\n",
							self->rows_total + 1);
					as_list_foreach(list, each_list_entry, self);
					self->rows_total++;
				}

				pthread_mutex_unlock(&self->l);
			}
				break;

			default: {
			}
		}
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

static bool
each_bin(const char* name, const as_val* val, void* udata)
{

	char* str = asql_val_str(val);
	fprintf(stdout, "%s: %s\n", name, str);
	free(str);
	return true;
}

static bool
each_map_entry(const as_val* key, const as_val* val, void* udata)
{
	return each_bin(as_string_get(as_string_fromval(key)), val, udata);
}

static bool
each_list_entry(as_val* val, void* udata)
{
	as_pair* pair = as_pair_fromval(val);
	return each_bin(as_string_get(as_string_fromval(pair->_1)), pair->_2, udata);
}


