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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>

#include <asql.h>


//=========================================================
// Globals.
//

uint64_t g_slap_tot_time;
uint64_t g_slap_tot_reqs;
uint64_t g_slap_iterations;
uint64_t g_slap_tot_success;

struct asql_cmd_file_desc;


//=========================================================
// Forward Declarations.
//

static uint64_t safe_delta_ms(uint64_t start_ms, uint64_t stop_ms);
static void init_slap(asql_config* c);
static void* run_th(void* o);
static void dump_slap_stats();
static void* parse_run_file_th(void* arg);

extern int run(void* o);
extern bool parse_and_run_file(struct asql_cmd_file_desc* des);


//=========================================================
// Public API.
//

void
asql_slap(asql_config* c, aconfig* ac)
{
	init_slap(c);
	pthread_t slaps[c->threads];
	asql_op op[c->threads];
	for (int j = 0; j < c->threads; j++) {
		op[j].c = c;
		op[j].ac = ac;
		op[j].backout = false;
		if (pthread_create(&slaps[j], 0, run_th, &op[j])) {
			fprintf(stderr, "Thread create failed !\n ");
		}
	}
	c->slap = false;
	int ret = 0;
	for (int j = 0; j < c->threads; j++) {
		pthread_join(slaps[j], (void*)&ret);
	}
	dump_slap_stats();

}

void
asql_slap_file(asql_config* c, struct asql_cmd_file_desc* des)
{
	pthread_t slaps[c->threads];
	for (int j = 0; j < c->threads; j++) {
		if (pthread_create(&slaps[j], 0, parse_run_file_th, &des)) {
			fprintf(stderr, "Thread create failed !\n ");
		}
	}
	c->slap = false;
	int ret = 0;
	for (int j = 0; j < c->threads; j++) {
		pthread_join(slaps[j], (void*)&ret);
	}
	dump_slap_stats();
}


//=========================================================
// Local Helpers.
//

static uint64_t
safe_delta_ms(uint64_t start_ms, uint64_t stop_ms)
{
	return start_ms > stop_ms ? 0: stop_ms - start_ms;
}


static void
init_slap(asql_config* c)
{
	g_slap_tot_time = g_slap_tot_reqs = 0;
	g_slap_iterations = c->iteration;
}

static void*
run_th(void* o)
{
	sleep(1); // Let asql spawn all the threads
	uint64_t itr = g_slap_iterations;
	uint64_t tdiff = 0;
	uint64_t reqs = 0;
	uint64_t success = 0;
	int ret = 0;
	for (int i = 0; i < itr; i++) {
		uint64_t start_time = cf_getms();
		if (run(o) == 0) {
			// to be checked for other functions that call run()
			success++;
		}
		uint64_t stop_time = cf_getms();
		uint64_t diff = safe_delta_ms(start_time, stop_time);
		tdiff += diff;
		reqs++;
	}
	g_slap_tot_time += tdiff;
	g_slap_tot_reqs += reqs;
	g_slap_tot_success += success;
	pthread_exit((void*)&ret);
}

static void
dump_slap_stats()
{
	fprintf(stderr, "Total requests:\t\t\t\t%" PRIu64 "\n", g_slap_tot_reqs);
	fprintf(stderr, "Total Success: \t\t\t\t%" PRIu64 "\n", g_slap_tot_success);
	fprintf(stderr, "Total time spent(ms):\t\t\t%" PRIu64 "\n",
	        g_slap_tot_time);
	fprintf(stderr, "Average time per req(ms):\t\t%f\n",
	        (double)(g_slap_tot_time / g_slap_tot_reqs));
}

static void*
parse_run_file_th(void* arg)
{
	sleep(1); // Let asql spawn all the threads
	int ret = 0;
	struct asql_cmd_file_desc* des = (struct asql_cmd_file_desc*)arg;
	ret = parse_and_run_file(des);
	pthread_exit((void*)&ret);
}
