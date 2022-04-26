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

#include <errno.h>
#include <stdlib.h>

#include <aerospike/as_log_macros.h>

#include <asql.h>
#include <asql_admin.h>
#include <asql_info.h>
#include <asql_key.h>
#include <asql_parser.h>
#include <asql_print.h>
#include <asql_query.h>
#include <asql_scan.h>
#include <asql_truncate.h>


//==========================================================
// Typedefs & constants.
//

typedef int (* op_fn)(asql_config* c, aconfig* ac);
typedef aconfig* (* parse_fn)(tokenizer* tknzr);
typedef void (* destroy_fn)(aconfig* ac);

typedef struct asql_cmd_file_desc {
	FILE* fp;
	asql_config* c;
} asql_cmd_file_desc;

typedef struct {
	char* cmd;
	asql_optype optype;
	parse_fn fn;
} parse_entry;


//=========================================================
// Forward Declarations.
//

static aconfig* parse(char* cmd);

static int runfile(asql_config* c, aconfig* ac);

static void destroy_select_param(select_param* s);
static void destroy_insert_param(insert_param* i);
static void destroy_udf_param(udf_param* u);
static void destroy_where(asql_where* w);
static void destroy_pkconfig(aconfig* ac);
static void destroy_skconfig(aconfig* ac);
static void destroy_infoconfig(aconfig* ac);
static void destroy_scanconfig(aconfig* ac);
static void destroy_runfileconfig(aconfig* ac);
static void destroy_admconfig(aconfig* ac);
static void destroy_truncateconfig(aconfig* ac);


//=========================================================
// Function table.
//

const op_fn op_map[OP_MAX] = {
	asql_query,
	asql_key,
	asql_info,
	asql_scan,
	runfile,
	aql_admin,
	asql_truncate,
};

const parse_entry parse_table[ASQL_OP_MAX] = {
	{ "EXPLAIN", ASQL_OP_EXPLAIN, aql_parse_explain },
	{ "INSERT", ASQL_OP_INSERT, aql_parse_insert },
	{ "DELETE", ASQL_OP_DELETE, aql_parse_delete },
	{ "TRUNCATE", ASQL_OP_TRUNCATE, aql_parse_truncate },
	{ "EXECUTE", ASQL_OP_EXECUTE, aql_parse_execute },

	{ "SELECT", ASQL_OP_SELECT, aql_parse_select },
	{ "AGGREGATE", ASQL_OP_AGGREGATE, aql_parse_aggregate },

	{ "CREATE", ASQL_OP_CREATE, aql_parse_create },
	{ "DROP", ASQL_OP_DROP, aql_parse_drop },
	{ "GRANT", ASQL_OP_GRANT, aql_parse_grant },
	{ "REVOKE", ASQL_OP_REMOVE, aql_parse_revoke },
	{ "REGISTER", ASQL_OP_REGISTER, aql_parse_registerudf },
	{ "REMOVE", ASQL_OP_REMOVE, aql_parse_removeudf },

	{ "SHOW", ASQL_OP_SHOW, aql_parse_show },
	{ "DESC", ASQL_OP_DESC, aql_parse_desc },
	{ "STAT", ASQL_OP_STAT, aql_parse_stat },

	{ "KILL_QUERY", ASQL_OP_KILL_Q, aql_parse_killquery },
	{ "KILL_SCAN", ASQL_OP_KILL_S, aql_parse_killscan },

	{ "RUN", ASQL_OP_RUN, aql_parse_run },
	{ "ASINFO", ASQL_OP_ASINFO, aql_parse_asinfo },

	{ "SET", ASQL_OP_SET, aql_parserun_set },
	{ "GET", ASQL_OP_GET, aql_parserun_get },
	{ "RESET", ASQL_OP_RESET, aql_parserun_reset },
	{ "PRINT", ASQL_OP_PRINT, aql_parserun_print },
	{ "SYSTEM", ASQL_OP_SYSTEM, aql_parserun_system }
};

const destroy_fn destroy_table[OP_MAX] = {
	destroy_skconfig,
	destroy_pkconfig,
	destroy_infoconfig,
	destroy_scanconfig,
	destroy_runfileconfig,
	destroy_admconfig,
	destroy_truncateconfig,
};


//=========================================================
// Public API.
//

void
destroy_vector(as_vector* list, bool is_name)
{
	for (uint32_t i = 0; i < list->size; i++) {
		if (is_name) {
			free(as_vector_get_ptr(list, i));
		}
		else {
			asql_free_value(as_vector_get(list, i));
		}
	}
}

int
destroy_aconfig(aconfig* ac)
{
	if (!ac) {
		return -1;
	}

	if (destroy_table[ac->type]) {
		destroy_table[ac->type](ac);
	}
	return 0;
}
	

int
run(void* o)
{
	asql_config* c = (asql_config*)((asql_op*)o)->c;
	aconfig* ac = (aconfig*)((asql_op*)o)->ac;
	if (op_map[ac->type]) {
		return op_map[ac->type](c, ac);
	}
	return 0;
}

// Get sub command with colon delimt
bool
parse_and_run_colon_delim(asql_config* c, char* cmd)
{
	char* subcmd = cmd;
	bool in_dquote = false;
	bool in_squote = false;

	while (true) {
		subcmd = cmd;
		while (true) {
			
			if (*cmd == '\0') {
				break;
			}
			
			// ignore ; in quotes
			if ((*cmd == ';') && ! in_dquote && ! in_squote) {
				break;
			}

			if (*cmd == '"') {
				in_dquote = ! in_dquote;
			}

			if (*cmd == '\'') {
				in_squote = ! in_squote;
			}
			
			cmd++;
		}

		if (*cmd == '\0') {
			if (!parse_and_run(c, subcmd)) {
				return false;
			}
			break;
		}

		*cmd = '\0';
		if (!parse_and_run(c, subcmd)) {
			return false;
		}
		cmd++;
	}

	return true;
}

bool
parse_and_run(asql_config* c, char* cmd)
{
	// ignore leading spaces.
	while (*cmd == ' ') {
		cmd++;
	}

	if (!strcasecmp(cmd, "EXIT") || !strcasecmp(cmd, "QUIT")
			|| !strcasecmp(cmd, "Q")) {
		return false;
	}

	if (!strncasecmp(cmd, "HELP", 4)) {
		print_help(cmd, false);
		return true;
	}

	aconfig* ac = parse(cmd);
	if (!ac) {
		return true;
	}

	asql_op op = { .c = c, .ac = ac, .backout = false, };
	run((void*)&op);

	destroy_aconfig(ac);
	return true;
}

bool
parse_and_run_file(asql_cmd_file_desc* des)
{
	char* cmd = NULL;
	des->c->base.echo = true;

	while (1) {

		size_t cmd_sz;
		int ret = (int)getline(&cmd, &cmd_sz, des->fp);

		if (ret == -1) {
			break;
		}

		if (!cmd)
			continue;

		int clen = (int)strlen(cmd);
		if (clen < 2) {
			free(cmd);
			cmd = NULL;
			continue;
		}

		cmd[clen - 1] = '\0'; // NULL the newline char

		if (!parse_and_run_colon_delim(des->c, cmd)) {
			break;
		}

		free(cmd);
		cmd = NULL;
	}

	if (cmd) {
		free(cmd);
	}
	return true;
}


//=========================================================
// Local Helper.
//

static int
runfile(asql_config* c, aconfig* ac)
{
	char* fname = ((runfile_config*)ac)->fname;
	FILE* fp = fopen(fname, "r");
	if (!fp) {
		fprintf(stderr, "Error: cannot open file %s : %s\n", fname,
				strerror(errno));
		return -1;
	}

	asql_cmd_file_desc des = {
		.fp = fp,
		.c = c
	};

	parse_and_run_file(&des);

	fclose(fp);
	return 0;
}

static aconfig*
parse(char* cmd)
{
	asql_config* c = g_config;
	tokenizer tknzr;
	init_tokenizer(&tknzr, cmd);
	char* ftok = tknzr.tok;
	aconfig* ac = NULL;

	if (!ftok) {
		goto End;
	}

	if (c->base.echo == true) {
		fprintf(stdout, "%s\n", tknzr.ocmd);
	}

	uint8_t i = 0;
	for (i = 0; i < ASQL_OP_MAX; i++) {
		if (!strcasecmp(ftok, parse_table[i].cmd)) {
			ac = parse_table[i].fn(&tknzr);
			if (ac) {
				ac->optype = parse_table[i].optype;
			}
			break;
		}
	}

	if (i == ASQL_OP_MAX) {
		fprintf(stdout, "\nERROR: 404: COMMAND NOT FOUND : %s\n", ftok);
	}

End:
	destroy_tokenizer(&tknzr);
	return ac;
}

// DESTRUCTORS
//

static void
destroy_select_param(select_param* s)
{
	if (s->bnames) {
		destroy_vector(s->bnames, true);
		as_vector_destroy(s->bnames);
	}
}

static void
destroy_insert_param(insert_param* i)
{
	if (i->bnames) {
		destroy_vector(i->bnames, true);
		as_vector_destroy(i->bnames);
	}
	if (i->values) {
		destroy_vector(i->values, false);
		as_vector_destroy(i->values);
	}
}

static void
destroy_udf_param(udf_param* u)
{
	if (u->udfpkg) free(u->udfpkg);
	if (u->udfname) free(u->udfname);

	if (u->params) {
		destroy_vector(u->params, false);
		as_vector_destroy(u->params);
	}
}

static void
destroy_where(asql_where* w)
{
	if (w->qtype == ASQL_QUERY_TYPE_EQUALITY) {
		asql_free_value(&w->beg);
	}
	else {
		asql_free_value(&w->beg);
		asql_free_value(&w->end);
	}
}

static void
destroy_pkconfig(aconfig* ac)
{
	pk_config* p = (pk_config*)ac;

	if (p->ns) free(p->ns);
	if (p->set) free(p->set);

	destroy_insert_param(&p->i);
	destroy_select_param(&p->s);
	destroy_udf_param(&p->u);
	asql_free_value(&p->key);
	free(p);
}

static void
destroy_skconfig(aconfig* ac)
{
	sk_config* s = (sk_config*)ac;

	if (s->ns) free(s->ns);
	if (s->set) free(s->set);
	if (s->ibname) free(s->ibname);
	if (s->itype) free(s->itype);

	destroy_select_param(&s->s);
	destroy_udf_param(&s->u);
	destroy_where(&s->where);
	free(s);
}

static void
destroy_infoconfig(aconfig* ac)
{
	info_config* i = (info_config*)ac;

	if (i->cmd) free(i->cmd);
	if (i->backout_cmd) free(i->backout_cmd);
	free(i);
}

static void
destroy_scanconfig(aconfig* ac)
{
	scan_config* s = (scan_config*)ac;

	if (s->ns) free(s->ns);
	if (s->set) free(s->set);

	destroy_select_param(&s->s);
	destroy_udf_param(&s->u);
	free(s);
}

static void
destroy_truncateconfig(aconfig* ac)
{
	truncate_config* tc = (truncate_config*)ac;

	if (tc->ns) free(tc->ns);
	if (tc->set) free(tc->set);

	free(tc);
}

static void
destroy_runfileconfig(aconfig* ac)
{
	runfile_config* r = (runfile_config*)ac;
	if (r->fname) free(r->fname);
	free(r);
}

static void
destroy_admconfig(aconfig* ac)
{
	admin_config* cfg = (admin_config*)ac;
	free(cfg->user);
	free(cfg->password);
	free(cfg->role);

	if (cfg->whitelist) {
		as_vector* list = cfg->whitelist;
		for (uint32_t i = 0; i < list->size; i++) {
			void* data = as_vector_get_ptr(list, i);
			free(data);
		}
		as_vector_destroy(list);
	}

	if (cfg->list) {
		as_vector* list = cfg->list;
		for (uint32_t i = 0; i < list->size; i++) {
			void* data = as_vector_get_ptr(list, i);
			free(data);
		}
		as_vector_destroy(list);
	}
	free(cfg);
}
