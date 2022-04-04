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

#include <stdlib.h>
#include <time.h>

#include <aerospike/as_admin.h>
#include <aerospike/as_string_builder.h>

#include <asql.h>
#include <asql_tokenizer.h>
#include <asql_admin.h>
#include <asql_conf.h>
#include <asql_info.h>
#include <asql_key.h>
#include <asql_print.h>
#include <asql_query.h>
#include <asql_scan.h>
#include <asql_truncate.h>

#include "renderer/json_renderer.h"
#include "renderer/no_renderer.h"
#include "renderer/raw_renderer.h"
#include "renderer/table.h"


//=========================================================
// Forward Declarations.
//

extern void destroy_vector(as_vector* list, bool is_name);
extern int destroy_aconfig(aconfig* ac);

void strncpy_and_strip_quotes(char* to, const char* from, size_t size);
static bool is_quoted_literal(char* s);
static bool check_illegal_characters(char* s);

static bool lut_is_valid(char* s, uint64_t lut);
static bool parse_lut(char* s, uint64_t* lut);
static bool parse_name(char* s, asql_name* name, bool allow_empty);
static int parse_expression(tokenizer* tknzr, asql_value* value);
static int parse_value(char* s, asql_value* value);
static bool parse_value_list(tokenizer* tknzr, as_vector* v);

static bool parse_ns_and_set(tokenizer* tknzr, char** ns, char** set);
static bool parse_name_list(tokenizer* tknzr, as_vector* v, bool allow_empty);
static bool parse_pkey(tokenizer* tknzr, asql_value* value);
static bool parse_naked_name_list(tokenizer* tknzr, as_vector* v);
static bool parse_skey(tokenizer* tknzr, asql_where* where, asql_name* ibname);
//static bool parse_predicate(tokenizer* tknzr, asql_predicate* where);
static bool parse_in(tokenizer* tknzr, asql_name* itype);
static char* parse_module(tokenizer* tknzr, bool filename_only);
static char* parse_module_pathname(tokenizer* tknzr);
static char* parse_module_filename(tokenizer* tknzr);
static int parse_permission(const char* token);
static int parse_roles(tokenizer* tknzr, const char* stop, as_vector* /*<char*>*/vector);
static int parse_privileges(tokenizer* tknzr, const char* stop, as_vector* /*<as_privilege*>*/vector);
static int parse_cast_expression(tokenizer* tknzr, asql_value* value);
static int parse_type_expression(tokenizer* tknzr, asql_value* value, asql_value_type_t vtype);

static aconfig* parse_grant_roles(tokenizer* tknzr);
static aconfig* parse_revoke_roles(tokenizer* tknzr);
static aconfig* parse_grant_privileges(tokenizer* tknzr);
static aconfig* parse_revoke_privileges(tokenizer* tknzr);
static aconfig* parse_query(tokenizer* tknzr, int type);
static aconfig* parse_createindex(tokenizer* tknzr);
static aconfig* parse_dropindex(tokenizer* tknzr);
static aconfig* parse_set_password(tokenizer* tknzr);
static aconfig* parse_set_whitelist(tokenizer* tknzr);
static aconfig* parse_create_role(tokenizer* tknzr);
static aconfig* parse_create_user(tokenizer* tknzr);
static aconfig* parse_drop_user(tokenizer* tknzr);
static aconfig* parse_drop_role(tokenizer* tknzr);
static aconfig* parse_show_user(tokenizer* tknzr);
static aconfig* parse_show_users(tokenizer* tknzr);
static aconfig* parse_show_role(tokenizer* tknzr);
static aconfig* parse_show_roles(tokenizer* tknzr);
static aconfig* parse_show_info(tokenizer* tknzr);

//=========================================================
// Inlines and Macros.
//

static inline bool
is_role_keyword(tokenizer* tknzr)
{
	return strcasecmp(tknzr->tok, "ROLE") == 0
	        || strcasecmp(tknzr->tok, "ROLES") == 0;
}

static inline bool
is_privilege_keyword(tokenizer* tknzr)
{
	return strcasecmp(tknzr->tok, "PRIVILEGE") == 0
	        || strcasecmp(tknzr->tok, "PRIVILEGES") == 0;
}

static inline bool
is_whitelist_keyword(tokenizer* tknzr)
{
	return strcasecmp(tknzr->tok, "WHITELIST") == 0;
}

static inline bool
verify_role(const char* role)
{
	return strlen(role) < AS_ROLE_SIZE;
}

static inline bool
verify_address(const char* address)
{
	return strlen(address) < 128;
}

#define IF_CURR_TOKEN_NULL_GOTO(x)    \
    if (!tknzr->tok) goto x;

#define GET_NEXT_TOKEN_OR_GOTO(x)     \
    get_next_token(tknzr);            \
	if (!tknzr->tok) goto x;

#define GET_NEXT_TOKEN_OR_RETURN(x)   \
    get_next_token(tknzr);            \
	if (!tknzr->tok) return x;

#define GET_NEXT_TOKEN_OR_ERROR(x)   \
    get_next_token(tknzr);           \
	if (!tknzr->tok) { predicting_parse_error(tknzr); return x; }



//=========================================================
// Public API.
//

aconfig*
aql_parse_insert(tokenizer* tknzr)
{
	asql_name ns = NULL;
	asql_name set = NULL;

	as_vector* bnames = NULL;
	as_vector* values = NULL;

	GET_NEXT_TOKEN_OR_GOTO(ERROR)
	if (strcasecmp(tknzr->tok, "INTO")) {
		goto ERROR;
	}

	GET_NEXT_TOKEN_OR_GOTO(ERROR)
	if (!parse_ns_and_set(tknzr, &ns, &set)) {
		goto ERROR;
	}

	if (check_illegal_characters(ns) || check_illegal_characters(set)) {
		goto ERROR;
	}

	if (set) {
		GET_NEXT_TOKEN_OR_GOTO(ERROR)
	}
	else if (!tknzr->tok) {
		goto ERROR;
	}

	bnames = as_vector_create(sizeof(asql_name), 5);
	if (!parse_name_list(tknzr, bnames, true)) {
		goto ERROR;
	}

	for (int i = 0; i < bnames->size; i++) {
		if (check_illegal_characters(as_vector_get_ptr(bnames, i)))
		{
			goto ERROR;
		}
	}

	// Must have at least one bin that is not a primary key.
	if (bnames->size < 2) {
		goto ERROR;
	}
	// Verify first bin name is PK.
	if (strcasecmp(as_vector_get_ptr(bnames, 0), "PK")) {
		goto ERROR;
	}

	GET_NEXT_TOKEN_OR_GOTO(ERROR)
	if (strcasecmp(tknzr->tok, "VALUES")) {
		goto ERROR;
	}

	GET_NEXT_TOKEN_OR_GOTO(ERROR)
	values = as_vector_create(sizeof(asql_value), 5);
	if (!parse_value_list(tknzr, values)) {
		goto ERROR;
	}

	// Ensure bin names match values.
	if (bnames->size != values->size) {
		goto ERROR;
	}

	pk_config* p = malloc(sizeof(pk_config));
	bzero(p, sizeof(pk_config));
	p->type = PRIMARY_INDEX_OP;
	p->op = WRITE_OP;
	p->ns = ns;
	p->set = set;

	// first value is PK
	memcpy(&p->key, as_vector_get(values, 0), sizeof(asql_value));
	as_vector_remove(values, 0);
	free(as_vector_get_ptr(bnames, 0));
	as_vector_remove(bnames, 0);


	if (p->key.type == AS_DOUBLE) {
		char* err_msg = "PK cannot be floating point value";
		g_renderer->render_error(-1, err_msg, NULL);
		goto ERROR;
	}

	p->i.bnames = bnames;
	p->i.values = values;

	return (aconfig*)p;

ERROR:
	predicting_parse_error(tknzr);
	if (ns) free(ns);
	if (set) free(set);

	if (bnames) {
		destroy_vector(bnames, true);
		as_vector_destroy(bnames);
	}

	if (values) {
		destroy_vector(values, false);
		as_vector_destroy(values);
	}
	return NULL;
}

aconfig*
aql_parse_delete(tokenizer* tknzr)
{
	asql_name ns = NULL;
	asql_name set = NULL;

	GET_NEXT_TOKEN_OR_GOTO(ERROR)

	if (strcasecmp(tknzr->tok, "FROM")) {
		goto ERROR;
	}

	GET_NEXT_TOKEN_OR_GOTO(ERROR)
	if (!parse_ns_and_set(tknzr, &ns, &set)) {
		goto ERROR;
	}

	if (set) {
		GET_NEXT_TOKEN_OR_GOTO(ERROR)
	}
	else if (!tknzr->tok) {
		goto ERROR;
	}

	if (strcasecmp(tknzr->tok, "WHERE")) {
		goto ERROR;
	}

	GET_NEXT_TOKEN_OR_GOTO(ERROR)

	pk_config* p = malloc(sizeof(pk_config));
	bzero(p, sizeof(pk_config));
	p->type = PRIMARY_INDEX_OP;
	p->op = DELETE_OP;
	p->ns = ns;
	p->set = set;
	if (!parse_pkey(tknzr, &p->key)) {
		destroy_aconfig((aconfig*)p);
		predicting_parse_error(tknzr);
		return NULL;
	}
	return (aconfig*)p;

ERROR:
	predicting_parse_error(tknzr);
	if (ns) free(ns);
	if (set) free(set);
	return NULL;
}

aconfig*
aql_parse_truncate(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_ERROR(NULL)

	truncate_config* tc = malloc(sizeof(truncate_config));
	memset(tc, 0, sizeof(truncate_config));

	tc->type = TRUNCATE_OP;
	tc->optype = ASQL_OP_TRUNCATE;

	uint64_t lut = 0;

	if (!parse_ns_and_set(tknzr, &tc->ns, &tc->set)) {
		predicting_parse_error(tknzr);
		goto truncate_error;
	}

	if (tc->set) {
		GET_NEXT_TOKEN_OR_GOTO(truncate_end)
	}

	if (! tknzr->tok) {
		goto truncate_end;
	}

	if (strcasecmp(tknzr->tok, "UPTO")) {
		predicting_parse_error(tknzr);
		goto truncate_error;
	}

	GET_NEXT_TOKEN_OR_GOTO(truncate_error)
	if (! parse_lut(tknzr->tok, &lut)) {
		g_renderer->render_error(-1, "Invalid lut, use format \"Dec 18 2017 09:53:20\" or 1513590800000000000.", NULL);
		goto truncate_error;
	}

	if (lut) {
		if (! lut_is_valid(tknzr->tok, lut)) {
			goto truncate_error;
		}
		tc->lut = lut;
	}

truncate_end:
	return (aconfig*)tc;

truncate_error:
	destroy_aconfig((aconfig*)tc);
	return NULL;
}

aconfig*
aql_parse_execute(tokenizer* tknzr)
{
	return parse_query(tknzr, ASQL_OP_EXECUTE);
}

aconfig*
aql_parse_select(tokenizer* tknzr)
{
	return parse_query(tknzr, ASQL_OP_SELECT);
}

aconfig*
aql_parse_explain(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_GOTO(ERROR);

	if (strcasecmp(tknzr->tok, "SELECT")) {
		g_renderer->render_error(
				-127,
				"\"Explain\" supports a primary-key operation. Type help for syntax.",
				NULL);
		goto ERROR;
	}

	aconfig* ac = aql_parse_select(tknzr);
	if (!ac) {
		// parse_select prints error.
		return NULL;
	}

	if (ac->type == PRIMARY_INDEX_OP) {
		((pk_config*)ac)->explain = true;
	} else {
		g_renderer->render_error(
				-127,
				"\"Explain\" supports a primary-key operation. Type help for syntax.",
				NULL);
		destroy_aconfig(ac);
		goto ERROR;
	}
	return ac;

ERROR:
	predicting_parse_error(tknzr);
	return NULL;
}

aconfig*
aql_parse_aggregate(tokenizer* tknzr)
{
	return parse_query(tknzr, ASQL_OP_AGGREGATE);
}

aconfig*
aql_parse_grant(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_ERROR(NULL)

	aconfig* cfg = 0;

	if (strcasecmp(tknzr->tok, "ROLE") == 0
	        || strcasecmp(tknzr->tok, "ROLES") == 0) {
		cfg = parse_grant_roles(tknzr);
	}
	else if (strcasecmp(tknzr->tok, "PRIVILEGE") == 0
	        || strcasecmp(tknzr->tok, "PRIVILEGES") == 0) {
		cfg = parse_grant_privileges(tknzr);
	}
	else {
		predicting_parse_error(tknzr);
		return 0;
	}
	return cfg;
}

aconfig*
aql_parse_revoke(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_ERROR(NULL)

	aconfig* cfg = 0;

	if (strcasecmp(tknzr->tok, "ROLE") == 0
	        || strcasecmp(tknzr->tok, "ROLES") == 0) {
		cfg = parse_revoke_roles(tknzr);
	}
	else if (strcasecmp(tknzr->tok, "PRIVILEGE") == 0
	        || strcasecmp(tknzr->tok, "PRIVILEGES") == 0) {
		cfg = parse_revoke_privileges(tknzr);
	}
	return cfg;
}

aconfig*
aql_parse_asinfo(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_ERROR(NULL)
	asql_name info_str;
	if (!parse_name(tknzr->tok, &info_str, false)) {
		return NULL;
	}

	info_config* i = asql_info_config_create(info_str, NULL, false);
	return (aconfig*)i;
}

aconfig*
aql_parse_desc(tokenizer* tknzr)
{
	char* filename = parse_module_filename(tknzr);

	if (!filename) {
		predicting_parse_error(tknzr);
		return NULL;
	}

	char infocmd[1024];
	sprintf(infocmd, "udf-get:filename=%s\n", filename);
	free(filename);

	info_config* i = asql_info_config_create(strdup(infocmd), NULL, false);
	return (aconfig*)i;
}

aconfig*
aql_parse_stat(tokenizer* tknzr)
{
	get_next_token(tknzr);
	info_config* i = NULL;

	if (!tknzr->tok)
		goto stat_error;

	if (!strcasecmp(tknzr->tok, "SYSTEM")) {
		i = asql_info_config_create(strdup("statistics"), NULL, false);
	}
	else if (!strcasecmp(tknzr->tok, "INDEX")) {

		GET_NEXT_TOKEN_OR_GOTO(stat_error);

		asql_name ns = NULL;
		if (!parse_name(tknzr->tok, &ns, false)) {
			return NULL;
		}

		GET_NEXT_TOKEN_OR_GOTO(stat_error);

		asql_name iname = NULL;
		if (!parse_name(tknzr->tok, &iname, false)) {
			return NULL;
		}

		char infocmd[1024];
		sprintf(infocmd, "sindex/%s/%s\n", ns, iname);
		i = asql_info_config_create(strdup(infocmd), NULL, false);

		if (iname) free(iname);
		if (ns) free(ns);
	}
	else if (!strcasecmp(tknzr->tok, "NAMESPACE")) {

		GET_NEXT_TOKEN_OR_GOTO(stat_error);

		asql_name ns = NULL;
		if (!parse_name(tknzr->tok, &ns, false)) {
			return NULL;
		}

		char infocmd[1024];
		sprintf(infocmd, "namespace/%s\n", ns);
		i = asql_info_config_create(strdup(infocmd), NULL, false);

		if (ns) free(ns);
	}
	else {
		goto stat_error;
	}

	return (aconfig*)i;

stat_error:
	predicting_parse_error(tknzr);
	return NULL;
}

aconfig*
aql_parse_registerudf(tokenizer* tknzr)
{
	char* pathname = parse_module_pathname(tknzr);
	if (!pathname) {
		predicting_parse_error(tknzr);
		return NULL;
	}

	info_config* i = asql_info_config_create(strdup("udf-put"), pathname,
			true);
	return (aconfig*)i;
}

aconfig*
aql_parse_removeudf(tokenizer* tknzr)
{
	char* filename = parse_module_filename(tknzr);

	if (!filename) {
		predicting_parse_error(tknzr);
		return NULL;
	}

	info_config* i = asql_info_config_create(strdup("udf-remove"),
			filename, true);
	return (aconfig*)i;
}

aconfig*
aql_parse_killquery(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_ERROR(NULL)
	char infocmd[1024];
	sprintf(infocmd, "query-kill:trid=%s", tknzr->tok);
	info_config* i = asql_info_config_create(strdup(infocmd), NULL, false);
	return (aconfig*)i;
}

aconfig*
aql_parse_killscan(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_ERROR(NULL)
	char infocmd[1024];
	sprintf(infocmd, "scan-abort:id=%s", tknzr->tok);
	info_config* i = asql_info_config_create(strdup(infocmd), NULL, false);
	return (aconfig*)i;
}

aconfig*
aql_parse_run(tokenizer* tknzr)
{
	asql_name fname = NULL;
	GET_NEXT_TOKEN_OR_GOTO(ERROR);

	if (!parse_name(tknzr->tok, &fname, false)) {
		goto ERROR;
	}

	runfile_config* r = malloc(sizeof(runfile_config));
	r->type = RUNFILE_OP;
	r->fname = fname;
	return (aconfig*)r;

ERROR:
	predicting_parse_error(tknzr);

	if (fname) free(fname);

	return NULL;
}

aconfig*
aql_parserun_set(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_ERROR(NULL);

	if (!strcasecmp(tknzr->tok, "PASSWORD")) {
		return parse_set_password(tknzr);
	}

	if (!strcasecmp(tknzr->tok, "WHITELIST")) {
		return parse_set_whitelist(tknzr);
	}

	char* name = strdup(tknzr->tok);
	GET_NEXT_TOKEN_OR_ERROR(NULL);
	char* value = tknzr->tok;

	if (! option_set(g_config, name, value)) {
		predicting_parse_error(tknzr);
	}

	free(name);
	return NULL;
}

aconfig*
aql_parserun_get(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_ERROR(NULL);
	if (! option_get(g_config, tknzr->tok)) {
		predicting_parse_error(tknzr);
	}
	return NULL;
}

aconfig*
aql_parserun_reset(tokenizer* tknzr)
{
	char* name = NULL;

	GET_NEXT_TOKEN_OR_ERROR(NULL);

	name = tknzr->tok;
	
	if (! option_reset(g_config, name)) {
		predicting_parse_error(tknzr);
	}

	return NULL;
}


aconfig*
aql_parserun_print(tokenizer* tknzr)
{
	char* printCmd = strdup(tknzr->ocmd);
	char* printContent = printCmd + 6;
	if (printContent) {
		fprintf(stdout, "%s\n", printContent);
	}
	if (printCmd) {
		free(printCmd);
	}
	fprintf(stdout, "\n\n");
	fprintf(stderr, "Warning: The PRINT command has been deprecated and will be removed in the next release of aql.\n"); 
	return NULL;
}

aconfig*
aql_parserun_system(tokenizer* tknzr)
{
	char* syscmd = strdup(tknzr->ocmd);
	char* cmd = syscmd + 7;

	if (cmd) {
		fprintf(stdout, "%s\n", cmd);
		int rv = system(cmd);
		if (rv == -1) {
			fprintf(
			stderr,
			        "Error executing system command. Could not create sub-process\n");
		}
		else if (cmd != NULL && rv) {
			fprintf(stderr, "Error %d executing system command.\n", rv);
		}
		else if (cmd == NULL && !rv) {
			fprintf(
			stderr,
			        "Error executing system command. Command is passed is NULL.\n");
		}
	}

	if (syscmd) {
		free(syscmd);
	}

	fprintf(stdout, "\n\n");
	fprintf(stderr, "Warning: The SYSTEM command has been deprecated and will be removed in the next release of aql.\n");
	return NULL;
}

aconfig*
aql_parse_create(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_ERROR(NULL)
	if (!strcasecmp(tknzr->tok, "INDEX") || !strcasecmp(tknzr->tok, "LIST")
	        || !strcasecmp(tknzr->tok, "MAPKEYS")
	        || !strcasecmp(tknzr->tok, "MAPVALUES")) {
		return parse_createindex(tknzr);
	}
	else if (strcasecmp(tknzr->tok, "USER") == 0) {
		return parse_create_user(tknzr);
	}
	else if (strcasecmp(tknzr->tok, "ROLE") == 0) {
		return parse_create_role(tknzr);
	}
	return NULL;
}

aconfig*
aql_parse_drop(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_ERROR(NULL)

	aconfig* cfg = 0;

	if (strcasecmp(tknzr->tok, "INDEX") == 0) {
		cfg = parse_dropindex(tknzr);
	}
	else if (strcasecmp(tknzr->tok, "USER") == 0) {
		cfg = parse_drop_user(tknzr);
	}
	else if (strcasecmp(tknzr->tok, "ROLE") == 0) {
		cfg = parse_drop_role(tknzr);
	}
	return cfg;
}

aconfig*
aql_parse_show(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_GOTO(show_error)
	if (!strcasecmp(tknzr->tok, "USERS")) {
		return parse_show_users(tknzr);
	}
	else if (!strcasecmp(tknzr->tok, "USER")) {
		return parse_show_user(tknzr);
	}
	else if (!strcasecmp(tknzr->tok, "ROLES")) {
		return parse_show_roles(tknzr);
	}
	else if (!strcasecmp(tknzr->tok, "ROLE")) {
		return parse_show_role(tknzr);
	}

	return parse_show_info(tknzr);

show_error:
	predicting_parse_error(tknzr);
	return NULL;
}

const char*
map_enum_to_string(map_enum_string map[], int value)
{
	for (size_t i = 0; map[i].name; i++) {
		if (map[i].value == value) {
			return map[i].name;
		}
	}
	return "unknown";
}

void
strncpy_and_strip_quotes(char* to, const char* from, size_t size)
{
	size_t len = 0;
	for (; len < size; len++) {
		if (from[len] == '\0') {
			break;
		}
	}
	if (!len) {
		return;
	}
	if (len > 2
	        && ((from[0] == '\'' && from[len - 1] == '\'')
	                || (from[0] == '\"' && from[len - 1] == '\"'))) {
		memcpy(to, from + 1, len - 2);
		to[len - 2] = '\0';
		return;
	}
	if (len == size) {
		len--;
	}
	memcpy(to, from, len);
	to[len] = '\0';
}




//=========================================================
// Local Helper.
//

static bool
is_quoted_literal(char* s)
{
	bool iql = false;
	if (s) {
		size_t len = strlen(s);
		if (len > 2
		        && ((*s == '\'' && s[len - 1] == '\'')
		                || (*s == '\"' && s[len - 1] == '\"'))) {
			iql = true;
		}
	}
	return iql;
}

// Does not check for all illegal character in https://docs.aerospike.com/guide/limitations
// just the ones that commonly mess up the info protocol
static bool
check_illegal_characters(char *s)
{
	char *c = NULL;
	if ((c = strstr(s, ";")) || (c = strstr(s, ":")))
	{
		char err_msg[25];
		snprintf(err_msg, 25, "Illegal character - '%c'", (char)c[0]);
		g_renderer->render_error(-1, err_msg, NULL);
		return true;
	}
	return false;
}

static bool
lut_is_valid(char *lut_str, uint64_t lut)
{
	char err_msg[1024];

	if ((lut <= CITRUSLEAF_EPOCH_NS)
			|| (cf_clepoch_ms_from_utc_ns(lut) > cf_clepoch_milliseconds())) {

		time_t rawtime;
		time(&rawtime);
		struct tm *cur_tm = localtime(&rawtime);

		char buf[255];
		strftime(buf, sizeof(buf), "%b %d %Y %H:%M:%S", cur_tm);

		snprintf(err_msg, 1023, "LUT %s out of range. Choose LUT greater than \"Jan 1 2010 00:00:00\" (1262304000000000000 ns) and less than current time \"%s\" (%"PRIu64" ns)",
				lut_str, buf, cf_clock_getabsolute() * 1000 * 1000);
		g_renderer->render_error(-1, err_msg, NULL);
		return false;
	}

	return true;
}

static bool
parse_lut(char* s, uint64_t *lut)
{
	char lut_str[256];
	memset(lut_str, 0, 256);
	char* endptr = 0;
	uint64_t val = strtoul(s, &endptr, 0);

	if (*endptr == 0) {
		*lut = val;
		return true;
	}

	struct tm lut_tm;
	memset(&lut_tm, 0, sizeof(struct tm));
	strncpy_and_strip_quotes(lut_str, s, strlen(s));
	strptime(lut_str, "%b %d %Y %H:%M:%S", &lut_tm);

	time_t t = mktime(&lut_tm);

	if (t < 0) {
		return false;
	}

	// return nano
	*lut = 1000000000 * t;
	return true;
}

static bool
parse_name(char* s, asql_name* name, bool allow_empty)
{
	int len = (int)strlen(s);

	if (!len) {
		return false;
	}

	if (len < 2)
	{
		*name = strdup(s);
		return true;
	}

	if ((*s == '\'' && s[len - 1] == '\'')
			|| (*s == '\"' && s[len - 1] == '\"')) {

		// Empty string
		if (len == 2) {
			if (allow_empty) {
				*name = strdup("");
				return true;
			} else {
				return false;
			}
		}

		// trim quotes
		char* str = malloc(len - 1);
		memcpy(str, s + 1, len - 2);
		str[len - 2] = 0;
		*name = str;
		return true;
	}

	*name = strdup(s);
	return true;
}

// Parse an expression, which may involve a type cast operation.
static int
parse_expression(tokenizer* tknzr, asql_value* value)
{
	char* s = tknzr->tok;

	if (!strlen(s)) {
		return -1;
	}

	asql_value_type_t vtype;
	if (!strcasecmp(s, "CAST")) {
		return parse_cast_expression(tknzr, value);
	}
	else if (ASQL_VALUE_TYPE_NONE
	        != (vtype = asql_value_type_from_type_name(s))) {
		return parse_type_expression(tknzr, value, vtype);
	}

	return parse_value(s, value);
}

static int
parse_value(char* s, asql_value* value)
{
	value->vt = ASQL_VALUE_TYPE_NONE;
	int len = (int)strlen(s);

	if (!len) {
		return -1;
	}

	if (strcasecmp(s, "NULL") == 0) {
		value->type = AS_NIL;
		return 0;
	}

	if (len > 2
	        && ((*s == '\'' && s[len - 1] == '\'')
	                || (*s == '\"' && s[len - 1] == '\"'))) {
		value->type = AS_STRING;
		char* str = malloc(len - 1);
		memcpy(str, s + 1, len - 2);
		str[len - 2] = 0;
		value->u.str = str;
		return 0;
	}

	// Allow empty strings
	if (len == 2
	        && ((*s == '\'' && s[len - 1] == '\'')
	                || (*s == '\"' && s[len - 1] == '\"'))) {
		value->type = AS_STRING;
		value->u.str = strdup("");
		return 0;
	}


	char* endptr = 0;
	int64_t val = strtoll(s, &endptr, 0);

	if (*endptr == 0) {
		value->type = AS_INTEGER;
		value->u.i64 = val;
		return 0;
	}

	if (strstr(s, ".") == NULL) {
		// was not parsable as int but does not contain '.'
		return -2;
	}

	double dbl = strtod(s, &endptr);

	if (*endptr == 0) {
		value->type = AS_DOUBLE;
		value->u.dbl = dbl;
		return 0;
	}
	return -2;
}

static bool
parse_value_list(tokenizer* tknzr, as_vector* v)
{
	if (strcmp(tknzr->tok, "(")) {
		return false;
	}

	GET_NEXT_TOKEN_OR_RETURN(false)
	if (!strcmp(tknzr->tok, ")")) {
		return true;
	}

	if (!strcmp(tknzr->tok, ",")) {
		return false;
	}

	while (1) {

		if (!strcasecmp(tknzr->tok, "NULL")) {
			asql_value value;
			value.type = AS_STRING;
			value.u.str = NULL;
			// "NULL" is empty bin
			as_vector_append(v, &value);
		}
		else {
			asql_value value;
			if (parse_expression(tknzr, &value)) {
				return false;
			}
			as_vector_append(v, &value);
		}

		GET_NEXT_TOKEN_OR_RETURN(false)
		if (strcmp(tknzr->tok, ",")) {
			if (strcmp(tknzr->tok, ")")) {
				return false;
			}
			return true;
		}
		GET_NEXT_TOKEN_OR_RETURN(false)
	}
}


static bool
parse_ns_and_set(tokenizer* tknzr, asql_name* ns, asql_name* set)
{
	if (!parse_name(tknzr->tok, ns, false)) {
		return false;
	}

	*set = NULL;

	GET_NEXT_TOKEN_OR_RETURN(true);

	if (!strcmp(tknzr->tok, ".")) {
		GET_NEXT_TOKEN_OR_RETURN(false)
		if (!parse_name(tknzr->tok, set, false)) {
			return false;
		}
		return true;
	}
	*set = NULL;
	return true;
}

static bool
parse_name_list(tokenizer* tknzr, as_vector* v, bool allow_empty)
{
	if (strcmp(tknzr->tok, "(")) {
		return false;
	}

	GET_NEXT_TOKEN_OR_RETURN(false)
	if (!strcmp(tknzr->tok, ")")) {
		return true;
	}

	if (!strcmp(tknzr->tok, ",")) {
		return false;
	}

	while (1) {

		asql_name name = NULL;
		if (!parse_name(tknzr->tok, &name, allow_empty)) {
			return false;
		}

		as_vector_append(v, &name);
		GET_NEXT_TOKEN_OR_RETURN(false)
		if (strcmp(tknzr->tok, ",")) {
			if (strcmp(tknzr->tok, ")")) {
				return false;
			}
			return true;
		}
		GET_NEXT_TOKEN_OR_RETURN(false)
	}
}

static bool
parse_naked_name_list(tokenizer* tknzr, as_vector* v)
{
	while (1) {
		asql_name name = NULL;

		if (!parse_name(tknzr->tok, &name, true)) {
			return false;
		}

		as_vector_append(v, &name);
		GET_NEXT_TOKEN_OR_RETURN(false)
		if (strcmp(tknzr->tok, ",")) {
			return true;
		}
		GET_NEXT_TOKEN_OR_RETURN(false)
	}
	return true;
}

static bool
parse_pkey(tokenizer* tknzr, asql_value* value)
{
	asql_value_type_t vt = ASQL_VALUE_TYPE_NONE;
	if (!strcasecmp(tknzr->tok, "DIGEST")) {
		vt = ASQL_VALUE_TYPE_DIGEST;
	}
	else if (!strcasecmp(tknzr->tok, "EDIGEST")) {
		vt = ASQL_VALUE_TYPE_EDIGEST;
	}
	else if (!strcasecmp(tknzr->tok, "PK")) {
		vt = ASQL_VALUE_TYPE_NONE;
	}
	else {
		return false;
	}

	GET_NEXT_TOKEN_OR_RETURN(false)
	if (strcmp(tknzr->tok, "=")) {
		return false;
	}

	GET_NEXT_TOKEN_OR_RETURN(false)
	if (parse_value(tknzr->tok, value) != 0) {
		return false;
	}

	if (vt != ASQL_VALUE_TYPE_NONE) {
		value->vt = vt;
	}
	return true;
}

static bool
parse_skey(tokenizer* tknzr, asql_where* where, asql_name* ibname)
{
	// SECONDARY INDEX QUERY
	if (!parse_name(tknzr->tok, ibname, false)) {
		return false;
	}

	GET_NEXT_TOKEN_OR_GOTO(filter_error)
	if (!strcmp(tknzr->tok, "=")) { // Equality Lookup
		GET_NEXT_TOKEN_OR_GOTO(filter_error)
		if (parse_expression(tknzr, &where->beg) != 0) {
			return false;
		}
		where->end = where->beg;
		where->qtype = ASQL_QUERY_TYPE_EQUALITY;
	}
	else if (!strcasecmp(tknzr->tok, "BETWEEN")) { // Range Lookup
		GET_NEXT_TOKEN_OR_GOTO(filter_error)
		if (parse_expression(tknzr, &where->beg) != 0) {
			return false;
		}

		GET_NEXT_TOKEN_OR_GOTO(filter_error)
		if (strcasecmp(tknzr->tok, "AND")) {
			return false;
		}

		GET_NEXT_TOKEN_OR_GOTO(filter_error)
		if (parse_expression(tknzr, &where->end) != 0) {
			return false;
		}

		if (where->beg.type != AS_INTEGER
				|| where->end.type != AS_INTEGER) {
			return false;
		}
		where->qtype = ASQL_QUERY_TYPE_RANGE;
	}
	else if (!strcasecmp(tknzr->tok, "CONTAINS")) { // GeoJSON Point-Within-Region Lookup
		GET_NEXT_TOKEN_OR_GOTO(filter_error)
		if (parse_expression(tknzr, &where->beg) != 0
				|| where->beg.type != AS_GEOJSON) {
			return false;
		}
		else {
			where->qtype = ASQL_QUERY_TYPE_CONTAINS;
		}
	}
	else if (!strcasecmp(tknzr->tok, "WITHIN")) { // GeoJSON Region-Contains-Point Lookup
		GET_NEXT_TOKEN_OR_GOTO(filter_error)
		if (parse_expression(tknzr, &where->beg) != 0
				|| where->beg.type != AS_GEOJSON) {
			return false;
		}
		else {
			where->qtype = ASQL_QUERY_TYPE_WITHIN;
		}
	}
	else {
		return false;
	}

	return true;

filter_error:
	return false;
}

static bool
parse_in(tokenizer* tknzr, asql_name* itype)
{
	if (!strcasecmp(tknzr->tok, "IN")) {
		GET_NEXT_TOKEN_OR_RETURN(false)
		if (strcasecmp(tknzr->tok, "LIST")
				&& strcasecmp(tknzr->tok, "MAPKEYS")
				&& strcasecmp(tknzr->tok, "MAPVALUES")) {
			return false;
		}
		*itype = strdup(tknzr->tok);
		GET_NEXT_TOKEN_OR_RETURN(false);
	}
	return true;
}


static char*
parse_module(tokenizer* tknzr, bool filename_only)
{
	GET_NEXT_TOKEN_OR_GOTO(module_error)
	if (!strcasecmp(tknzr->tok, "PACKAGE")
	        || !strcasecmp(tknzr->tok, "MODULE")) {
		GET_NEXT_TOKEN_OR_GOTO(module_error)
		if (!filename_only) {
			if (!is_quoted_literal(tknzr->tok)) {
				goto module_error;
			}
		}
		asql_name pfile = NULL;

		if (!parse_name(tknzr->tok, &pfile, false)) {
			goto module_error;
		}

		if (!filename_only) {
			return pfile;
		}

		as_string string_filename;
		const char* pname = as_basename(&string_filename, pfile);

		char* extension = 0;
		get_next_token(tknzr);
		if (tknzr->tok && !strcmp(tknzr->tok, ".")) {
			get_next_token(tknzr);
			if (tknzr->tok) {
				extension = strdup(tknzr->tok);
			}
		}
		char* filename;
		if (extension) {
			filename = malloc(strlen(pname) + 1 + strlen(extension) + 1);
			sprintf(filename, "%s.%s", pname, extension);
			free(extension);
		}
		else {
			filename = strdup(pname);
		}

		as_string_destroy(&string_filename);
		free(pfile);

		return filename;
	}
module_error:
	return NULL;
}

static char*
parse_module_pathname(tokenizer* tknzr)
{
	return parse_module(tknzr, false);
}

static char*
parse_module_filename(tokenizer* tknzr)
{
	return parse_module(tknzr, true);
}

static int
parse_permission(const char* token)
{
	if (strcasecmp(token, "user-admin") == 0) {
		return AS_PRIVILEGE_USER_ADMIN;
	}

	if (strcasecmp(token, "sys-admin") == 0) {
		return AS_PRIVILEGE_SYS_ADMIN;
	}

	if (strcasecmp(token, "data-admin") == 0) {
		return AS_PRIVILEGE_DATA_ADMIN;
	}

	if (strcasecmp(token, "read") == 0) {
		return AS_PRIVILEGE_READ;
	}

	if (strcasecmp(token, "read-write") == 0) {
		return AS_PRIVILEGE_READ_WRITE;
	}

	if (strcasecmp(token, "read-write-udf") == 0) {
		return AS_PRIVILEGE_READ_WRITE_UDF;
	}

	if (strcasecmp(token, "write") == 0) {
		return AS_PRIVILEGE_WRITE;
	}
	return -1;
}

static int
parse_roles(tokenizer* tknzr, const char* stop, as_vector* /*<char*>*/vector)
{
	asql_name name = NULL;

	while (1) {
		GET_NEXT_TOKEN_OR_RETURN(0)

		if (stop && strcasecmp(tknzr->tok, stop) == 0) {
			return 1;
		}

		if (strcmp(tknzr->tok, ",") == 0) {
			continue;
		}

		if (!verify_role(tknzr->tok)) {
			char err_msg[1024];
			snprintf(err_msg, 1023, "Invalid role: %s", tknzr->tok);
			g_renderer->render_error(AEROSPIKE_ERR_CLIENT, err_msg, NULL);
			return -1;
		}

		if (!parse_name(tknzr->tok, &name, false)) {
			return -1;
		}

		as_vector_append(vector, &name);

		GET_NEXT_TOKEN_OR_RETURN(0)

		if (strcmp(tknzr->tok, ",") == 0) {
			continue;
		}

		if (stop && strcasecmp(tknzr->tok, stop) == 0) {
			return 1;
		}

		char err_msg[1024];
		snprintf(err_msg, 1023, "Role not separated by comma: %s", tknzr->tok);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, err_msg, NULL);
		return -1;
	}
}

static int
parse_privileges(tokenizer* tknzr, const char* stop,
                 as_vector* /*<as_privilege*>*/vector)
{
	as_privilege* priv;

	while (1) {
		GET_NEXT_TOKEN_OR_RETURN(0)

		if (stop && strcasecmp(tknzr->tok, stop) == 0) {
			return 1;
		}

		if (strcmp(tknzr->tok, ",") == 0) {
			continue;
		}

		if (strcmp(tknzr->tok, ".") == 0) {
			g_renderer->render_error(AEROSPIKE_ERR_CLIENT,
			                         "Invalid privilege: Extraneous '.'", NULL);
			return -1;
		}

		int code = parse_permission(tknzr->tok);

		if (code < 0) {
			char err_msg[1024];
			snprintf(err_msg, 1023, "Invalid privilege: %s", tknzr->tok);
			g_renderer->render_error(AEROSPIKE_ERR_CLIENT, err_msg, NULL);
			return -1;
		}

		priv = malloc(sizeof(as_privilege));
		priv->code = code;
		priv->ns[0] = 0;
		priv->set[0] = 0;
		as_vector_append(vector, &priv);

		GET_NEXT_TOKEN_OR_RETURN(0)

		if (strcmp(tknzr->tok, ",") == 0) {
			continue;
		}

		if (stop && strcasecmp(tknzr->tok, stop) == 0) {
			return 1;
		}

		if (strcmp(tknzr->tok, ".") != 0) {
			char err_msg[1024];
			snprintf(err_msg, 1023, "Expected '.' but received '%s'",
			         tknzr->tok);
			g_renderer->render_error(AEROSPIKE_ERR_CLIENT, err_msg, NULL);
			return -1;
		}

		GET_NEXT_TOKEN_OR_RETURN(0)

		as_strncpy(priv->ns, tknzr->tok, sizeof(priv->ns));

		GET_NEXT_TOKEN_OR_RETURN(0)

		if (strcmp(tknzr->tok, ",") == 0) {
			continue;
		}

		if (stop && strcasecmp(tknzr->tok, stop) == 0) {
			return 1;
		}

		if (strcmp(tknzr->tok, ".") != 0) {
			char err_msg[1024];
			snprintf(err_msg, 1023, "Expected '.' but received '%s'",
			         tknzr->tok);
			g_renderer->render_error(AEROSPIKE_ERR_CLIENT, err_msg, NULL);
			return -1;
		}

		GET_NEXT_TOKEN_OR_RETURN(0)

		as_strncpy(priv->set, tknzr->tok, sizeof(priv->set));
	}
}

static int
parse_address(tokenizer* tknzr, const char* stop, as_string_builder* sb)
{
	as_string_builder_reset(sb);

	while (1) {
		GET_NEXT_TOKEN_OR_RETURN(0)

		if (stop && strcasecmp(tknzr->tok, stop) == 0) {
			return 1;
		}

		if (tknzr->tok[0] == ',') {
			return 2;
		}

		as_string_builder_append(sb, tknzr->tok);
	}
}

static int
parse_whitelist(tokenizer* tknzr, const char* stop, as_vector* /*<char*>*/vector)
{
	as_string_builder sb;
	as_string_builder_inita(&sb, 128, false);

	int status;

	while (1) {
		status = parse_address(tknzr, stop, &sb);

		if (sb.length > 0) {
			char* address;

			if (parse_name(sb.data, &address, false)) {
				as_vector_append(vector, &address);
			}
		}

		if (status != 2) {
			return status;
		}
	}
}

// Parse expressions of the form:  CAST(<ValueString> AS <TypeName>)
static int
parse_cast_expression(tokenizer* tknzr, asql_value* value)
{
	asql_value_type_t vtype = ASQL_VALUE_TYPE_NONE;

	char* value_str = NULL;
	GET_NEXT_TOKEN_OR_GOTO(parse_error);

	if (strcasecmp(tknzr->tok, "(")) {
		goto parse_error;
	}

	GET_NEXT_TOKEN_OR_GOTO(parse_error);

	value_str = strdup(tknzr->tok);

	GET_NEXT_TOKEN_OR_GOTO(parse_error);

	if (strcasecmp(tknzr->tok, "AS")) {
		goto parse_error;
	}

	GET_NEXT_TOKEN_OR_GOTO(parse_error);

	if (ASQL_VALUE_TYPE_NONE
	        != (vtype = asql_value_type_from_type_name(tknzr->tok))) {
	}
	else {
		goto parse_error;
	}

	GET_NEXT_TOKEN_OR_GOTO(parse_error);

	if (strcasecmp(tknzr->tok, ")")) {
		goto parse_error;
	}

	// Parse the value string as the specified internal value type.
	if (asql_parse_value_as(value_str, value, vtype) != 0) {
		goto parse_error;
	}

	if (value_str) {
		free(value_str);
	}

	return 0;

parse_error:
	predicting_parse_error(tknzr);

	if (value_str) {
		free(value_str);
	}
	return -1;
}

// Parse expressions of the form:  <TypeName>(<ValueString>)
static int
parse_type_expression(tokenizer* tknzr, asql_value* value,
                           asql_value_type_t vtype)
{
	char* value_str = NULL;
	GET_NEXT_TOKEN_OR_GOTO(parse_error);

	if (strcasecmp(tknzr->tok, "(")) {
		goto parse_error;
	}

	GET_NEXT_TOKEN_OR_GOTO(parse_error);

	value_str = strdup(tknzr->tok);

	GET_NEXT_TOKEN_OR_GOTO(parse_error);

	if (strcasecmp(tknzr->tok, ")")) {
		goto parse_error;
	}

	// Parse the value string as the specified internal value type.
	if (asql_parse_value_as(value_str, value, vtype) != 0) {
		goto parse_error;
	}

	if (value_str) {
		free(value_str);
	}

	return 0;

parse_error:
	predicting_parse_error(tknzr);
	if (value_str) {
		free(value_str);
	}

	return -1;
}

static aconfig*
parse_grant_roles(tokenizer* tknzr)
{
	admin_config* cfg = 0;

	cfg = aql_admin_config_create();
	cfg->optype = ASQL_OP_GRANT;
	cfg->cmd = AQL_GRANT_ROLES;
	cfg->list = as_vector_create(sizeof(char*), 5);

	int status = parse_roles(tknzr, "TO", cfg->list);

	if (status < 0) {
		destroy_aconfig((aconfig*)cfg);
		return NULL;
	}

	if (status == 1) {
		GET_NEXT_TOKEN_OR_GOTO(ERROR)
		if (!parse_name(tknzr->tok, &cfg->user, false)) {
			return NULL;
		}

		return (aconfig*)cfg;
	}

ERROR:
	predicting_parse_error(tknzr);

	if (cfg) {
		destroy_aconfig((aconfig*)cfg);
	}
	return 0;
}

static aconfig*
parse_revoke_roles(tokenizer* tknzr)
{
	admin_config* cfg = 0;

	cfg = aql_admin_config_create();
	cfg->optype = ASQL_OP_REVOKE;
	cfg->cmd = AQL_REVOKE_ROLES;
	cfg->list = as_vector_create(sizeof(char*), 5);

	int status = parse_roles(tknzr, "FROM", cfg->list);

	if (status < 0) {
		destroy_aconfig((aconfig*)cfg);
		return 0;
	}

	if (status == 1) {
		GET_NEXT_TOKEN_OR_GOTO(ERROR)

		if (!parse_name(tknzr->tok, &cfg->user, false)) {
			return NULL;
		}
		return (aconfig*)cfg;
	}

ERROR:
	predicting_parse_error(tknzr);

	if (cfg) {
		destroy_aconfig((aconfig*)cfg);
	}
	return 0;
}

static aconfig*
parse_grant_privileges(tokenizer* tknzr)
{
	admin_config* cfg = 0;

	cfg = aql_admin_config_create();
	cfg->optype = ASQL_OP_GRANT;
	cfg->cmd = AQL_GRANT_PRIVILEGES;
	cfg->list = as_vector_create(sizeof(as_privilege*), 5);

	int status = parse_privileges(tknzr, "TO", cfg->list);

	if (status < 0) {
		destroy_aconfig((aconfig*)cfg);
		return 0;
	}

	if (status == 1) {
		GET_NEXT_TOKEN_OR_GOTO(ERROR)
		if (!parse_name(tknzr->tok, &cfg->role, false)) {
			return NULL;
		}

		return (aconfig*)cfg;
	}

ERROR:
	predicting_parse_error(tknzr);

	if (cfg) {
		destroy_aconfig((aconfig*)cfg);
	}
	return 0;
}

static aconfig*
parse_revoke_privileges(tokenizer* tknzr)
{
	admin_config* cfg = 0;

	cfg = aql_admin_config_create();
	cfg->optype = ASQL_OP_REVOKE;
	cfg->cmd = AQL_REVOKE_PRIVILEGES;
	cfg->list = as_vector_create(sizeof(as_privilege*), 5);

	int status = parse_privileges(tknzr, "FROM", cfg->list);

	if (status < 0) {
		destroy_aconfig((aconfig*)cfg);
		return 0;
	}

	if (status == 1) {
		GET_NEXT_TOKEN_OR_GOTO(ERROR)
		if (!parse_name(tknzr->tok, &cfg->role, false)) {
			return NULL;
		}
		return (aconfig*)cfg;
	}

ERROR:
	predicting_parse_error(tknzr);

	if (cfg) {
		destroy_aconfig((aconfig*)cfg);
	}
	return 0;
}

static aconfig*
parse_query(tokenizer* tknzr, int type)
{
	asql_name ns = NULL;
	asql_name set = NULL;
	asql_name udfpkg = NULL;
	asql_name udfname = NULL;
	asql_name itype = NULL;
	asql_name ibname = NULL;
	as_vector* bnames = NULL;
	as_vector* params = NULL;

	if (type == ASQL_OP_SELECT) {
		GET_NEXT_TOKEN_OR_GOTO(ERROR)
		if (strcmp(tknzr->tok, "*")) {
			bnames = as_vector_create(sizeof(asql_name), 5);
			// consumes one extra token.
			if (!parse_naked_name_list(tknzr, bnames)) {
				goto ERROR;
			}
		}
		else {
			GET_NEXT_TOKEN_OR_GOTO(ERROR)
		}

		// FROM
		if (strcasecmp(tknzr->tok, "FROM")) {
			goto ERROR;
		}
	}
	else {
		GET_NEXT_TOKEN_OR_GOTO(ERROR)
		if (!parse_name(tknzr->tok, &udfpkg, false)) {
			goto ERROR;
		}


		GET_NEXT_TOKEN_OR_GOTO(ERROR)
		if (strcmp(tknzr->tok, ".")) {
			goto ERROR;
		}

		GET_NEXT_TOKEN_OR_GOTO(ERROR)
		if (!parse_name(tknzr->tok, &udfname, false)) {
			goto ERROR;
		}

		GET_NEXT_TOKEN_OR_GOTO(ERROR)
		params = as_vector_create(sizeof(asql_value), 5);
		if (!parse_value_list(tknzr, params)) {
			goto ERROR;
		}

		// ON
		GET_NEXT_TOKEN_OR_GOTO(ERROR)
		if (strcasecmp(tknzr->tok, "ON")) {
			goto ERROR;
		}
	}

	// <ns>.<set>
	GET_NEXT_TOKEN_OR_GOTO(ERROR)
	if (!parse_ns_and_set(tknzr, &ns, &set))
		goto ERROR;
	if (set)
		get_next_token(tknzr);

	// SCAN Operations
	if (!tknzr->tok) {
		scan_config* s = malloc(sizeof(scan_config));
		bzero(s, sizeof(scan_config));
		s->type = SCAN_OP;
		s->ns = ns;
		s->set = set;
		if (type == ASQL_OP_SELECT) {
			s->s.bnames = bnames;
		}
		else {
			s->u.udfpkg = udfpkg;
			s->u.udfname = udfname;
			s->u.params = params;
		}
		return (aconfig*)s;
	}

	if (!parse_in(tknzr, &itype)) {
		goto ERROR;
	}

	if (strcasecmp(tknzr->tok, "WHERE")) {
		goto ERROR;
	}

	GET_NEXT_TOKEN_OR_GOTO(ERROR)

	// PRIMARY KEY QUERY
	if ((!strcasecmp(tknzr->tok, "PK")
			|| !strcasecmp(tknzr->tok, "EDIGEST")
			|| !strcasecmp(tknzr->tok, "DIGEST"))) { // PK Lookup

		// No IN clause or Aggregation on primary key.
		if (itype
			|| (type == ASQL_OP_AGGREGATE)) {
			goto ERROR;
		}

		// Parse primary key value.
		pk_config* p = malloc(sizeof(pk_config));
		bzero(p, sizeof(pk_config));
		p->type = PRIMARY_INDEX_OP;
		p->op = READ_OP;
		p->ns = ns;
		p->set = set;
		if (type == ASQL_OP_SELECT) {
			p->s.bnames = bnames;
		}
		else {
			p->u.udfpkg = udfpkg;
			p->u.udfname = udfname;
			p->u.params = params;
		}

		if (!parse_pkey(tknzr, &p->key)) {
			destroy_aconfig((aconfig*)p);
			predicting_parse_error(tknzr);
			return NULL;
		}

		return (aconfig*)p;
	}

	sk_config* s = malloc(sizeof(sk_config));
	bzero(s, sizeof(sk_config));
	s->type = SECONDARY_INDEX_OP;
	s->ns = ns;
	s->set = set;
	if (type == ASQL_OP_SELECT) {
		s->s.bnames = bnames;
	}
	else {
		s->u.udfpkg = udfpkg;
		s->u.udfname = udfname;
		s->u.params = params;
	}
	s->itype = itype;

	if (!parse_skey(tknzr, &s->where, &s->ibname)) {
		destroy_aconfig((aconfig*)s);
		predicting_parse_error(tknzr);
		return NULL;
	}

	return (aconfig*)s;

ERROR:
	predicting_parse_error(tknzr);
	if (ns) free(ns);
	if (set) free(set);
	if (udfpkg) free(udfpkg);
	if (udfname) free(udfname);
	if (ibname) free(ibname);
	if (itype) free(itype);

	if (bnames) {
		destroy_vector(bnames, true);
		as_vector_destroy(bnames);
	}

	if (params) {
		destroy_vector(params, false);
		as_vector_destroy(params);
	}
	return NULL;
}

static aconfig*
parse_createindex(tokenizer* tknzr)
{
	asql_name ns = NULL;
	asql_name set = NULL;
	asql_name itype = NULL;
	asql_name iname = NULL;
	asql_name bname = NULL;
	asql_name deco = NULL;

	// CREATE <lists,mapkeys,mapvalues>
	if (strcasecmp(tknzr->tok, "LIST") == 0
	        || strcasecmp(tknzr->tok, "MAPKEYS") == 0
	        || strcasecmp(tknzr->tok, "MAPVALUES") == 0) {
		deco = strdup(tknzr->tok);
		GET_NEXT_TOKEN_OR_GOTO(create_end)
	}

	// INDEX <index name>
	if (strcasecmp(tknzr->tok, "INDEX")) {
		goto create_end;
	}

	GET_NEXT_TOKEN_OR_GOTO(create_end)
	if (!parse_name(tknzr->tok, &iname, false)) {
		goto create_end;
	}

	// ON <ns.set>
	GET_NEXT_TOKEN_OR_GOTO(create_end)
	if (strcasecmp(tknzr->tok, "ON"))
		goto create_end;


	GET_NEXT_TOKEN_OR_GOTO(create_end)
	if (!parse_ns_and_set(tknzr, &ns, &set))
		goto create_end;
	if (set) {
		GET_NEXT_TOKEN_OR_GOTO(create_end)
	}


	// (<bin_path>)
	if (strcmp(tknzr->tok, "("))
		goto create_end;

	GET_NEXT_TOKEN_OR_GOTO(create_end)
	if (!parse_name(tknzr->tok, &bname, true)) {
		goto create_end;
	}

	GET_NEXT_TOKEN_OR_GOTO(create_end)
	if (strcmp(tknzr->tok, ")"))
		goto create_end;
	GET_NEXT_TOKEN_OR_GOTO(create_end)


	// numeric/string/geo2dspher
	if (strcasecmp(tknzr->tok, "NUMERIC") && strcasecmp(tknzr->tok, "STRING")
	        && strcasecmp(tknzr->tok, "GEO2DSPHERE")) {
		goto create_end;
	}
	itype = strdup(tknzr->tok);


	char infocmd[1024]; /* Command */
	char infobcmd[1024]; // Backout Command
	if (!deco) {
		sprintf(infocmd, "sindex-create:ns=%s%s%s;indexname=%s;"
		        "numbins=1;indexdata=%s,%s;priority=normal\n",
		        ns, (set ? ";set=": ""), (set ? set: ""), iname, bname, itype);
	}
	else {
		sprintf(infocmd, "sindex-create:ns=%s%s%s;indexname=%s;"
		        "numbins=1;indextype=%s;indexdata=%s,%s;priority=normal\n",
		        ns, (set ? ";set=": ""), (set ? set: ""), iname, deco, bname,
		        itype);
	}
	sprintf(infobcmd, "sindex-delete:ns=%s%s%s;indexname=%s\n", ns,
	        (set ? ";set=": ""), (set ? set: ""), iname);

	info_config* i = asql_info_config_create(strdup(infocmd), strdup(infobcmd),
			true);

	if (ns) free(ns);
	if (set) free(set);
	if (itype) free(itype);
	if (iname) free(iname);
	if (bname) free(bname);
	if (deco) free(deco);

	return (aconfig*)i;

create_end:
	if (ns) free(ns);
	if (set) free(set);
	if (itype) free(itype);
	if (iname) free(iname);
	if (bname) free(bname);
	if (deco) free(deco);

	predicting_parse_error(tknzr);
	return NULL;
}

static aconfig*
parse_dropindex(tokenizer* tknzr)
{
	asql_name ns = NULL;
	asql_name set = NULL;
	asql_name iname = NULL;

	GET_NEXT_TOKEN_OR_GOTO(drop_end)
	if (!parse_ns_and_set(tknzr, &ns, &set))
		goto drop_end;
	if (set) {
		GET_NEXT_TOKEN_OR_GOTO(drop_end)
	}

	if (!parse_name(tknzr->tok, &iname, false)) {
		goto drop_end;
	}

	char infocmd[1024];
	sprintf(infocmd, "sindex-delete:ns=%s%s%s;indexname=%s\n", ns,
	        (set ? ";set=": ""), (set ? set: ""), iname);

	info_config* i = asql_info_config_create(strdup(infocmd), NULL, true);

	if (ns) free(ns);
	if (set) free(set);
	if (iname) free(iname);

	return (aconfig*)i;

drop_end:
	if (ns) free(ns);
	if (set) free(set);
	if (iname) free(iname);

	predicting_parse_error(tknzr);
	return NULL;
}

static aconfig*
parse_set_password(tokenizer* tknzr)
{
	admin_config* cfg = aql_admin_config_create();
	cfg->optype = ASQL_OP_SET;
	cfg->cmd = AQL_SET_PASSWORD;

	GET_NEXT_TOKEN_OR_GOTO(ERROR)
	// Empty password is allowed !!
	if (! parse_name(tknzr->tok, &cfg->password, true)) {
		goto ERROR;
	}

	int ret = as_sql_lexer(0, &tknzr->tok);

	if (ret == -2) {
		goto ERROR;
	}

	if (!tknzr->tok) {
		goto END;
	}

	if (strcasecmp(tknzr->tok, "FOR") == 0) {
		GET_NEXT_TOKEN_OR_GOTO(ERROR)
		if (!parse_name(tknzr->tok, &cfg->user, false)) {
			goto ERROR;
		}
	}
	else {
		goto ERROR;
	}

END:
	fprintf(stdout, "\n");
	return (aconfig*)cfg;

ERROR:
	predicting_parse_error(tknzr);
	destroy_aconfig((aconfig*)cfg);
	return 0;
}

static aconfig*
parse_set_whitelist(tokenizer* tknzr)
{
	admin_config* cfg = aql_admin_config_create();
	cfg->whitelist = as_vector_create(sizeof(char*), 16);
	cfg->optype = ASQL_OP_SET;
	cfg->cmd = AQL_SET_WHITELIST;

	int status = parse_whitelist(tknzr, "FOR", cfg->whitelist);

	if (status < 0) {
		destroy_aconfig((aconfig*)cfg);
		return 0;
	}

	if (status == 1) {
		GET_NEXT_TOKEN_OR_GOTO(ERROR)
		if (!parse_name(tknzr->tok, &cfg->role, false)) {
			goto ERROR;
		}

		return (aconfig*)cfg;
	}

ERROR:
	predicting_parse_error(tknzr);
	destroy_aconfig((aconfig*)cfg);
	return 0;
}

static aconfig*
parse_create_role(tokenizer* tknzr)
{
	admin_config* cfg = aql_admin_config_create();
	cfg->whitelist = as_vector_create(sizeof(char*), 16);
	cfg->list = as_vector_create(sizeof(as_privilege*), 16);
	cfg->optype = ASQL_OP_CREATE;
	cfg->cmd = AQL_CREATE_ROLE;

	GET_NEXT_TOKEN_OR_GOTO(ERROR)
	if (!parse_name(tknzr->tok, &cfg->role, false)) {
		return 0;
	}

	int status = 0;

	while (true) {
		get_next_token(tknzr);

		if (tknzr->tok) {
			if (is_privilege_keyword(tknzr)) {
				status = parse_privileges(tknzr, "WHITELIST", cfg->list);

				if (status < 0) {
					destroy_aconfig((aconfig*)cfg);
					return 0;
				}

				if (status == 1) {
					status = parse_whitelist(tknzr, 0, cfg->whitelist);

					if (status < 0) {
						destroy_aconfig((aconfig*)cfg);
						return 0;
					}
				}
				break;
			}
			else if (is_whitelist_keyword(tknzr)) {
				status = parse_whitelist(tknzr, 0, cfg->whitelist);

				if (status < 0) {
					destroy_aconfig((aconfig*)cfg);
					return 0;
				}
				break;
			}
		}
		else {
			break;
		}
	}

	if (! (cfg->list->size > 0 || cfg->whitelist->size > 0)) {
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT,
		                         "Either privileges or whitelist is required", NULL);
		return 0;
	}
	return (aconfig*)cfg;

ERROR:
	predicting_parse_error(tknzr);
	destroy_aconfig((aconfig*)cfg);
	return 0;
}

static aconfig*
parse_create_user(tokenizer* tknzr)
{
	admin_config* cfg = aql_admin_config_create();
	cfg->list = as_vector_create(sizeof(asql_name), 5);
	cfg->optype = ASQL_OP_CREATE;
	cfg->cmd = AQL_CREATE_USER;

	GET_NEXT_TOKEN_OR_GOTO(ERROR)
	if (!parse_name(tknzr->tok, &cfg->user, false)) {
		goto ERROR;
	}
	// Default to empty password
	cfg->password = strdup("");

	while (true) {
		int ret = as_sql_lexer(0, &tknzr->tok);

		if (ret == -2) {
			goto ERROR;
		}

		if (tknzr->tok) {
			if (strcasecmp(tknzr->tok, "PASSWORD") == 0) {
				GET_NEXT_TOKEN_OR_GOTO(ERROR)
				if (!parse_name(tknzr->tok, &cfg->password, true)) {
					goto ERROR;
				}
			}
			else if (is_role_keyword(tknzr)) {
				if (parse_roles(tknzr, 0, cfg->list) < 0) {
					destroy_aconfig((aconfig*)cfg);
					return 0;
				}
				break;
			}
			else {
				goto ERROR;
			}
		}
		else {
			break;
		}
	}
	return (aconfig*)cfg;

ERROR:
	predicting_parse_error(tknzr);
	destroy_aconfig((aconfig*)cfg);
	return 0;
}

static aconfig*
parse_drop_user(tokenizer* tknzr)
{
	admin_config* cfg = aql_admin_config_create();
	cfg->optype = ASQL_OP_DROP;
	cfg->cmd = AQL_DROP_USER;

	GET_NEXT_TOKEN_OR_GOTO(ERROR)
	if (!parse_name(tknzr->tok, &cfg->user, false)) {
		return 0;
	}
	return (aconfig*)cfg;

ERROR:
	predicting_parse_error(tknzr);
	destroy_aconfig((aconfig*)cfg);
	return 0;
}

static aconfig*
parse_drop_role(tokenizer* tknzr)
{
	admin_config* cfg = aql_admin_config_create();
	cfg->optype = ASQL_OP_DROP;
	cfg->cmd = AQL_DROP_ROLE;

	GET_NEXT_TOKEN_OR_GOTO(ERROR)
	if (!parse_name(tknzr->tok, &cfg->role, false)) {
		return 0;
	}
	return (aconfig*)cfg;

ERROR:
	predicting_parse_error(tknzr);
	destroy_aconfig((aconfig*)cfg);
	return 0;
}

static aconfig*
parse_show_user(tokenizer* tknzr)
{
	admin_config* cfg = aql_admin_config_create();
	cfg->optype = ASQL_OP_SHOW;
	cfg->cmd = AQL_SHOW_USER;

	GET_NEXT_TOKEN_OR_GOTO(END)
	if (!parse_name(tknzr->tok, &cfg->user, false)) {
		return 0;
	}
	return (aconfig*)cfg;

END:
	return (aconfig*)cfg;
}

static aconfig*
parse_show_users(tokenizer* tknzr)
{
	admin_config* cfg = aql_admin_config_create();
	cfg->optype = ASQL_OP_SHOW;
	cfg->cmd = AQL_SHOW_USERS;
	return (aconfig*)cfg;
}

static aconfig*
parse_show_role(tokenizer* tknzr)
{
	get_next_token(tknzr);

	if (!tknzr->tok) {
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, "Role is required",
		NULL);
		return 0;
	}

	if (!verify_role(tknzr->tok)) {
		char err_msg[1024];
		snprintf(err_msg, 1023, "Invalid role: %s", tknzr->tok);
		g_renderer->render_error(AEROSPIKE_ERR_CLIENT, err_msg, NULL);
		return 0;
	}

	admin_config* cfg = aql_admin_config_create();
	cfg->optype = ASQL_OP_SHOW;
	cfg->cmd = AQL_SHOW_ROLE;
	if (!parse_name(tknzr->tok, &cfg->role, false)) {
		return 0;
	}
	return (aconfig*)cfg;
}

static aconfig*
parse_show_roles(tokenizer* tknzr)
{
	admin_config* cfg = aql_admin_config_create();
	cfg->optype = ASQL_OP_SHOW;
	cfg->cmd = AQL_SHOW_ROLES;
	return (aconfig*)cfg;
}

static aconfig*
parse_show_info(tokenizer* tknzr)
{
	info_config* i = NULL;

	if (!strcasecmp(tknzr->tok, "NAMESPACES")) {
		i = asql_info_config_create(strdup("namespaces"), NULL, false);
	}
	else if (!strcasecmp(tknzr->tok, "SETS")) {
		i = asql_info_config_create(strdup("sets"), NULL, false);
	}
	else if (!strcasecmp(tknzr->tok, "BINS")) {
		i = asql_info_config_create(strdup("bins"), NULL, false);
	}
	else if (!strcasecmp(tknzr->tok, "QUERIES")) {
		i = asql_info_config_create(strdup("jobs:module=query"), NULL, false);
	}
	else if (!strcasecmp(tknzr->tok, "SCANS")) {
		i = asql_info_config_create(strdup("jobs:module=scan"), NULL, false);
	}
	else if (!strcasecmp(tknzr->tok, "PACKAGES")
	        || !strcasecmp(tknzr->tok, "MODULES")) {
		i = asql_info_config_create(strdup("udf-list"), NULL, false);
	}
	else if (!strcasecmp(tknzr->tok, "INDEXES")) {
		get_next_token(tknzr);
		if (!tknzr->tok) {
			i = asql_info_config_create(strdup("sindex-list:"), NULL, false);
		}
		else {
			asql_name ns = NULL;
			asql_name set = NULL;

			if (!parse_ns_and_set(tknzr, &ns, &set)) {
				goto show_error;
			}

			char infocmd[1024];
			if (set)
				sprintf(infocmd, "sindex-list:ns=%s;set=%s\n", ns, set);
			else
				sprintf(infocmd, "sindex-list:ns=%s;\n", ns);

			i = asql_info_config_create(strdup(infocmd), NULL, false);

			if (ns) free(ns);
			if (set) free(set);
		}
	}
	else {
		goto show_error;
	}

	return (aconfig*)i;

show_error:
	predicting_parse_error(tknzr);
	return NULL;
}
