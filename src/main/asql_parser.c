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

#include <stdlib.h>
#include <time.h>

#include <aerospike/as_admin.h>
#include <aerospike/as_string_builder.h>

#include <asql.h>
#include <asql_tokenizer.h>
#include <asql_conf.h>
#include <asql_info.h>
#include <asql_key.h>
#include <asql_print.h>
#include <asql_query.h>
#include <asql_scan.h>

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
static bool parse_skey(tokenizer* tknzr, asql_where* where, asql_where **where2);
static bool parse_in(tokenizer* tknzr, asql_name* itype);
static char* parse_module(tokenizer* tknzr, bool filename_only);
static char* parse_module_pathname(tokenizer* tknzr);
static char* parse_module_filename(tokenizer* tknzr);
static int parse_cast_expression(tokenizer* tknzr, asql_value* value);
static int parse_type_expression(tokenizer* tknzr, asql_value* value, asql_value_type_t vtype);

static aconfig* parse_query(tokenizer* tknzr, int type);
static aconfig* parse_show_info(tokenizer* tknzr);

//=========================================================
// Inlines and Macros.
//

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
	p->optype = ASQL_OP_INSERT;
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
	p->optype = ASQL_OP_DELETE;
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

	info_config* i = asql_info_config_create(ASQL_OP_DESC, strdup(infocmd), NULL, false);
	return (aconfig*)i;
}

aconfig*
aql_parse_registerudf(tokenizer* tknzr)
{
	char* pathname = parse_module_pathname(tknzr);
	if (!pathname) {
		predicting_parse_error(tknzr);
		return NULL;
	}

	info_config* i = asql_info_config_create(ASQL_OP_REGISTER, strdup("udf-put"), pathname,
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

	info_config* i = asql_info_config_create(ASQL_OP_REMOVE, strdup("udf-remove"),
			filename, true);
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
	r->optype = ASQL_OP_RUN;
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
aql_parse_show(tokenizer* tknzr)
{
	GET_NEXT_TOKEN_OR_GOTO(show_error)

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
check_illegal_characters(char* s)
{
	if (s == NULL) {
		return false;
	}

	char* c = NULL;
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

	if (strcasecmp(s, "true") == 0) {
		value->type = AS_BOOLEAN;
		value->u.bol = true;
		return 0;
	}

	if (strcasecmp(s, "false") == 0)
	{
		value->type = AS_BOOLEAN;
		value->u.bol = false;
		return 0;
	}

	char* endptr = 0;
	int64_t val = strtoll(s, &endptr, 0);

	if (*endptr == 0) {
		value->type = AS_INTEGER;
		value->u.i64 = val;
		return 0;
	}

	// This check should be after all other type checks.
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
parse_skey(tokenizer* tknzr, asql_where* where, asql_where **where2)
{
	// SECONDARY INDEX QUERY
	if (!parse_name(tknzr->tok, &where->ibname, false)) {
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

		char* peek = peek_next_token(tknzr);
		if (peek != NULL && !strcasecmp(peek, "LIMIT")) {
			free(peek);
			return true;
		}
		free(peek);

		GET_NEXT_TOKEN_OR_RETURN(true);
		if (strcasecmp(tknzr->tok, "AND"))
		{
			return false;
		}

		*where2 = malloc(sizeof(asql_where));

		GET_NEXT_TOKEN_OR_GOTO(filter_error);
		if (!parse_name(tknzr->tok, &(*where2)->ibname, false))
		{
			return false;
		}

		GET_NEXT_TOKEN_OR_GOTO(filter_error);
		if (strcmp(tknzr->tok, "="))
		{
			return false;
		}

		GET_NEXT_TOKEN_OR_GOTO(filter_error);
		if (parse_expression(tknzr, &(*where2)->beg) != 0)
		{
			return false;
		}

		(*where2)->end = (*where2)->beg;
		(*where2)->qtype = ASQL_QUERY_TYPE_EQUALITY;

	} else if (!strcasecmp(tknzr->tok, "BETWEEN")) { // Range Lookup
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

	char* peek = peek_next_token(tknzr);
	if (peek != NULL && !strcasecmp(peek, "and")) {
		free(peek);
		return false;
	}
	free(peek);

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

static bool
parse_limit(tokenizer* tknzr, asql_value **value) 
{
	if (strcasecmp(tknzr->tok, "LIMIT")) {
		return false;
	}

	GET_NEXT_TOKEN_OR_RETURN(false);
	
	*value = malloc(sizeof(asql_value));

	if (parse_expression(tknzr, *value)) {
		asql_free_value(*value);
		*value = NULL;
		return false;
	}

	if ((*value)->type != AS_INTEGER) {
		return false;
	}

	return true;
}

static aconfig* parse_query(tokenizer* tknzr, int type)
{
	asql_name ns = NULL;
	asql_name set = NULL;
	asql_name udfpkg = NULL;
	asql_name udfname = NULL;
	asql_name itype = NULL;
	asql_name ibname = NULL;
	as_vector* bnames = NULL;
	as_vector* params = NULL;
	asql_value* limit = NULL;

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
	if (tknzr->tok && !strcasecmp(tknzr->tok, "LIMIT") && !parse_limit(tknzr, &limit))
		goto ERROR;
	if (limit)
		get_next_token(tknzr);

	if (!tknzr->tok) {
		scan_config* s = malloc(sizeof(scan_config));
		bzero(s, sizeof(scan_config));
		s->optype = type;
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
		s->limit = limit;
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
		p->optype = type;
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
	s->optype = type;
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

	if (!parse_skey(tknzr, &s->where, &s->where2)) {
		get_next_token(tknzr);
		
		/*
		 * If someone tried to type "BETWEEN a = 1 and b = 3 and . . ." or if 
		 * there was an error parsing the double where clause like 
		 * "a = 1 and b != 3" (!= not supported).
		 */
		if (tknzr->tok && ((!strcasecmp(tknzr->tok, "AND") && s->where.qtype != ASQL_QUERY_TYPE_NONE) || s->where2 != NULL)) {
			fprintf(stderr, "Unsupported command format\n");
			fprintf(stderr, "Double where clause only supports '=' expressions.\n");
		} else {
			predicting_parse_error(tknzr);
			destroy_aconfig((aconfig*)s);
		}
		return NULL;
	}

	if ((s->itype && s->where2)) {
		fprintf(stderr, "Unsupported command format\n");
		fprintf(stderr, "\"IN <indextype>\" not supported with double where clause.\n");	
	}

	s->limit = limit;

	GET_NEXT_TOKEN_OR_RETURN((aconfig *)s;)

	// limit could have been set by previous attempts to parse hence the NULL check. 
	// This is not the documented way of setting the limit but still possible.
	if (s->limit == NULL && !parse_limit(tknzr, &s->limit))
	{
		predicting_parse_error(tknzr);
		destroy_aconfig((aconfig*)s);
		return NULL;
	}

	GET_NEXT_TOKEN_OR_RETURN((aconfig *)s;)

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

	asql_free_value(limit);

	return NULL;
}

static aconfig*
parse_show_info(tokenizer* tknzr)
{
	info_config* i = NULL;

	if (!strcasecmp(tknzr->tok, "NAMESPACES")) {
		i = asql_info_config_create(ASQL_OP_SHOW, strdup("namespaces"), NULL, false);
	}
	else if (!strcasecmp(tknzr->tok, "SETS")) {
		i = asql_info_config_create(ASQL_OP_SHOW, strdup("sets"), NULL, false);
	}
	else if (!strcasecmp(tknzr->tok, "BINS")) {
		i = asql_info_config_create(ASQL_OP_SHOW, strdup("bins"), NULL, false);
	}
	else if (!strcasecmp(tknzr->tok, "PACKAGES")
	        || !strcasecmp(tknzr->tok, "MODULES")) {
		i = asql_info_config_create(ASQL_OP_SHOW, strdup("udf-list"), NULL, false);
	}
	else if (!strcasecmp(tknzr->tok, "INDEXES"))
	{
		get_next_token(tknzr);
		if (!tknzr->tok) {
			i = asql_info_config_create(ASQL_OP_SHOW, strdup("sindex-list:"), NULL, false);
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

			i = asql_info_config_create(ASQL_OP_SHOW, strdup(infocmd), NULL, false);

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
