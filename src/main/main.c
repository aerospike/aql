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
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>

#include <readline/readline.h>
#include <readline/history.h>

#include <aerospike/as_log_macros.h>
#include <aerospike/as_scan.h>
#include <citrusleaf/cf_b64.h>

#include "asql.h"
#include "asql_conf.h"
#include "asql_print.h"

#include "renderer/table.h"
#include "renderer/json_renderer.h"
#include "renderer/no_renderer.h"
#include "renderer/raw_renderer.h"


//==========================================================
// Typedefs & constants.
//

#define ASQL_HISTORY_FILE ".aql_history"
#define ASQL_HISTORY_MAXLINES 1000
#define TLS_KEYFILE_PASSWORD_MAX_LEN 1024


//=========================================================
// Globals.
//

char* g_prompt = "aql> ";
static aerospike s_aerospike;
bool g_inprogress = false;
asql_config* g_config = NULL;
aerospike* g_aerospike = &s_aerospike;
renderer* g_renderer = &table_renderer;


//=========================================================
// Forward Declarations.
//

static void do_single(asql_config* c, char* cmd);
static void do_file(asql_config* c, char* fname);
static void do_prompt(asql_config* c);

static bool asql_init(asql_config* c);
static void asql_shutdown(asql_config* c);
static void sig_hdlr(int sig_num);
static void sig_hdlr_init();
static bool client_log_cb(as_log_level level, const char* func, const char* file, uint32_t line, const char* fmt, ...);

static bool password_has_source(const char* value);
static bool password_literal_fits(const char* opt, const char* pw, size_t max_len);
static bool read_password(const char* opt, char** ptr, size_t max_len, bool first_line);
static void add_tls_host(asql_config* c, as_config* config);

//=========================================================
// Inline and Macros.
//

#define ASQL_SET_OPTION_BOOL(var, name, help, val) {ASQL_SET_OPTION_TYPE_BOOL, offsetof(struct asql_config, var), name, help, .default_value=val}
#define ASQL_SET_OPTION_INT(var, name, help, val) {ASQL_SET_OPTION_TYPE_INT, offsetof(struct asql_config, var), name, help, .default_value=val}
#define ASQL_SET_OPTION_ENUM(var, name, map, val) {ASQL_SET_OPTION_TYPE_ENUM, offsetof(struct asql_config, var), name, .enum_map=map, .default_value=val}
#define ASQL_SET_OPTION_STRING(var, name, help, val, fn) {ASQL_SET_OPTION_TYPE_STRING, offsetof(struct asql_config, var), name, help, .default_string=strdup(val), .validate=fn}


//=========================================================
// Main.
//


int
main(int argc, char** argv)
{
	map_enum_string output_t_map[] = {
		{TABLE, "TABLE"},
		{JSON, "JSON"},
		{MUTE, "MUTE"},
		{RAW, "RAW"},
		{0, NULL}
	};

	asql_set_option asql_set_option_table[] = {
		// General set options, also available at command line.
		ASQL_SET_OPTION_BOOL(base.echo, "ECHO", NULL, false),
		ASQL_SET_OPTION_BOOL(base.verbose, "VERBOSE", NULL, false),
		// C client thread pool size (negative value means use the default.)
		ASQL_SET_OPTION_ENUM(base.outputmode, "OUTPUT", output_t_map, TABLE),
		ASQL_SET_OPTION_BOOL(base.outputtypes, "OUTPUT_TYPES",	NULL, true),
		ASQL_SET_OPTION_INT(base.timeout_ms, "TIMEOUT", "time in ms", 1000),
		ASQL_SET_OPTION_INT(base.socket_timeout_ms, "SOCKET_TIMEOUT", "time in ms", -1),
		ASQL_SET_OPTION_STRING(base.lua_userpath, "LUA_USERPATH", "<path>", "/opt/aerospike/usr/udf/lua", NULL),

		// Operation specific set options, not available at command line.
		ASQL_SET_OPTION_INT(record_ttl_sec, "RECORD_TTL", "time in sec", 0),
		ASQL_SET_OPTION_BOOL(record_print_metadata, "RECORD_PRINT_METADATA", "prints record metadata", false),
		ASQL_SET_OPTION_BOOL(key_send, "KEY_SEND", NULL, true),
		ASQL_SET_OPTION_BOOL(durable_delete, "DURABLE_DELETE", NULL, false),
		ASQL_SET_OPTION_INT(scan_records_per_second, "SCAN_RECORDS_PER_SECOND", "Limit returned records per second (rps) rate for each server", 0),
		ASQL_SET_OPTION_BOOL(no_bins, "NO_BINS", "No bins as part of scan and query result", false),

		{.offset=-1}
	};
	
	asql_config conf;

	// INIT Base Config
	memset(&conf.base, 0, sizeof(asql_base_config));
	
	option_init(&conf, asql_set_option_table);

	// Overlay command line over conf loaded from file
	char* fname = NULL;
	char* cmd = NULL;
	bool print_only = false;

	if (! config_init(&conf, argc, argv, &cmd, &fname, &print_only)) {
		return -1;
	}

	if (print_only) {
		// Already displayed required information like help and version
		return 0;
	}

	g_config = &conf;
	
	if (! asql_init(g_config)) {
		config_free(&conf);
		return -1;
	}

	if (cmd) {
		do_single(g_config, cmd);
	}
	else if (fname) {
		do_file(g_config, fname);
	}
	else {
		print_version();
		do_prompt(g_config);
	}

	option_free(asql_set_option_table);
	config_free(&conf);

	asql_shutdown(g_config);
	return 0;
}


//=========================================================
// Local API.
//

static void
write_cmd_history()
{
	char hist_fname[128];
	snprintf(hist_fname, 127, "%s/%s", getenv("HOME"), ASQL_HISTORY_FILE);
	int ret = write_history(hist_fname);
	if (ret) {
		as_log_info("History Not logged Error : %d", ret)
	}
}

static void
read_cmd_history()
{
	char hist_fname[128];
	snprintf(hist_fname, 127, "%s/%s", getenv("HOME"), ASQL_HISTORY_FILE);
	history_truncate_file(hist_fname, ASQL_HISTORY_MAXLINES);
	read_history(hist_fname);
}

static void
do_single(asql_config* c, char* cmd)
{
	c->base.echo = true;
	parse_and_run_colon_delim(c, cmd);
}

static void
do_file(asql_config* c, char* fname)
{
	char* cmd = malloc(snprintf(NULL, 0, "RUN '%s'", fname) + 1);
	sprintf(cmd, "RUN '%s'", fname);
	parse_and_run(c, cmd);
	free(cmd);
}

static void
do_prompt(asql_config* c)
{
	bool next = true;

	while (next) {

		char* cmd = readline(g_prompt);

		if (!cmd) {
			break;
		}

		g_inprogress = true;

		add_history(cmd);

		if (!parse_and_run_colon_delim(c, (char* )cmd)) {
			free(cmd);
			break;
		}
		g_inprogress = false;

		free(cmd);
	}
}

bool
asql_init(asql_config* c)
{
	sig_hdlr_init();

	as_config config;
	as_config_init(&config);

	if (! c->base.host) {
		fprintf(stderr, "Error -1: Not able to connect any cluster with %s\n",
				c->base.host);
		return false;
	}


	if (! as_config_add_hosts(&config, c->base.host, c->base.port)) {
		printf("Invalid host(s) %s\n", c->base.host);
		return false;
	}

	if (c->base.tls_name) {
		add_tls_host(c, &config);
	}


	bool pki = c->base.auth_mode && strcasecmp(c->base.auth_mode, "PKI") == 0;
	const char* password_opt = c->base.password_from_config ?
			"password in config file" : "--password";

	// User name NULL means attempt to connect to 
	// insecure cluster.
	if (c->base.user) {
		// Prompt after first screen
		if (strcmp(c->base.password, DEFAULTPASSWORD) == 0) {
			free(c->base.password);
			c->base.password = strdup(getpass("Enter Password: "));

			if (! pki && ! password_literal_fits(password_opt, c->base.password, AS_PASSWORD_SIZE - 1)) {
				return false;
			}
		}
		else if (! pki && ! read_password(password_opt, &c->base.password, AS_PASSWORD_SIZE - 1, false)) {
			return false;
		}

		if (! as_config_set_user(&config, c->base.user, c->base.password)) {
			printf("Invalid password for user name `%s`\n", c->base.user);
			return false;
		}
	}
	else if (! pki && password_has_source(c->base.password)) {
		fprintf(stderr, "Warning: %s: ignored because no user is set\n", password_opt);
	}

	strcpy(config.lua.user_path, c->base.lua_userpath);
	config.conn_timeout_ms = c->base.timeout_ms;
	config.fail_if_not_connected = true;
	config.use_services_alternate = c->base.use_services_alternate;

	// (A negative value means use the C client default.)
	if (c->base.threadpoolsize >= 0) {
		config.thread_pool_size = c->base.threadpoolsize;
	}

	if (c->base.auth_mode && ! as_auth_mode_from_string(&config.auth_mode, c->base.auth_mode)) {
		fprintf(stderr, "Invalid authentication mode %s. Allowed values are INTERNAL / EXTERNAL / EXTERNAL_INSECURE / PKI\n",
				c->base.auth_mode);
		return false;
	}

	if (c->base.tls.keyfile && c->base.tls.keyfile_pw) {
		const char* keyfile_pw_opt = c->base.tls_keyfile_pw_from_config ?
				"tls-keyfile-password in config file" : "--tls-keyfile-password";

		if (strcmp(c->base.tls.keyfile_pw, DEFAULTPASSWORD) == 0) {
			free(c->base.tls.keyfile_pw);
			c->base.tls.keyfile_pw = strdup(getpass("Enter TLS-Keyfile Password: "));
		}

		if (! read_password(keyfile_pw_opt, &c->base.tls.keyfile_pw, TLS_KEYFILE_PASSWORD_MAX_LEN, true)) {
			return false;
		}
	}

	// Transfer ownership of all heap allocated
	// TLS fields via shallow copy.
	memcpy(&config.tls, &c->base.tls, sizeof(as_config_tls));

	aerospike_init(g_aerospike, &config);

	if (c->base.verbose) {
		as_log_set_level(AS_LOG_LEVEL_DEBUG);
	}
	else {
		as_log_set_level(AS_LOG_LEVEL_WARN);
	}

	// Set a callback for all c-sdk outputs
	// to be logged in stderr via a callback.
	as_log_set_callback(client_log_cb);

	as_error err;

	aerospike_connect(g_aerospike, &err);

	if (err.code != AEROSPIKE_OK) {
		fprintf(stderr, "Error %d: %s\n", err.code, err.message);
		return false;
	}

	read_cmd_history();

	return true;
}

static void
asql_shutdown(asql_config* c)
{
	write_cmd_history();

	as_error err;
	aerospike_close(g_aerospike, &err);

	if (err.code != AEROSPIKE_OK) {
		g_renderer->render_error(err.code, err.message, NULL);
		return;
	}

	aerospike_destroy(g_aerospike);
	fprintf(stdout, "\n");
}

static void
sig_hdlr(int sig_num)
{
	if (sig_num == SIGPIPE)
		return;
	if (!g_inprogress) {
		as_log_info("Ctrl-C -- exit!")
		asql_shutdown(g_config);
		exit(-1);
	}
}

static void
sig_hdlr_init()
{
	signal(SIGINT, sig_hdlr);
	signal(SIGTERM, sig_hdlr);
	signal(SIGPIPE, sig_hdlr);
}

static bool
client_log_cb(as_log_level level, const char* func, const char* file,
                       uint32_t line, const char* fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);

	// Write message all at once so messages
	// generated from multiple threads have
	// less of a chance of getting garbled.
	char fmtbuf[1024];
	time_t now = time(NULL);
	struct tm* t = localtime(&now);
	int len = sprintf(fmtbuf, "%d-%02d-%02d %02d:%02d:%02d %s ",
	                  t->tm_year + 1900, t->tm_mon + 1, t->tm_mday, t->tm_hour,
	                  t->tm_min, t->tm_sec, as_log_level_tostring(level));
	char* p = stpcpy(fmtbuf + len, fmt);
	*p++ = '\n';
	*p = 0;

	vfprintf(stderr, fmtbuf, ap);

	va_end(ap);
	return true;
}

static char*
password_env(const char* opt, const char* var)
{
	const char* pw = getenv(var);

	if (pw == NULL || pw[0] == 0) {
		fprintf(stderr, "%s: environment variable %s is not set or empty\n",
				opt, var);
		return NULL;
	}

	char* copy = strdup(pw);

	if (copy == NULL) {
		fprintf(stderr, "%s: out of memory reading environment variable %s\n",
				opt, var);
	}

	return copy;
}

static char*
password_file(const char* opt, const char* path, size_t max_len, bool first_line)
{
	FILE* fh = fopen(path, "r");

	if (fh == NULL) {
		fprintf(stderr, "%s: cannot read file %s: %s\n", opt, path,
				strerror(errno));
		return NULL;
	}

	// Room for a trailing \r\n and one byte more, so an over-long value is
	// caught without reading the rest of the file.
	size_t cap = max_len + 3;
	char* pw = malloc(cap + 1);

	if (pw == NULL) {
		fprintf(stderr, "%s: out of memory reading file %s\n", opt, path);
		fclose(fh);
		return NULL;
	}

	size_t len = 0;
	int c = EOF;

	while (len < cap && (c = getc(fh)) != EOF && c != 0) {
		// --tls-keyfile-password has always used only the first line.
		if (first_line && (c == '\n' || c == '\r')) {
			break;
		}

		pw[len++] = (char)c;
	}

	int err = errno;
	bool failed = ferror(fh) != 0;

	fclose(fh);

	if (failed) {
		fprintf(stderr, "%s: cannot read file %s: %s\n", opt, path,
				strerror(err));
		free(pw);
		return NULL;
	}

	if (c == 0) {
		fprintf(stderr, "%s: file %s contains a NUL byte\n", opt, path);
		free(pw);
		return NULL;
	}

	if (len != 0 && pw[len - 1] == '\n') {
		len--;

		if (len != 0 && pw[len - 1] == '\r') {
			len--;
		}
	}

	pw[len] = 0;

	if (len == 0) {
		fprintf(stderr, "%s: password from %sfile %s is empty\n", opt,
				first_line && c != EOF ? "first line of " : "", path);
		free(pw);
		return NULL;
	}

	return pw;
}

static char*
password_b64(const char* opt, const char* in, const char* what, const char* name)
{
	char* pw = malloc(strlen(in) + 1);

	if (pw == NULL) {
		fprintf(stderr, "%s: out of memory decoding %s%s\n", opt, what, name);
		return NULL;
	}

	uint32_t len = 0;

	// Match Go's base64 decoder, which skips CR and LF.
	for (const char* p = in; *p != 0; p++) {
		if (*p != '\n' && *p != '\r') {
			pw[len++] = *p;
		}
	}

	uint32_t pw_len;

	if (! cf_b64_validate_and_decode_in_place((uint8_t*)pw, len, &pw_len)) {
		fprintf(stderr, "%s: invalid base64 in %s%s\n", opt, what, name);
		free(pw);
		return NULL;
	}

	if (memchr(pw, 0, pw_len) != NULL) {
		fprintf(stderr, "%s: %s%s decodes to a value containing a NUL byte\n",
				opt, what, name);
		free(pw);
		return NULL;
	}

	if (pw_len != 0 && pw[pw_len - 1] == '\n') {
		pw_len--;
	}

	pw[pw_len] = 0;

	if (pw_len == 0) {
		fprintf(stderr, "%s: password from %s%s is empty\n", opt, what, name);
		free(pw);
		return NULL;
	}

	return pw;
}

static char*
password_fit(const char* opt, char* pw, size_t max_len, const char* what, const char* name)
{
	if (strlen(pw) <= max_len) {
		return pw;
	}

	fprintf(stderr, "%s: %s%s is longer than %zu bytes\n", opt, what, name, max_len);
	free(pw);
	return NULL;
}

static bool
password_literal_fits(const char* opt, const char* pw, size_t max_len)
{
	if (strlen(pw) <= max_len) {
		return true;
	}

	fprintf(stderr, "%s: password is longer than %zu bytes\n", opt, max_len);
	return false;
}

static bool
password_has_source(const char* value)
{
	return strncmp(value, "env:", 4) == 0 || strncmp(value, "env-b64:", 8) == 0 ||
			strncmp(value, "b64:", 4) == 0 || strncmp(value, "file:", 5) == 0;
}

static bool
read_password(const char* opt, char** ptr, size_t max_len, bool first_line)
{
	const char* value = *ptr;
	char* pw;

	if (strncmp(value, "env:", 4) == 0) {
		const char* var = value + 4;

		if (*var == 0) {
			fprintf(stderr, "%s: env: requires a variable name\n", opt);
			return false;
		}

		if ((pw = password_env(opt, var)) == NULL ||
				(pw = password_fit(opt, pw, max_len, "value from environment variable ", var)) == NULL) {
			return false;
		}
	}
	else if (strncmp(value, "env-b64:", 8) == 0) {
		const char* var = value + 8;

		if (*var == 0) {
			fprintf(stderr, "%s: env-b64: requires a variable name\n", opt);
			return false;
		}

		char* b64 = password_env(opt, var);

		if (b64 == NULL) {
			return false;
		}

		pw = password_b64(opt, b64, "environment variable ", var);
		free(b64);

		if (pw == NULL ||
				(pw = password_fit(opt, pw, max_len, "decoded value from environment variable ", var)) == NULL) {
			return false;
		}
	}
	else if (strncmp(value, "b64:", 4) == 0) {
		if ((pw = password_b64(opt, value + 4, "b64: value", "")) == NULL ||
				(pw = password_fit(opt, pw, max_len, "decoded b64: value", "")) == NULL) {
			return false;
		}
	}
	else if (strncmp(value, "file:", 5) == 0) {
		const char* path = value + 5;

		if (*path == 0) {
			fprintf(stderr, "%s: file: requires a path\n", opt);
			return false;
		}

		if ((pw = password_file(opt, path, max_len, first_line)) == NULL ||
				(pw = password_fit(opt, pw, max_len, "value from file ", path)) == NULL) {
			return false;
		}
	}
	else {
		return password_literal_fits(opt, value, max_len);
	}

	free(*ptr);
	*ptr = pw;
	return true;
}

static void
add_tls_host(asql_config* c, as_config* config) {
	as_host* config_p;
	int num_hosts = config->hosts->capacity;
	int tls_name_length = strlen(c->base.tls_name);

	for (int i = 0; i < num_hosts; i++) {
		config_p = (as_host*)as_vector_get(config->hosts, i);

		if( !config_p->tls_name) {
		config_p->tls_name = cf_malloc(strlen(c->base.tls_name) + 1);
		memcpy(config_p->tls_name, c->base.tls_name, tls_name_length);
		config_p->tls_name[tls_name_length] = 0;
		}
	}
}
