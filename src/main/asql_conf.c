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
#include <limits.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <arpa/inet.h>

#include <aerospike/mod_lua.h>
#include <aerospike/mod_lua_config.h>

#include <asql.h>
#include "asql_print.h"
#include "toml.h"

#include "renderer/json_renderer.h"
#include "renderer/no_renderer.h"
#include "renderer/raw_renderer.h"
#include "renderer/table.h"



//==========================================================
// Typedefs & constants.
//

#define ASQL_CONFIG_FILE ".aerospike/astools.conf"
#define ERR_BUF_SIZE 1024

#define SA_DEFAULT_HOST "127.0.0.1"
#define SA_DEFAULT_PORT "3005"
#define SA_DEFAULT_TIMEOUT_MS 1000

//=========================================================
// Inline and Macros.
//

#define ASQL_SET_OPTION_IS_VAR(entry, var) ((entry).offset == offsetof(struct asql_config, var))
#define PTR(c, offset) (((char* )(c)) + (offset))

#ifndef MAX
#define MAX(a,b) ((a) > (b) ? (a) : (b))
#endif


//=========================================================
// Globals.
//
char* DEFAULTPASSWORD = "SomeRandomDefaultPassword";
asql_set_option* g_asql_set_option_table;

static struct option options[] =
{
	// Non Config file options
	{"version", no_argument, 0, 'V'},
	{"options", no_argument, 0, 'O'},
	{"help", no_argument, 0, 'E'},
	{"command", required_argument, 0, 'c'},
	{"file", required_argument, 0, 'f'},

	{"instance", required_argument, 0, 'I'},
	{"config-file", required_argument, 0, 'C'},
	{"no-config-file", no_argument, 0, 'N'},
	{"only-config-file", required_argument, 0, 'Y'},

	// Config options
	{"echo", no_argument, 0, 'e'},
	{"verbose", no_argument, 0, 'v'},
	{"host", required_argument, 0, 'h'},
	{"tls-name", required_argument, 0, 1014},
	{"services-alternate", no_argument, 0, 'a'},
	{"port", required_argument, 0, 'p'},
	{"user", required_argument, 0, 'U'},
	{"password", optional_argument, 0, 'P'},
	{"auth", required_argument, 0, 'A'},
	{"tls-enable", no_argument, 0, 1000},
	{"tls-encrypt-only", no_argument, 0, 1001},
	{"tls-cafile", required_argument, 0, 1002},
	{"tls-capath", required_argument, 0, 1003},
	{"tls-protocols", required_argument, 0, 1004},
	{"tls-cipher-suite", required_argument, 0, 1005},
	{"tls-crl-check", no_argument, 0, 1006},
	{"tls-crl-checkall", no_argument, 0, 1007},
	{"tls-cert-blacklist", required_argument, 0, 1008},
	{"tls-keyfile", required_argument, 0, 1010},
	{"tls-certfile", required_argument, 0, 1011},
	{"tls-keyfile-password", optional_argument, 0, 1012},
	{"threadpoolsize", required_argument, 0, 'z'},
	{"outputmode", required_argument, 0, 'o'},
	{"outputtypes", required_argument, 0, 'n'},
	{"timeout", required_argument, 0, 'T'},
	{"socket-timeout", required_argument, 0, 1013},
	{"udfuser", required_argument, 0, 'u'},
	{"sa-address", required_argument, 0, 1015},
	{"sa-port", required_argument, 0, 1016},
	{"sa-timeout", required_argument, 0, 1017},
	{"sa-cafile", required_argument, 0, 1018},

	// Legacy
	{"tlsEnable", no_argument, 0, 1000},
	{"tlsEncryptOnly", no_argument, 0, 1001},
	{"tlsCaFile", required_argument, 0, 1002},
	{"tlsCaPath", required_argument, 0, 1003},
	{"tlsProtocols", required_argument, 0, 1004},
	{"tlsCipherSuite", required_argument, 0, 1005},
	{"tlsCrlCheck", no_argument, 0, 1006},
	{"tlsCrlCheckAll", no_argument, 0, 1007},
	{"tlsCertBlackList", required_argument, 0, 1008},
	{"tlsLogSessionInfo", no_argument, 0, 1009},
	{"tlsKeyFile", required_argument, 0, 1010},
	{"tlsCertFile", required_argument, 0, 1011},

	{0, 0, 0, 0}
};


//=========================================================
// Forward Declarations.
//
extern void strncpy_and_strip_quotes(char* to, const char* from, size_t size);

static bool print_option(uint16_t i);
static bool set(uint16_t i, char* value);
static char* safe_strdup(char* in, char* val);

static bool sa_parse_timeout(const char* str, int* timeout_ms);
static const char* sa_apply(asql_sa_config* sa, const char* address, const char* port);

static bool config_str(toml_table_t* curtab, const char* name, char** ptr);
static bool config_int(toml_table_t* curtab, const char* name, int* ptr);
static bool config_bool(toml_table_t* curtab, const char* name, bool* ptr);
static bool config_cluster(toml_table_t* conftab, asql_config* c, const char* instance, char errbuf[]);
static bool config_aql(toml_table_t* conftab, asql_config* c, const char* instance, char errbuf[]);
static bool config_secret_agent(toml_table_t* conftab, asql_config* c, const char* instance, char errbuf[]);
static bool config_include(toml_table_t* conftab, asql_config* c, const char* instance, int level);

static bool config_parse_file(const char* fname, toml_table_t** tab, char errbuf[]);

static bool config_from_dir(asql_config* c, const char* instance, char* dirname, int level);
static bool config_from_file(asql_config* c, const char* instance, const char* fname, int level);
static bool config_from_files(asql_config* c, const char* instance, const char* cmd_config_fname);
static void config_default(asql_config* c, const char* instance);

// DEBUG
// static void print_base_config(asql_config* c);
//=========================================================
// Public API.
//

void
print_config_file_option()
{
	fprintf(stdout, "\n");
	fprintf(stdout, "Configuration File Allowed Options\n");
	fprintf(stdout, "----------------------------------\n\n");
	fprintf(stdout, "[cluster]\n");
	fprintf(stdout, " -h, --host=HOST\n");
	fprintf(stdout, "                      HOST is \"<host1>[:<tlsname1>][:<port1>],...\" \n");
	fprintf(stdout, "                      Server seed hostnames or IP addresses. The tlsname is \n");
	fprintf(stdout, "                      only used when connecting with a secure TLS enabled \n");
	fprintf(stdout, "                      server. Default: localhost:3000\n");
	fprintf(stdout, "                      Examples:\n");
	fprintf(stdout, "                        host1\n");
	fprintf(stdout, "                        host1:3000,host2:3000\n");
	fprintf(stdout, "                        192.168.1.10:cert1:3000,192.168.1.20:cert2:3000\n");
	fprintf(stdout, " --services-alternate\n");
	fprintf(stdout, "                      Use to connect to alternate access address when the \n");
	fprintf(stdout, "                      cluster's nodes publish IP addresses through access-address \n");
	fprintf(stdout, "                      which are not accessible over WAN and alternate IP addresses \n");
	fprintf(stdout, "                      accessible over WAN through alternate-access-address. Default: false.\n");
	fprintf(stdout, " -p, --port=PORT Server default port. Default: 3000\n");
	fprintf(stdout, " -U, --user=USER User name used to authenticate with cluster. Default: none\n");
	fprintf(stdout, " -P, --password\n");
	fprintf(stdout, "                      Password used to authenticate with cluster. Default: none\n"
                    "                      It can be one of the following:\n"
                    "                      1) Environment variable: 'env:<VAR>'\n"
                    "                      2) Base64 encoded environment variable: 'env-b64:<VAR>'\n"
                    "                      3) Base64 encoded string: 'b64:<VALUE>'\n"
                    "                      4) File: 'file:<PATH>'\n"
                    "                      5) Aerospike Secret Agent: 'secrets:<resource>:<key>'\n"
                    "                      6) String: 'PASSWORD'\n"
                    "                      User will be prompted on command line if -P specified and no\n"
                    "                      password is given.\n");
	fprintf(stdout, " --auth\n");
	fprintf(stdout, "                      Set authentication mode when user/password is defined. Modes are\n");
	fprintf(stdout, "                      (INTERNAL, EXTERNAL, EXTERNAL_INSECURE, PKI). Default: INTERNAL\n");
	fprintf(stdout, "                      This mode must be set EXTERNAL when using LDAP\n");
	fprintf(stdout, " --tls-enable         Enable TLS on connections. By default TLS is disabled.\n");
	// Deprecated
	//fprintf(stdout, " --tls-encrypt-only   Disable TLS certificate verification.\n");
	fprintf(stdout, " --tls-name=TLS_NAME\n");
	fprintf(stdout, "                      Specify host tls name.\n");
	fprintf(stdout, " --tls-cafile=TLS_CAFILE\n");
	fprintf(stdout, "                      Path to a trusted CA certificate file.\n");
	fprintf(stdout, " --tls-capath=TLS_CAPATH.\n");
	fprintf(stdout, "                      Path to a directory of trusted CA certificates.\n");
	fprintf(stdout, " --tls-protocols=TLS_PROTOCOLS\n");
	fprintf(stdout, "                      Set the TLS protocol selection criteria. This format\n"
                    "                      is the same as Apache's SSLProtocol documented at http\n"
                    "                      s://httpd.apache.org/docs/current/mod/mod_ssl.html#ssl\n"
                    "                      protocol . If not specified the asadm will use ' -all\n"
                    "                      +TLSv1.2' if has support for TLSv1.2,otherwise it will\n"
                    "                      be ' -all +TLSv1'.\n");
	fprintf(stdout, " --tls-cipher-suite=TLS_CIPHER_SUITE\n");
	fprintf(stdout, "                     Set the TLS cipher selection criteria. The format is\n"
                	"                     the same as Open_sSL's Cipher List Format documented\n"
                	"                     at https://www.openssl.org/docs/man1.0.1/apps/ciphers.\n"
                	"                     html\n");
	fprintf(stdout, " --tls-keyfile=TLS_KEYFILE\n");
	fprintf(stdout, "                      Path to the key for mutual authentication (if\n"
                    "                      Aerospike Cluster is supporting it).\n");
	fprintf(stdout, " --tls-keyfile-password=TLS_KEYFILE_PASSWORD\n");
	fprintf(stdout, "                      Password to load protected tls-keyfile.\n"
                    "                      It can be one of the following:\n"
                    "                      1) Environment variable: 'env:<VAR>'\n"
                    "                      2) Base64 encoded environment variable: 'env-b64:<VAR>'\n"
                    "                      3) Base64 encoded string: 'b64:<VALUE>'\n"
                    "                      4) File: 'file:<PATH>'\n"
                    "                      5) Aerospike Secret Agent: 'secrets:<resource>:<key>'\n"
                    "                      6) String: 'PASSWORD'\n"
                    "                      Default: none\n"
                    "                      User will be prompted on command line if --tls-keyfile-password\n"
                    "                      specified and no password is given.\n");
	fprintf(stdout, " --tls-certfile=TLS_CERTFILE <path>\n");
	fprintf(stdout, "                      Path to the chain file for mutual authentication (if\n"
                    "                      Aerospike Cluster is supporting it).\n");
	fprintf(stdout, " --tls-cert-blacklist <path> (DEPRECATED)\n");
	fprintf(stdout, "                      Path to a certificate\n"
					" 					   blacklist file. The file should contain one line for\n"
					"					   each blacklisted certificate. Each line starts with\n" 
					"					   the certificate serial number expressed in hex. Each\n" 
					"					   entry may optionally specify the issuer name of the\n" 
					"					   certificate (serial numbers are only required to be\n" 
					"					   unique per issuer).Example: 867EC87482B2\n"
					"					   /C=US/ST=CA/O=Acme/OU=Engineering/CN=TestChainCA\n");

	fprintf(stdout, " --tls-crl-check      Enable CRL checking for leaf certificate. An error\n"
                	"                      occurs if a valid CRL files cannot be found in\n"
                    "                      tls_capath.\n");
	fprintf(stdout, " --tls-crl-checkall   Enable CRL checking for entire certificate chain. An\n"
                	"                      error occurs if a valid CRL files cannot be found in\n"
                    "                      tls_capath.\n");

	fprintf(stdout, "[aql]\n");
	fprintf(stdout, " -z, --threadpoolsize=count\n");
	fprintf(stdout, "                      Set the number of client threads used to talk to the\n");
  	fprintf(stdout, "                      server. Default: 16\n");
	fprintf(stdout, " -o, --outputmode=mode\n");
	fprintf(stdout, "                      Set the output mode. (json | table | raw | mute)\n");
	fprintf(stdout, "                      Default: table\n");
	fprintf(stdout, " -n, --outputtypes    Disable outputting types for values (e.g., GeoJSON, JSON)\n");
	fprintf(stdout, "                      to distinguish them from generic strings\n");
	fprintf(stdout, " -T, --timeout=ms     Set the timeout (ms) for commands. Default: 1000\n");
	fprintf(stdout, " --socket-timeout=ms  Set the socket idle timeout (ms) for commands.\n");
	fprintf(stdout, "                      Default: same as C client\n");
	fprintf(stdout, "                      Default for scan/query: 30000ms\n");
	fprintf(stdout, "                      Default for other commands: 0 (no socket idle time limit)\n");
	fprintf(stdout, " -u, --udfuser=path   Path to User managed UDF modules.\n");
	fprintf(stdout, "                      Default: /opt/aerospike/usr/udf/lua\n");

	fprintf(stdout, "[secret-agent]\n");
	fprintf(stdout, " Aerospike Secret Agent used for 'secrets:<resource>:<key>' passwords.\n");
	fprintf(stdout, " --sa-address=HOST\n");
	fprintf(stdout, "                      HOST is \"<host>[:<port>]\" or \"[<ipv6>][:<port>]\".\n");
	fprintf(stdout, "                      Secret Agent hostname or IP address.\n");
	fprintf(stdout, "                      Default: 127.0.0.1:3005\n");
	fprintf(stdout, " --sa-port=PORT       Secret Agent port. Overrides a port in --sa-address.\n");
	fprintf(stdout, "                      Default: 3005\n");
	fprintf(stdout, " --sa-timeout=ms      Set the timeout (ms) for Secret Agent requests, 1 or more.\n");
	fprintf(stdout, "                      It does not apply to the TCP connect or the name lookup.\n");
	fprintf(stdout, "                      Default: 1000\n");
	fprintf(stdout, " --sa-cafile=path     Path to a CA certificate file. Enables TLS encryption to\n");
	fprintf(stdout, "                      the Secret Agent, but the agent's certificate is not\n");
	fprintf(stdout, "                      verified. Default: none\n");
}

void
print_config_help(int argc, char* argv[])
{
	print_version();
	fprintf(stdout, "\n");
	fprintf(stdout, "Usage: aql [OPTIONS]\n");
	fprintf(stdout, "------------------------------------------------------------------------------");
	fprintf(stdout, "\n");
	fprintf(stdout, " -V, --version        Print AQL version information.\n");
	fprintf(stdout, " -O, --options        Print command-line options message.\n");
	fprintf(stdout, " -E, --help           Print command-line options message and AQL commands \n");
	fprintf(stdout, "                      documentation.\n");
	fprintf(stdout, " -c, --command=cmd    Execute the specified command.\n");
	fprintf(stdout, " -f, --file=path      Execute the commands in the specified file.\n");


	// Base Config
	fprintf(stdout, " -e, --echo           Enable echoing of commands. Default: disabled\n");
	fprintf(stdout, " -v, --verbose        Enable verbose output. Default: disabled\n");
	print_config_file_option();

	fprintf(stdout, "\n\n");
	fprintf(stdout, "Default configuration files are read from the following files in the given order:\n");
	fprintf(stdout, "/etc/aerospike/astools.conf ~/.aerospike/astools.conf\n");
	fprintf(stdout, "The following sections are read: (cluster aql secret-agent include)\n");
	fprintf(stdout, "The following options effect configuration file behavior\n");
	fprintf(stdout, " --no-config-file \n");
	fprintf(stdout, "                      Do not read any config file. Default: disabled\n");
	fprintf(stdout, " --instance=name\n");
	fprintf(stdout, "                      Section with these instance is read. e.g in case instance `a` is specified\n");
	fprintf(stdout, "                      sections cluster_a, aql_a, secret-agent_a is read.\n");
	fprintf(stdout, " --config-file=path\n");
	fprintf(stdout, "                      Read this file after default configuration file.\n");
	fprintf(stdout, " --only-config-file=path\n");
	fprintf(stdout, "                      Read only this configuration file.\n");
	fprintf(stdout, "\n\n");
}

bool
config_init(asql_config* conf, int argc, char* argv[], char** cmd, char** fname, bool* print_only)
{

	// Check print_only options like help and version
	// If found then print respective information, set print_only to true and return
	int optcase;

	// option string should start with '-' to avoid argv permutation
	// we need same argv sequence in third check to support space separated optional argument value
	while ((optcase = getopt_long(argc, argv,
					"-OVEf:I:C:Nevh:c:p:A:U:P::z:o:n:T:u:St:i",
					options, 0)) != -1) {
		switch (optcase) {
			case 'O':
				print_config_help(argc, argv);
				*print_only = true;
				return true;

			case 'V':
				print_version();
				*print_only = true;
				return true;

			case 'E':
				print_help(NULL, true);
				*print_only = true;
				return true;
		}
	}

	// Read Config file and instance name in case passed
	// as command line option
	char* config_fname = NULL;
	char* instance = NULL;
	bool read_conf_files = true;
	bool read_only_conf_file = false;
	optind = 0;
	while ((optcase = getopt_long(argc, argv,
					"-OVEf:I:C:Nevh:c:p:A:U:P::z:o:n:T:u:s:St:i",
					options, 0)) != -1) {
		switch (optcase) {
			case 'C':
				config_fname = optarg;
				break;

			case 'I':
				instance = optarg;
				break;

			case 'N':
				read_conf_files = false;
				break;

			case 'Y':
				config_fname = optarg;
				read_only_conf_file = true;
				break;

		}
	}

	config_default(conf, instance);

	if (read_conf_files) {
		if (read_only_conf_file) {
			if (! config_from_file(conf, instance, config_fname, 0)) {
				return false;
			}
		} else {
			if (! config_from_files(conf, instance, config_fname)) {
				return false;
			}
		}
	} else {
		if (read_only_conf_file) {
			fprintf(stderr, "--no-config-file and only-config-file are mutually exclusive option. Please enable only one.\n");
			return false;
		}
	}

	// DEBUG
	// print_base_config(conf);
	asql_base_config* base = &conf->base;
	const char* sa_address = NULL;
	const char* sa_port = NULL;

	// Reset to optind (internal variable)
	// to parse all options again
	optind = 0;
	while ((optcase = getopt_long(argc, argv,
					"OVEf:I:C:NY:evh:c:p:A:U:P::z:o:n:T:u:St:i",
					options, 0)) != -1) {
		switch (optcase) {

			// Non Config Options (other than help and version)


			case 'c':
				*cmd = optarg;
				break;

			case 'f':
				*fname = optarg;
				break;

				// ignore instance and config file
				// name in second iteration
			case 'C':
			case 'I':
			case 'N':
			case 'Y':
				break;

				// Config option
			case 'e':
				base->echo = true;
				break;

			case 'v':
				base->verbose = true;
				break;

			case 'h':
				base->host = safe_strdup(base->host, optarg);
				break;

			case 1014:
				base->tls_name = safe_strdup(base->tls_name, optarg);
				break;

			case 'a':
				base->use_services_alternate = true;
				break;

			case 'p':
				base->port = atoi(optarg);
				break;

			case 'U':
				base->user = safe_strdup(base->user, optarg);
				break;

			case 'P':
				if (optarg) {
					base->password =  safe_strdup(base->password, optarg);
				} else {
					if (optind < argc && NULL != argv[optind] && '-' != argv[optind][0] ) {
						// space separated argument value
						base->password = safe_strdup(base->password, argv[optind++]);
					} else {
						// no input value, need to prompt
						base->password = safe_strdup(base->password, DEFAULTPASSWORD);
					}
				}
				break;

			case 'A':
				base->auth_mode = safe_strdup(base->auth_mode, optarg);
				break;

			case 1000:
				base->tls.enable = true;
				break;

			case 1001:
				// encrypt only is deprecated
				break;

			case 1002:
				base->tls.cafile = safe_strdup(base->tls.cafile, optarg);
				break;

			case 1003:
				base->tls.capath = safe_strdup(base->tls.capath, optarg);
				break;

			case 1004:
				base->tls.protocols = safe_strdup(base->tls.protocols, optarg);
				break;

			case 1005:
				base->tls.cipher_suite = safe_strdup(base->tls.cipher_suite,
						optarg);
				break;

			case 1006:
				base->tls.crl_check = true;
				break;

			case 1007:
				base->tls.crl_check_all = true;
				break;

			case 1008:
				base->tls.cert_blacklist = safe_strdup(
						base->tls.cert_blacklist, optarg);
				fprintf(stderr, "Warning: --tls-cert-blacklist is deprecated and will be removed in the next release.  Use a crl instead.\n\n");
				break;

			case 1010:
				base->tls.keyfile = safe_strdup(base->tls.keyfile, optarg);
				break;

			case 1012:
				if (optarg) {
					base->tls.keyfile_pw = safe_strdup(base->tls.keyfile_pw, optarg);;
				} else {
                                        if (optind < argc && NULL != argv[optind] && '-' != argv[optind][0] ) {
						// space separated argument value
                                                base->tls.keyfile_pw = safe_strdup(base->tls.keyfile_pw, argv[optind++]);
                                        } else {
						// no input value, need to prompt
                                                base->tls.keyfile_pw = safe_strdup(base->tls.keyfile_pw, DEFAULTPASSWORD);
                                        }

				}
				break;

			case 1011:
				base->tls.certfile = safe_strdup(base->tls.certfile, optarg);
				break;

			case 'z':
				base->threadpoolsize = atoi(optarg);
				break;

			case 'o':
				if (!strcasecmp(optarg, "json")) {
					base->outputmode = JSON;
				}
				else if (!strcasecmp(optarg, "raw")) {
					base->outputmode = RAW;
				}
				else if (!strcasecmp(optarg, "mute")) {
					base->outputmode = MUTE;
				}
				else {
					base->outputmode = TABLE;
				}
				break;

			case 'n':
				base->outputtypes = false;
				break;

			case 'T':
				base->timeout_ms = atoi(optarg);
				break;

			case 1013:
				base->socket_timeout_ms = atoi(optarg);
				break;

			case 'u':
				base->lua_userpath = safe_strdup(base->lua_userpath, optarg);
				break;

			case 1015:
				sa_address = optarg;
				break;

			case 1016:
				sa_port = optarg;
				break;

			case 1017:
				if (! sa_parse_timeout(optarg, &base->sa.timeout_ms)) {
					fprintf(stderr, "--sa-timeout: invalid value %s, expected an integer from 1 to %d\n",
							optarg, INT_MAX);
					return false;
				}
				break;

			case 1018:
				base->sa.cafile = safe_strdup(base->sa.cafile, optarg);
				break;

			default:
				print_config_help(argc, argv);
				return false;
		}
	}

	const char* bad_sa_option = sa_apply(&base->sa, sa_address, sa_port);

	if (bad_sa_option) {
		fprintf(stderr, "--%s: invalid value %s\n", bad_sa_option,
				strcmp(bad_sa_option, "sa-port") == 0 ? sa_port : sa_address);
		return false;
	}

	switch (base->outputmode) {
		case JSON:
			g_renderer = &json_renderer;
			break;
		case RAW:
			g_renderer = &raw_renderer;
			break;
		case MUTE:
			g_renderer = &no_renderer;
			break;
		default:
			g_renderer = &table_renderer;
			break;
	}

	// Print Connection statistics only for interactive case.
	if (! *cmd && ! *fname) {
		if (instance) {
			fprintf(stdout, "Instance:     %s\n", instance);
		}
		fprintf(stdout, "Seed:         %s\n", conf->base.host);
		fprintf(stdout, "User:         %s\n", conf->base.user
				? conf->base.user : "None");

		if (read_conf_files) {
			char user_config_fname[128];
			snprintf(user_config_fname, 127, "%s/%s", getenv("HOME"), ASQL_CONFIG_FILE);
			fprintf(stdout, "Config File:  /etc/aerospike/astools.conf %s %s\n",
					user_config_fname, config_fname ? config_fname : "");
		} else {
			fprintf(stdout, "Config File:  None\n");
		}
	}

	return true;
}

void
config_free(asql_config* conf)
{
	asql_base_config* base = &conf->base;

	if (base->host) {
		free(base->host);
	}

	if (base->tls_name) {
		free(base->tls_name);
	}

	if (base->user) {
		free(base->user);
	}

	if (base->password) {
		free(base->password);
	}

	// Tls ownership is passed to as_config.tls so that would
	// free it up under aerospike_destroy.
	if (base->lua_userpath) {
		free(base->lua_userpath);
	}

	free(base->sa.host);
	free(base->sa.port);
	free(base->sa.cafile);
}


void
print_option_help()
{
	const asql_set_option* table = g_asql_set_option_table;
	static const size_t indent_space = 8;
	size_t max_name_len = 0;
	for (size_t i = 0; table[i].offset >= 0; i++) {
		max_name_len = MAX(max_name_len, strlen(table[i].name));
	}

	char fmt[128];
	sprintf(fmt, "%%%zus%%-%zus", indent_space, max_name_len + indent_space);

	for (size_t i = 0; table[i].offset >= 0; i++) {

		const asql_set_option* option = table + i;
		fprintf(stdout, fmt, "", option->name);

		switch (option->type) {
			case ASQL_SET_OPTION_TYPE_BOOL: {
				if (option->help) {
					fprintf(stdout, "(true | false, default %s, %s)\n",
					        option->default_value ? "true": "false",
					        option->help);
				}
				else {
					fprintf(stdout, "(true | false, default %s)\n",
					        option->default_value ? "true": "false");
				}
			}
			break;

			case ASQL_SET_OPTION_TYPE_INT: {
				fprintf(stdout, "(%s, default: %d)\n", option->help,
				        option->default_value);
			}
			break;

			case ASQL_SET_OPTION_TYPE_ENUM: {
				fprintf(stdout, "(");
				for (size_t j = 0; option->enum_map[j].name; j++) {
					if (j > 0) {
						fprintf(stdout, " | ");
					}
					fprintf(stdout, "%s", option->enum_map[j].name);
				}
				fprintf(stdout, ", default %s)\n",
				        map_enum_to_string(option->enum_map,
				                           option->default_value));
			}
			break;

			case ASQL_SET_OPTION_TYPE_STRING: {
				fprintf(stdout, "%s, default : %s\n", option->help,
				        option->default_string);
			}
			break;

			default:
				break;
		}
	}
}

void
option_init(asql_config* c, struct asql_set_option_s* table)
{
	g_asql_set_option_table = table;

	for (size_t i = 0; table[i].offset >= 0; i++) {
		asql_set_option* option = table + i;
		void* ptr = PTR(c, option->offset);
		switch (option->type) {
			case ASQL_SET_OPTION_TYPE_BOOL:
				*((bool*)ptr) = option->default_value;
				break;
			case ASQL_SET_OPTION_TYPE_INT:
			case ASQL_SET_OPTION_TYPE_ENUM:
				*((int*)ptr) = option->default_value;
				break;
			case ASQL_SET_OPTION_TYPE_STRING: {
				*((char**)ptr)= strdup(option->default_string);
			}
				break;
			default:
				break;
		}
	}
}

void
option_free(struct asql_set_option_s* table)
{
	g_asql_set_option_table = table;

	for (size_t i = 0; table[i].offset >= 0; i++) {
		asql_set_option* option = table + i;
		switch (option->type) {
			case ASQL_SET_OPTION_TYPE_STRING: {
				free((char*)option->default_string);
			}
				break;
			default:
				break;
		}
	}
}


// normal automatic config handling
bool
option_set(asql_config* c, char* name, char* value)
{
	const asql_set_option* table = g_asql_set_option_table;
	size_t i = 0;
	for (; table[i].offset > 0; i++) {
		if (strcasecmp(name, table[i].name) != 0) {
			continue;
		}
		break;
	}
	if (table[i].offset < 0) {
		return false;
	}

	if (! set(i, value)) {
		return false;
	}

	// handle side effects if any
	if (ASQL_SET_OPTION_IS_VAR(table[i], base.verbose)) {
		as_log_set_level(c->base.verbose
				? AS_LOG_LEVEL_TRACE
				: AS_LOG_LEVEL_INFO);
	}
	else if (ASQL_SET_OPTION_IS_VAR(table[i], base.outputmode)) {
		if (c->base.outputmode == JSON) {
			g_renderer = &json_renderer;
		}
		else if (c->base.outputmode == TABLE) {
			g_renderer = &table_renderer;
		}
		else if (c->base.outputmode == RAW) {
			g_renderer = &raw_renderer;
		}
		else if (c->base.outputmode == MUTE) {
			g_renderer = &no_renderer;
		}
	}
	else if (ASQL_SET_OPTION_IS_VAR(table[i], base.lua_userpath)) {

		as_config_lua lua;
		as_config_lua_init(&lua);

		strncpy_and_strip_quotes(lua.user_path, c->base.lua_userpath,
				sizeof(lua.user_path));

		aerospike_init_lua(&lua);
	}
	print_option(i);
	return true;
}

bool
option_reset(asql_config* c, char* name)
{
	const asql_set_option* table = g_asql_set_option_table;
	size_t i = 0;
	for (; table[i].offset >= 0; i++) {
		if (strcasecmp(name, table[i].name) != 0) {
			continue;
		}
		break;
	}
	if (table[i].offset < 0) {
		return false;
	}

	void* ptr = PTR(g_config, table[i].offset);
	switch (table[i].type) {
		case ASQL_SET_OPTION_TYPE_BOOL:
			*((bool*)ptr) = table[i].default_value;
			break;
		case ASQL_SET_OPTION_TYPE_INT:
			*((int*)ptr) = table[i].default_value;
			break;
		case ASQL_SET_OPTION_TYPE_ENUM:
			*((int*)ptr) = table[i].default_value;
			break;
		case ASQL_SET_OPTION_TYPE_STRING:
			free(*((char**)ptr));
			*((char**)ptr) = strdup(table[i].default_string);
			break;
		default:
			return false;
	}

	print_option(i);
	return true;
}

bool
option_get(asql_config* c, char* name)
{
	const asql_set_option* table = g_asql_set_option_table;

	if (!strcasecmp(name, "all")) {
		size_t i = 0;
		for (; table[i].offset >= 0; i++) {
			print_option(i);
		}
		return true;
	}

	size_t i = 0;
	for (; table[i].offset >= 0; i++) {
		if (strcasecmp(name, table[i].name) != 0) {
			continue;
		}
		break;
	}

	if (table[i].offset < 0) {
		return false;
	}

	print_option(i);
	return true;
}


//=========================================================
// Local API.
//

static bool
print_option(uint16_t i)
{
	const asql_set_option* table = g_asql_set_option_table;
	void* ptr = PTR(g_config, table[i].offset);
	fprintf(stdout, "%s = ", table[i].name);
	switch (table[i].type) {
		case ASQL_SET_OPTION_TYPE_BOOL: {
			fprintf(stdout, "%s\n", *((bool*)ptr) ? "true": "false");
		}
			break;
		case ASQL_SET_OPTION_TYPE_INT: {
			fprintf(stdout, "%d\n", *((int*)ptr));
		}
			break;
		case ASQL_SET_OPTION_TYPE_ENUM: {
			fprintf(stdout, "%s\n", map_enum_to_string(table[i].enum_map,
			                          *(int*)ptr));
		}
			break;
		case ASQL_SET_OPTION_TYPE_STRING: {
			fprintf(stdout, "%s\n", *((char**)ptr));
		}
			break;
		default:
			return false;
	}
	return true;
}

static bool
set(uint16_t i, char* value)
{
	const asql_set_option* table = g_asql_set_option_table;
	void* ptr = PTR(g_config, table[i].offset);
	switch (table[i].type) {
		case ASQL_SET_OPTION_TYPE_BOOL: {
			if (!strcasecmp(value, "TRUE") || !strcasecmp(value, "T")
			        || !strcasecmp(value, "1")) {
				*((bool*)ptr) = true;
			}
			else if (!strcasecmp(value, "FALSE")
			        || !strcasecmp(value, "F")
			        || !strcasecmp(value, "0")) {
				*((bool*)ptr) = false;
			}
			else {
				return false;
			}
			break;
		}
		case ASQL_SET_OPTION_TYPE_INT: {
			*((int*)ptr) = (int)atol(value);
			break;
		}
		case ASQL_SET_OPTION_TYPE_ENUM: {
			size_t j = 0;
			for (; table[i].enum_map[j].name; j++) {
				if (!strcasecmp(value, table[i].enum_map[j].name)) {
					*((int*)ptr) = (int)j;
					break;
				}
			}
			if (!table[i].enum_map[j].name) {
				return false;
			}
			break;
		}
		case ASQL_SET_OPTION_TYPE_STRING: {
			if (table[i].validate
					&& !table[i].validate((char*)value)) {
				return false;
			}

			free(*((char**)ptr));
			*((char**)ptr) = strdup(value);
			break;
		}
		default:
			return false;
	}
	return true;
}

static bool
config_str(toml_table_t* curtab, const char* name, char** ptr)
{
	const char* value = toml_raw_in(curtab, name);
	if (! value) {
		return false;
	}

	if (*ptr) {
		free(*ptr);
		*ptr = NULL;
	}

	char* sval;
	if (0 == toml_rtos(value, &sval)) {
		*ptr = sval;
		return true;
	}
	return false;
}

static bool
config_int(toml_table_t* curtab, const char* name, int* ptr)
{
	const char* value = toml_raw_in(curtab, name);
	if (! value) {
		return false;
	}

	int64_t ival;
	if (0 == toml_rtoi(value, &ival)) {
		*ptr = ival;
		return true;
	}
	return false;
}

static bool
config_bool(toml_table_t* curtab, const char* name, bool* ptr)
{
	const char* value = toml_raw_in(curtab, name);
	if (! value) {
		return false;
	}

	int bval;
	if (0 == toml_rtob(value, &bval)) {
		*ptr = bval ? true : false;
		return true;
	}
	return false;
}

static bool
config_aql(toml_table_t* conftab, asql_config* c, const char* instance, char errbuf[])
{
	// Defaults to "aql" section in case present.
	toml_table_t* curtab = toml_table_in(conftab, "aql");

	char aql[256] = {"aql"};
	if (instance) {
		snprintf(aql, 255, "aql_%s", instance);
		// override if it exists otherwise use
		// default section
		if (toml_table_in(conftab, aql)) {
			curtab = toml_table_in(conftab, aql);
		}
	}

	if (! curtab) {
		return true;
	}

	const char* name;
	const char* value;

	for (uint8_t i = 0; 0 != (name = toml_key_in(curtab, i)); i++) {

		value = toml_raw_in(curtab, name);
		if (!value) {
			continue;
		}
		bool status;

		if (! strcasecmp("threadpoolsize", name)) {
			status = config_int(curtab, name, &c->base.threadpoolsize);

		} else if (! strcasecmp("outputmode", name)) {
			char* mode = NULL;
			status = config_str(curtab, name, &mode);
			if (status) {
				if (!strcasecmp(mode, "json")) {
					c->base.outputmode = JSON;
				}
				else if (!strcasecmp(mode, "raw")) {
					c->base.outputmode = RAW;
				}
				else if (!strcasecmp(mode, "mute")) {
					c->base.outputmode = MUTE;
				}
				else {
					c->base.outputmode = TABLE;
				}
				free(mode);
			}

		} else if (! strcasecmp("outputtypes", name)) {
			status = config_bool(curtab, name, &c->base.outputtypes);

		} else if (! strcasecmp("timeout", name)) {
			status = config_int(curtab, name, &c->base.timeout_ms);

		} else if (! strcasecmp("socket-timeout", name)) {
			status = config_int(curtab, name, &c->base.socket_timeout_ms);

		} else if (! strcasecmp("udfuser", name)) {
			status = config_str(curtab, name, &c->base.lua_userpath);

		} else {
			fprintf(stderr, "Unknown parameter `%s` in `%s` section\n", name,
					aql);
			return false;
		}

		if (! status) {
			snprintf(errbuf, ERR_BUF_SIZE, "Invalid parameter value for `%s` in `%s` section\n",
					name, aql);
			return false;
		}
	}
	return true;
}

static bool
config_cluster(toml_table_t* conftab, asql_config* c, const char* instance, char errbuf[])
{
	// Defaults to "cluster" section in case present.
	toml_table_t* curtab = toml_table_in(conftab, "cluster");

	char cluster[256] = {"cluster"};
	if (instance) {
		snprintf(cluster, 255, "cluster_%s", instance);
		// No override for cluster section.
		curtab = toml_table_in(conftab, cluster);
	}

	if (! curtab) {
		return true;
	}

	const char* name;

	for (uint8_t i = 0; 0 != (name = toml_key_in(curtab, i)); i++) {

		bool status;

		if (! strcasecmp("host", name)) {
			status = config_str(curtab, name, &c->base.host);

		} else if (! strcasecmp("services-alternate",  name)) {
			status = config_bool(curtab, name, &c->base.use_services_alternate);

		} else if (! strcasecmp("port", name)) {
			status = config_int(curtab, name, &c->base.port);

		} else if (! strcasecmp("user", name)) {
			status = config_str(curtab, name, &c->base.user);

		} else if (! strcasecmp("password", name)) {
			status = config_str(curtab, name, (void*)&c->base.password);

		} else if (! strcasecmp("auth", name)) {
			status = config_str(curtab, name, &c->base.auth_mode);

		} else if (! strcasecmp("tls-enable", name)) {
			status = config_bool(curtab, name, &c->base.tls.enable);

		} else if (! strcasecmp("tls-name", name)) {
			status = config_str(curtab, name, &c->base.tls_name);

		} else if (! strcasecmp("tls-protocols", name)) {
			status = config_str(curtab, name, &c->base.tls.protocols);

		} else if (! strcasecmp("tls-cipher-suite", name)) {
			status = config_str(curtab, name, &c->base.tls.cipher_suite);

		} else if (! strcasecmp("tls-crl-check", name)) {
			status = config_bool(curtab, name, &c->base.tls.crl_check);

		} else if (! strcasecmp("tls-crl-check-all", name)) {
			status = config_bool(curtab, name, &c->base.tls.crl_check_all);

		} else if (! strcasecmp("tls-keyfile", name)) {
			status = config_str(curtab, name, &c->base.tls.keyfile);

		} else if (! strcasecmp("tls-keyfile-password", name)) {
			status = config_str(curtab, name, (void*)&c->base.tls.keyfile_pw);

		} else if (! strcasecmp("tls-cafile", name)) {
			status = config_str(curtab, name, &c->base.tls.cafile);

		} else if (! strcasecmp("tls-capath", name)) {
			status = config_str(curtab, name, &c->base.tls.capath);

		} else if (! strcasecmp("tls-certfile", name)) {
			status = config_str(curtab, name, &c->base.tls.certfile);

		} else if (! strcasecmp("tls-cert-blacklist", name)) {
			status = config_str(curtab, name, &c->base.tls.cert_blacklist);
			fprintf(stderr, "Warning: --tls-cert-blacklist is deprecated and will be removed in the next release.  Use a crl instead.\n\n");
		} else {
			snprintf(errbuf, ERR_BUF_SIZE, "Unknown parameter `%s` in `%s` section.\n", name,
					cluster);
			return false;
		}

		if (! status) {
			snprintf(errbuf, ERR_BUF_SIZE, "Invalid parameter value for `%s` in `%s` section.\n",
					name, cluster);
			return false;
		}
	}
	return true;
}

static bool
config_sa_port(toml_table_t* curtab, const char* name, char** ptr)
{
	if (config_str(curtab, name, ptr)) {
		return true;
	}

	int64_t ival;

	if (0 != toml_rtoi(toml_raw_in(curtab, name), &ival)) {
		return false;
	}

	char port[32];
	snprintf(port, sizeof(port), "%lld", (long long)ival);
	*ptr = strdup(port);
	return true;
}

static bool
config_sa_timeout(toml_table_t* curtab, const char* name, int* ptr)
{
	int64_t ival;

	if (0 != toml_rtoi(toml_raw_in(curtab, name), &ival) || ival < 1 || ival > INT_MAX) {
		return false;
	}

	*ptr = (int)ival;
	return true;
}

static bool
config_secret_agent(toml_table_t* conftab, asql_config* c, const char* instance, char errbuf[])
{
	// Defaults to "secret-agent" section, replaced by the instance section if it exists.
	char section[256] = {"secret-agent"};
	toml_table_t* curtab = toml_table_in(conftab, section);

	if (instance) {
		char instance_section[256];
		snprintf(instance_section, sizeof(instance_section), "secret-agent_%s", instance);

		if (toml_table_in(conftab, instance_section)) {
			curtab = toml_table_in(conftab, instance_section);
			strcpy(section, instance_section);
		}
	}

	if (! curtab) {
		return true;
	}

	char* address = NULL;
	char* port = NULL;
	const char* name;
	bool status = true;

	for (uint8_t i = 0; status && 0 != (name = toml_key_in(curtab, i)); i++) {

		if (! strcasecmp("sa-address", name)) {
			status = config_str(curtab, name, &address);

		} else if (! strcasecmp("sa-port", name)) {
			status = config_sa_port(curtab, name, &port);

		} else if (! strcasecmp("sa-timeout", name)) {
			status = config_sa_timeout(curtab, name, &c->base.sa.timeout_ms);

		} else if (! strcasecmp("sa-cafile", name)) {
			status = config_str(curtab, name, &c->base.sa.cafile);

		} else {
			snprintf(errbuf, ERR_BUF_SIZE, "Unknown parameter `%s` in `%s` section.\n", name,
					section);
			free(address);
			free(port);
			return false;
		}

		if (! status) {
			snprintf(errbuf, ERR_BUF_SIZE, "Invalid parameter value for `%s` in `%s` section.\n",
					name, section);
		}
	}

	if (status) {
		const char* bad = sa_apply(&c->base.sa, address, port);

		if (bad) {
			snprintf(errbuf, ERR_BUF_SIZE, "Invalid parameter value for `%s` in `%s` section.\n",
					bad, section);
			status = false;
		}
	}

	free(address);
	free(port);
	return status;
}

static bool
config_from_dir(asql_config* c, const char* instance, char* dirname, int level)
{
	DIR* dp;
	struct dirent* entry;

	if ((dp = opendir(dirname)) == NULL) {
		fprintf(stderr, "Failed to open directory %s\n", dirname);
		return false;
	}

	while ((entry = readdir(dp)) != NULL) {

		if (strcmp(".", entry->d_name) == 0
				|| strcmp("..", entry->d_name) == 0) {
			continue;
		}

		char path[strlen(dirname) + 1 + strlen(entry->d_name)];
		sprintf(path, "%s/%s", dirname, entry->d_name);

		struct stat statbuf;
		lstat(path, &statbuf);

		if (S_ISDIR(statbuf.st_mode)) {
			if (! config_from_dir(c, instance, path, level)) {
				// ignore file loading error inside include directory
				fprintf(stderr, "Skipped .....\n");
			}

		} else if (S_ISREG(statbuf.st_mode)) {
			if (! config_from_file(c, instance, path, level)) {
				// ignore file loading error inside include directory
				fprintf(stderr, "Skipped .....\n");
			}
		}
	}

	closedir(dp);
	return true;
}

static bool
config_include(toml_table_t* conftab, asql_config* c, const char* instance, int level)
{
	if (level > 3) {
		fprintf(stderr, "include max recursion level %d", level);
		return false;
	}

	// Get include section
	toml_table_t* curtab = toml_table_in(conftab, "include");
	if (! curtab) {
		return true;
	}

	const char* name;
	for (uint8_t i = 0; 0 != (name = toml_key_in(curtab, i)); i++) {

		bool status;

		if (! strcasecmp("file", name)) {
			char* fname = NULL;
			status = config_str(curtab, name, &fname);

			if (status) {
				if (! config_from_file(c, instance, fname, level + 1)) {
					free(fname);
					return false;
				}
				free(fname);
			}

		} else if (! strcasecmp("directory", name)) {
			char* dirname = NULL;
			status = config_str(curtab, name, &dirname);
			if (status) {
				if (! config_from_dir(c, instance, dirname, level + 1)) {
					free(dirname);
					return false;
				}
				free(dirname);
			}

		} else {
			fprintf(stderr, "Unknown parameter `%s` in `include` section.\n", name);
			return false;
		}

		if (! status) {
			fprintf(stderr, "Invalid parameter value for `%s` in `include` section.\n",
					name);
			return false;
		}
	}
	return true;
}

static bool
config_from_file(asql_config* c, const char* instance, const char* fname, int level)
{
	bool status = true;
	//fprintf(stderr, "Load file %d:%s\n", level, fname);
	toml_table_t* conftab = NULL;

	char errbuf[ERR_BUF_SIZE] = {""};
	if (! config_parse_file((char*)fname, &conftab, errbuf)) {
		status = false;
	}
	else if (! conftab) {
		status = true;
	}
	else if (! config_cluster(conftab, c, instance, errbuf)) {
		status = false;
	}
	else if (! config_aql(conftab, c, instance, errbuf)) {
		status = false;
	}
	else if (! config_secret_agent(conftab, c, instance, errbuf)) {
		status = false;
	}
	else if (! config_include(conftab, c, instance, level)) {
		status = false;
	}

	toml_free(conftab);

	if (! status) {
		fprintf(stderr, "Parse error `%s` in file [%d:%s]\n", errbuf, level, fname);
	}
	return status;
}

static bool
config_parse_file(const char* fname, toml_table_t** tab, char errbuf[])
{
	FILE* fp = fopen(fname, "r");

	if (! fp) {
		// it ok if file is not found
		return true;
	}

	*tab = toml_parse_file(fp, errbuf, ERR_BUF_SIZE);
	fclose(fp);

	if (! *tab) {
		return false;
	}

	return true;
}

static void
config_default(asql_config* c, const char* instance)
{
	// Do not default to any host if instance is
	// specified
	if (! instance) {
		c->base.host = strdup("127.0.0.1");
	}
	c->base.port = 3000;
	c->base.auth_mode = NULL;

	// Non dynamically configuration option
	// -1 is to set it to default
	c->base.threadpoolsize = -1;
	c->base.user = NULL;
	c->base.password = strdup(DEFAULTPASSWORD);

	memset(&c->base.tls, 0, sizeof(as_config_tls));

	c->base.sa.host = strdup(SA_DEFAULT_HOST);
	c->base.sa.port = strdup(SA_DEFAULT_PORT);
	c->base.sa.timeout_ms = SA_DEFAULT_TIMEOUT_MS;
	c->base.sa.cafile = NULL;
}

static bool
config_from_files(asql_config* c, const char* instance,
		const char* cmd_config_fname)
{
	// Load /etc/aerospike/astools.conf
	if (! config_from_file(c, instance, "/etc/aerospike/astools.conf", 0)) {
		return false;
	}

	// Load $HOME/.aerospike/astools.conf
	char user_config_fname[128];
	snprintf(user_config_fname, 127, "%s/%s", getenv("HOME"), ASQL_CONFIG_FILE);
	if (! config_from_file(c, instance, user_config_fname, 0)) {
		return false;
	}

	// Load user passed conf file
	if (cmd_config_fname) {
		if (! config_from_file(c, instance, cmd_config_fname, 0)) {
			return false;
		}
	}
	return true;
}

static char*
safe_strdup(char* in, char* val)
{
	if (in) {
		free(in);
	}
	in = strdup(val);
	return in;
}

static bool
sa_parse_number(const char* str, long max, long* val)
{
	if (str[0] < '0' || str[0] > '9') {
		return false;
	}

	char* end;
	errno = 0;
	long v = strtol(str, &end, 10);

	if (errno != 0 || *end != 0 || v > max) {
		return false;
	}

	*val = v;
	return true;
}

static bool
sa_parse_timeout(const char* str, int* timeout_ms)
{
	long val;

	if (! sa_parse_number(str, INT_MAX, &val) || val == 0) {
		return false;
	}

	*timeout_ms = (int)val;
	return true;
}

static bool
sa_parse_port(const char* str, char** port)
{
	long val;

	if (! sa_parse_number(str, 65535, &val) || val == 0) {
		return false;
	}

	char buf[8];
	snprintf(buf, sizeof(buf), "%ld", val);
	*port = strdup(buf);
	return true;
}

// The zone ("fe80::1%lo0") is checked by the resolver later; glibc's inet_pton rejects it.
static bool
sa_is_ipv6(const char* host, size_t len)
{
	const char* zone = memchr(host, '%', len);
	size_t addr_len = zone != NULL ? (size_t)(zone - host) : len;
	char buf[INET6_ADDRSTRLEN];
	struct in6_addr ipv6;

	if (addr_len >= sizeof(buf) || (zone != NULL && addr_len + 1 == len)) {
		return false;
	}

	memcpy(buf, host, addr_len);
	buf[addr_len] = 0;
	return inet_pton(AF_INET6, buf, &ipv6) == 1;
}

// A bracketed host or one with a colon must be IPv6; unbracketed, a single colon separates the port.
static bool
sa_parse_address(const char* address, char** host, char** port)
{
	bool bracketed = address[0] == '[';
	const char* start = address;
	const char* end;
	const char* port_str = NULL;

	if (bracketed) {
		start = address + 1;
		end = strchr(start, ']');

		if (end == NULL || (end[1] != 0 && end[1] != ':')) {
			return false;
		}

		if (end[1] == ':') {
			port_str = end + 2;
		}
	}
	else {
		const char* colon = strchr(address, ':');
		end = address + strlen(address);

		if (colon != NULL && strchr(colon + 1, ':') == NULL) {
			end = colon;
			port_str = colon + 1;
		}
	}

	if (end == start) {
		return false;
	}

	if ((bracketed || memchr(start, ':', end - start) != NULL) && ! sa_is_ipv6(start, end - start)) {
		return false;
	}

	if (port_str != NULL && ! sa_parse_port(port_str, port)) {
		return false;
	}

	*host = strndup(start, end - start);
	return true;
}

// Applies one source (a config section or the command line); its explicit port beats its address port.
static const char*
sa_apply(asql_sa_config* sa, const char* address, const char* port)
{
	char* new_host = NULL;
	char* addr_port = NULL;
	char* new_port = NULL;

	if (address && ! sa_parse_address(address, &new_host, &addr_port)) {
		return "sa-address";
	}

	if (port && ! sa_parse_port(port, &new_port)) {
		free(new_host);
		free(addr_port);
		return "sa-port";
	}

	if (new_host) {
		free(sa->host);
		sa->host = new_host;
	}

	if (new_port) {
		free(addr_port);
	}
	else {
		new_port = addr_port;
	}

	if (new_port) {
		free(sa->port);
		sa->port = new_port;
	}

	return NULL;
}

#if 0
// DEBUG
static void
print_base_config(asql_config* c)
{
	asql_base_config* base = &c->base;
	fprintf(stderr, "host:%s\n", base->host ? base->host : " ");
	fprintf(stderr, "port:%d\n", base->port);
	fprintf(stderr, "host:%s\n", base->user ? base->user : " ");
	fprintf(stderr, "password:%s\n", base->password ? base->password : " ");

	as_config_tls* tls = &base->tls;

	fprintf(stderr, "tls-enable:%s\n", tls->enable ? "true" : "false");
	fprintf(stderr, "tls-cafile:%s\n", tls->cafile ? tls->cafile : " ");
	fprintf(stderr, "tls-capath:%s\n", tls->capath ? tls->capath : " ");
	fprintf(stderr, "tls-protocols:%s\n", tls->protocols ? tls->protocols : " ");
	fprintf(stderr, "tls-cipher-suite:%s\n", tls->cipher_suite ? tls->cipher_suite : " ");
	fprintf(stderr, "tls-crl-check:%s\n", tls->crl_check ? "true" : "false");
	fprintf(stderr, "tls-crl-checkall:%s\n", tls->crl_check_all ? "true" : "false");
	fprintf(stderr, "tls-cert-blacklist:%s\n", tls->cert_blacklist ? tls->cert_blacklist : " ");
	fprintf(stderr, "tls-certfile:%s\n", tls->certfile ? tls->certfile : " ");
	fprintf(stderr, "tls-keyfile:%s\n", tls->keyfile ? tls->keyfile : " ");
	fprintf(stderr, "tls-keyfile-password:%s\n", tls->keyfile_pw ? tls->keyfile_pw : " ");
}
#endif
