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

#include <string.h>
#include <stdlib.h>

#include <asql_tokenizer.h>


//==========================================================
// Globals.
//
extern void yylex_destroy();


//==========================================================
// Public API.
//

void
init_tokenizer(tokenizer* tknzr, char* cmd)
{
	bzero(tknzr, sizeof(tokenizer));
	tknzr->ocmd = strdup(cmd);
	int rv;
	if ((rv = as_sql_lexer(cmd, &tknzr->tok))) {
//		fprintf(stderr, "Warning:  Lexer returned %d\n", rv);
	}
}

void
destroy_tokenizer(tokenizer* tknzr)
{
	if (tknzr->ocmd) {
		free(tknzr->ocmd);
	}
	yylex_destroy();
}

void
predicting_parse_error(tokenizer* tknzr)
{
	if (!tknzr->tok) {
		fprintf(stderr, "Syntax error near token -  \'%s\' \n", tknzr->ocmd);
		fprintf(stderr, "Make sure string values are enclosed in quotes.\n");
		fprintf(stderr, "Type \" aql --help \" from console or simply \"help\" from within the aql-prompt. \n\n");
	}
	else {
		fprintf(stderr, "Unsupported command format with token -  \'%s\' \n",
		        tknzr->tok);
		fprintf(stderr, "Make sure string values are enclosed in quotes.\n");
		fprintf(stderr, "Type \" aql --help \" from console or simply \"help\" from within the aql-prompt. \n\n");
	}
}
