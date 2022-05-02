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

#pragma once

//==========================================================
// Includes.
//

#include <stdio.h>

#include <sql-lexer.h>


//==========================================================
// Typedefs & Constants.
//

typedef struct tokenizer
{
  char* tok;
  char* ocmd;
} tokenizer;


//=========================================================
// Inlines and Macros.
//

static inline void
get_next_token (tokenizer* tknzr)
{
	as_sql_lexer(0, &tknzr->tok);
}


//==========================================================
// Public API.
//

void init_tokenizer (tokenizer* tknzr, char* cmd);
void destroy_tokenizer (tokenizer* tknzr);
void predicting_parse_error (tokenizer* tknzr);


