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

#pragma once

//==========================================================
// Includes.
//

#include <asql_tokenizer.h>

//==========================================================
// Public API.
//

aconfig* aql_parse_insert(tokenizer* tknzr);
aconfig* aql_parse_explain(tokenizer* tknzr);
aconfig* aql_parse_delete(tokenizer* tknzr);
aconfig* aql_parse_truncate(tokenizer* tknzr);
aconfig* aql_parse_execute(tokenizer* tknzr);

aconfig* aql_parse_select(tokenizer* tknzr);
aconfig* aql_parse_aggregate(tokenizer* tknzr);

aconfig* aql_parse_create(tokenizer* tknzr);
aconfig* aql_parse_drop(tokenizer* tknzr);
aconfig* aql_parse_grant(tokenizer* tknzr);
aconfig* aql_parse_revoke(tokenizer* tknzr);
aconfig* aql_parse_registerudf(tokenizer* tknzr);
aconfig* aql_parse_removeudf(tokenizer* tknzr);

aconfig* aql_parse_show(tokenizer* tknzr);
aconfig* aql_parse_desc(tokenizer* tknzr);
aconfig* aql_parse_stat(tokenizer* tknzr);

aconfig* aql_parse_killquery(tokenizer* tknzr);
aconfig* aql_parse_killscan(tokenizer* tknzr);

aconfig* aql_parse_run(tokenizer* tknzr);
aconfig* aql_parse_asinfo(tokenizer* tknzr);

aconfig* aql_parserun_set(tokenizer* tknzr);
aconfig* aql_parserun_get(tokenizer* tknzr);
aconfig* aql_parserun_reset(tokenizer* tknzr);
aconfig* aql_parserun_print(tokenizer* tknzr);
aconfig* aql_parserun_system(tokenizer* tknzr);
