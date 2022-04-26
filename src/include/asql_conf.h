/*
 * Copyright 2019-2022 Aerospike, Inc.
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
// Typedefs & constants.
//

struct asql_config;
struct asql_set_option_s;


//==========================================================
// Public API.
//

bool config_init(asql_config* c, int argc, char *argv[], char** cmd, char** fname, bool* print_only);
bool config_free(asql_config* c);
void print_config_help(int argc, char* argv[]);

void option_init(asql_config* c, struct asql_set_option_s* table);
void option_free(struct asql_set_option_s* table);
bool option_set(struct asql_config* c, char* name, char* value);
bool option_reset(struct asql_config* c, char* name);
bool option_get(struct asql_config* c, char* name);
void print_option_help();
