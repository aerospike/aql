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


//==========================================================
// Inlines and Macros.
//

#define asql_log_error(__fmt, ... ) \
	if ( g_aerospike->log.callback && AS_LOG_LEVEL_ERROR <= g_aerospike->log.level ) {\
		((as_log_callback) g_aerospike->log.callback)(AS_LOG_LEVEL_ERROR, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

#define asql_log_warn(__fmt, ... ) \
	if ( g_aerospike->log.callback && AS_LOG_LEVEL_WARN <= g_aerospike->log.level ) {\
		((as_log_callback) g_aerospike->log.callback)(AS_LOG_LEVEL_WARN, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

#define asql_log_info(__fmt, ... ) \
	if ( g_aerospike->log.callback && AS_LOG_LEVEL_INFO <= g_aerospike->log.level ) {\
		((as_log_callback) g_aerospike->log.callback)(AS_LOG_LEVEL_INFO, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

#define asql_log_debug(__fmt, ... ) \
	if ( g_aerospike->log.callback && AS_LOG_LEVEL_DEBUG <= g_aerospike->log.level ) {\
		((as_log_callback) g_aerospike->log.callback)(AS_LOG_LEVEL_DEBUG, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}

#define asql_log_trace(__fmt, ... ) \
	if ( g_aerospike->log.callback && AS_LOG_LEVEL_TRACE <= g_aerospike->log.level ) {\
		((as_log_callback) g_aerospike->log.callback)(AS_LOG_LEVEL_TRACE, __func__, __FILE__, __LINE__, __fmt, ##__VA_ARGS__);\
	}
