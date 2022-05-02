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

#include <json.h>
#include <jansson.h>
#include <asql_value.h>
#include <asql.h>


//==========================================================
// Typedefs & constants.
//
typedef struct asql_type_s {
	char* name;
	asql_value_type_t vtype;
} asql_type_t;

static asql_type_t asql_types[] = {
	// Integer type names:
	{ "DECIMAL", ASQL_VALUE_TYPE_INT },
	{ "INT", ASQL_VALUE_TYPE_INT },
	{ "NUMERIC", ASQL_VALUE_TYPE_INT },

	// Float type names:
	{ "FLOAT", ASQL_VALUE_TYPE_FLOAT },
	{ "REAL", ASQL_VALUE_TYPE_FLOAT },

	// GeoJSON type name:
	{ "GEOJSON", ASQL_VALUE_TYPE_GEOJSON },

	// JSON type name:
	{ "JSON", ASQL_VALUE_TYPE_JSON },

	// List type:
	{ "LIST", ASQL_VALUE_TYPE_LIST },

	// Map type:
	{ "MAP", ASQL_VALUE_TYPE_MAP },

	// String type names:
	{ "CHAR", ASQL_VALUE_TYPE_STRING },
	{ "STRING", ASQL_VALUE_TYPE_STRING },
	{ "TEXT", ASQL_VALUE_TYPE_STRING },
	{ "VARCHAR", ASQL_VALUE_TYPE_STRING },

	// End of the type name
	{ NULL, ASQL_VALUE_TYPE_NONE }
};

//=========================================================
// Forward Declarations.
//



//=========================================================
// Public API.
//

int
asql_set_args(as_error* err, as_vector* udfargs, as_arraylist* arglist)
{
	if (!udfargs) {
		as_arraylist_append(arglist, (as_val*)&as_nil);
		return 0;
	}

	for (uint16_t i = 0; i < udfargs->size; i++) {

		asql_value* value = as_vector_get(udfargs, i);

		switch (value->type) {
			case AS_INTEGER: {
				as_arraylist_append_int64(arglist, value->u.i64);
				break;
			}
			case AS_DOUBLE: {
				as_arraylist_append_double(arglist, value->u.dbl);
				break;
			}
			case AS_STRING: {
				char* str = value->u.str;

				// TODO: In-band type to be deprecated in favor
				// of ASQL internal value type.
				if (!strncmp(str, "JSON", 4)
						|| (ASQL_VALUE_TYPE_JSON == value->vt)
						|| (ASQL_VALUE_TYPE_LIST == value->vt)
						|| (ASQL_VALUE_TYPE_MAP == value->vt)) {

					json_t* json = json_string(str);

					as_val* val = NULL;
					if (json) {
						val = as_json_arg((char*)json_string_value(json),
										  value->vt);
						json_decref(json);
					}
					if (val) {
						as_arraylist_append(arglist, val);
					}
					else {
						return as_error_update(
								err, AEROSPIKE_ERR_CLIENT,
								"Error: Value is invalid JSON: %s", str);
					}
				}
				else {
					// Transfer ownership of string from asql_value to as_val.
					as_arraylist_append(arglist,
										(as_val*)as_string_new_strdup(str));
				}
				break;
			}
			case AS_NIL: {
				as_arraylist_append(arglist, (as_val*)&as_nil);
				break;
			}

			default: {
				return as_error_update(err, AEROSPIKE_ERR_CLIENT,
									   "Error: Invalid type: %d",
									   value->type);
			}
		}
	}
	return 0;
}

void
asql_free_value(void* ptr)
{
	asql_value* value = (asql_value* )ptr;
	if (value &&
			(value->type == AS_STRING ||
			 value->type == AS_GEOJSON)) {
		if (value->u.str) {
			free(value->u.str);
		}
	}
}

// Return the ASQL internal value type for the given type name,
// or ASQL_VALUE_TYPE_NONE if no such type exists.
asql_value_type_t
asql_value_type_from_type_name(char* str)
{
	asql_value_type_t vtype = ASQL_VALUE_TYPE_NONE;
	int i = 0;
	while (asql_types[i].name) {
		if (!strcasecmp(asql_types[i].name, str)) {
			vtype = asql_types[i].vtype;
			break;
		}
		i++;
	}

	return vtype;
}

char*
asql_val_str(const as_val* val)
{
	// In some instances, C-client can return as_val with count=0.
	// In such cases, we should not be calling as_val_to_string.
	char* str = val->count ? as_val_tostring(val): NULL;
	
	// In some instances, C-client can return a NULL here ( different from AS_NIL )
	// Example : If we have an empty non-null blob or byte-array, with valid length, size etc
	uint32_t len = str ? (uint32_t)strlen(str): 0;

	if (g_config->base.outputtypes) {
		if (AS_GEOJSON == as_val_type(val)) {
			len += 10;
			char* str2 = malloc(len);
			snprintf(str2, len, "GeoJSON(%s)", str);
			char* c;
			if ((c = index(str2, '"'))) {
				*c = '\'';
			}
			if ((c = rindex(str2, '"'))) {
				*c = '\'';
			}
			free(str);
			str = str2;
		}
		else if (AS_LIST == as_val_type(val)) {
			len += strlen("LIST") + 5;
			char* str2 = malloc(len);
			snprintf(str2, len, "%s('%s')", "LIST", str);
			free(str);
			str = str2;
		} else if (AS_MAP == as_val_type(val)) {
			uint32_t mflags = ((as_map*)val)->flags;
			switch(mflags) {
				case 1: {
					len += strlen("KEY_ORDERED_MAP") + 5;
					char* str2 = malloc(len);
					snprintf(str2, len, "%s('%s')", "KEY_ORDERED_MAP", str);
					free(str);
					str = str2;
					break;
				}
				case 3: {
					len += strlen("KEY_VALUE_ORDERED_MAP") + 5;
					char* str2 = malloc(len);
					snprintf(str2, len, "%s('%s')", "KEY_VALUE_ORDERED_MAP", str);
					free(str);
					str = str2;
					break;
				}

				default: {
					len += strlen("MAP") + 5;
					char* str2 = malloc(len);
					snprintf(str2, len, "%s('%s')", "MAP", str);
					free(str);
					str = str2;
					break;
				}
			}
		}
	}
	return str;
}

// Parse a value string as the specified value type.
// Return 0 if successful, < 0 otherwise.
int
asql_parse_value_as(char* s, asql_value* value, asql_value_type_t vtype)
{
	size_t len = strlen(s);

	if (!len) {
		return -1;
	}

	// Special case.
	if (!strcasecmp(s, "NULL")) {
		value->type = AS_STRING;
		value->u.str = 0;
		return 0;
	}

	switch (vtype) {
		case ASQL_VALUE_TYPE_INT: {
			char s0 = *s;
			if (len > 2
			        && ((*s == '\'' && s[len - 1] == '\'')
			                || (*s == '\"' && s[len - 1] == '\"'))) {
				s += 1;
			}

			char* endptr = 0;
			int64_t val = strtoll(s, &endptr, 0);

			if (*endptr == 0 || *endptr == s0) {
				value->type = AS_INTEGER;
				value->u.i64 = val;
			}
			else {
				fprintf(stdout, "Error!  Cannot cast \"%s\" to int!\n", s);
				return -2;
			}
			break;
		}
		case ASQL_VALUE_TYPE_FLOAT: {
			char s0 = *s;
			if (len > 2
			        && ((*s == '\'' && s[len - 1] == '\'')
			                || (*s == '\"' && s[len - 1] == '\"'))) {
				s += 1;
			}

			char* endptr = 0;
			double dbl = strtod(s, &endptr);

			if (*endptr == 0 || *endptr == s0) {
				value->type = AS_DOUBLE;
				value->u.dbl = dbl;
			}
			else {
				fprintf(stdout, "Error!  Cannot cast \"%s\" to float!\n", s);
				return -2;
			}
			break;
		}
		case ASQL_VALUE_TYPE_GEOJSON:
		case ASQL_VALUE_TYPE_JSON:
		case ASQL_VALUE_TYPE_LIST:
		case ASQL_VALUE_TYPE_MAP:
		case ASQL_VALUE_TYPE_STRING: {
			int start = 0, end = (int)len;
			if (len > 2
			        && ((*s == '\'' && s[len - 1] == '\'')
			                || (*s == '\"' && s[len - 1] == '\"'))) {
				start++;
				end -= 2;
			}

			if (ASQL_VALUE_TYPE_GEOJSON == vtype) {
				value->type = AS_GEOJSON;
			}
			else {
				value->type = AS_STRING;
			}

			char* str = malloc(end + 1);
			memcpy(str, s + start, end);
			str[end] = 0;
			value->u.str = str;
			break;
		}
		default:
			fprintf(stdout, "Error!  Unknown ASQL value type: %d\n", vtype);
			return -2;
	}

	// Save the ASQL-internal value type for eventual processing.
	value->vt = vtype;

	return 0;
}
