/*
 * Copyright 2026 Aerospike, Inc.
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
#include <string.h>

#include <aerospike/as_status.h>

#include <citrusleaf/cf_b64.h>

#include <asql_decode.h>


//==========================================================
// Typedefs & constants.
//

#define DIGEST_HEX_LEN ((uint32_t)(sizeof(as_digest_value) * 2))

#define DIGEST_B64_LEN ((uint32_t)(((sizeof(as_digest_value) + 2) / 3) * 4))

// cf_b64_decode() writes whole three byte groups, so decoding an encoded digest
// needs up to two bytes more than the digest itself.
#define DIGEST_DECODE_BUF_SIZE (sizeof(as_digest_value) + 2)


//==========================================================
// Forward Declarations.
//

static bool hex_nibble(char c, uint8_t* val);


//==========================================================
// Public API.
//

bool
asql_digest_from_hex(as_error* err, as_digest_value dig, const char* in)
{
	size_t in_len = in ? strlen(in) : 0;

	if (in_len != DIGEST_HEX_LEN) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"Digest must be %u hex characters, got %zu: '%s'",
				DIGEST_HEX_LEN, in_len, in ? in : "");
		return false;
	}

	uint8_t buf[sizeof(as_digest_value)];

	for (uint32_t i = 0; i < sizeof(buf); i++) {
		uint8_t hi;
		uint8_t lo;

		if (!hex_nibble(in[i * 2], &hi)
				|| !hex_nibble(in[(i * 2) + 1], &lo)) {
			as_error_update(err, AEROSPIKE_ERR_CLIENT,
					"Digest is not valid hex: '%s'", in);
			return false;
		}

		buf[i] = (uint8_t)((hi << 4) | lo);
	}

	memcpy(dig, buf, sizeof(buf));

	return true;
}

bool
asql_digest_from_b64(as_error* err, as_digest_value dig, const char* in)
{
	uint32_t encoded_len = DIGEST_B64_LEN;
	size_t in_len = in ? strlen(in) : 0;

	if (in_len != encoded_len) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"Edigest must be %u base64 characters, got %zu: '%s'",
				encoded_len, in_len, in ? in : "");
		return false;
	}

	uint8_t buf[DIGEST_DECODE_BUF_SIZE];
	uint32_t buf_size = 0;

	// Unvalidated input decodes to a valid looking digest of the wrong record.
	if (!cf_b64_validate_and_decode(in, encoded_len, buf, &buf_size)
			|| buf_size != sizeof(as_digest_value)) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"Edigest is not a valid base64 encoded digest: '%s'", in);
		return false;
	}

	char reencoded[DIGEST_B64_LEN];

	cf_b64_encode(buf, sizeof(as_digest_value), reencoded);

	if (memcmp(reencoded, in, sizeof(reencoded)) != 0) {
		as_error_update(err, AEROSPIKE_ERR_CLIENT,
				"Edigest is not canonically base64 encoded: '%s'", in);
		return false;
	}

	memcpy(dig, buf, sizeof(as_digest_value));

	return true;
}

char*
asql_b64_decode_str(const char* in, uint32_t* out_size)
{
	if (!in) {
		return NULL;
	}

	size_t in_len = strlen(in);

	// The decoder reads whole four character groups.
	if (in_len == 0 || (in_len % 4) != 0 || in_len > UINT32_MAX / 4) {
		return NULL;
	}

	uint32_t buf_size = cf_b64_decoded_buf_size((uint32_t)in_len);
	uint8_t* buf = (uint8_t*)malloc((size_t)buf_size + 1);

	if (!buf) {
		return NULL;
	}

	uint32_t decoded_size = 0;

	if (!cf_b64_validate_and_decode(in, (uint32_t)in_len, buf, &decoded_size)) {
		free(buf);
		return NULL;
	}

	buf[decoded_size] = '\0';

	if (out_size) {
		*out_size = decoded_size;
	}

	return (char*)buf;
}


//==========================================================
// Local Helpers.
//

static bool
hex_nibble(char c, uint8_t* val)
{
	if (c >= '0' && c <= '9') {
		*val = (uint8_t)(c - '0');
		return true;
	}

	if (c >= 'a' && c <= 'f') {
		*val = (uint8_t)(c - 'a' + 10);
		return true;
	}

	if (c >= 'A' && c <= 'F') {
		*val = (uint8_t)(c - 'A' + 10);
		return true;
	}

	return false;
}
