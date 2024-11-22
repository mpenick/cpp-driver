/*
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include "cassandra.h"

#include "result_response.hpp"

using namespace datastax;
using namespace datastax::internal::core;

extern "C" {

CassCursor cass_cursor(const CassResult* result) {
  const char* column = result->row_decoder().buffer();
  return CassCursor{
    result,
    result->row_count(),
    result->column_count(),
    column, column + result->row_decoder().remaining(),
    0,
    CASS_OK,
  };
}

cass_bool_t cass_cursor_next(CassCursor* cursor) {
  if (cursor->error_code != CASS_OK || cursor->column == cursor->end) {
    return cass_false;
  }

  cursor->column += cursor->column_size; // Move to the next column

  if (cursor->column + sizeof(int32_t) > cursor->end) {
    cursor->error_code = CASS_ERROR_LIB_NOT_ENOUGH_DATA;
    return cass_false;
  }

  // Decode the size of the next column
  cursor->column_size = 0;
  cursor->column = internal::decode_int32(cursor->column, cursor->column_size);

  if (cursor->column_size > 0 && cursor->column + cursor->column_size > cursor->end) {
      cursor->error_code = CASS_ERROR_LIB_NOT_ENOUGH_DATA;
    return cass_false;
  }

  return cass_true;
}

cass_int8_t cass_cursor_get_int8(const CassCursor* cursor) {
  if (cursor->column_size != 1) {
    return 0;
  }
  return cursor->column[0];
}

cass_int16_t cass_cursor_get_int16(const CassCursor* cursor) {
  if (cursor->column_size != 2) {
    return 0;
  }
  cass_int16_t result;
  internal::decode_int16(cursor->column, result);
  return result;
}

cass_int32_t cass_cursor_get_int32(const CassCursor* cursor) {
  if (cursor->column_size != 2) {
    return 0;
  }
  cass_int32_t result;
  internal::decode_int32(cursor->column, result);
  return result;
}

void cass_cursor_get_string(const CassCursor* cursor,
                            const char** output,
                            size_t* output_size) {
  if (cursor->column_size < 0) {
    *output = NULL;
    *output_size = 0;
  }
  *output = cursor->column;
  *output_size = cursor->column_size;
}

} // extern "C"
