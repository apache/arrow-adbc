// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <errno.h>
#include <string.h>

#include "nanoarrow.h"

static void ArrowSchemaViewSetPrimitive(struct ArrowSchemaView* schema_view,
                                        enum ArrowType data_type) {
  schema_view->data_type = data_type;
  schema_view->storage_data_type = data_type;
}

static ArrowErrorCode ArrowSchemaViewParse(struct ArrowSchemaView* schema_view,
                                           const char* format,
                                           const char** format_end_out,
                                           struct ArrowError* error) {
  *format_end_out = format;

  // needed for decimal parsing
  const char* parse_start;
  char* parse_end;

  switch (format[0]) {
    case 'n':
      schema_view->data_type = NANOARROW_TYPE_NA;
      schema_view->storage_data_type = NANOARROW_TYPE_NA;
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'b':
      ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_BOOL);
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'c':
      ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT8);
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'C':
      ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_UINT8);
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 's':
      ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT16);
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'S':
      ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_UINT16);
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'i':
      ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT32);
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'I':
      ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_UINT32);
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'l':
      ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT64);
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'L':
      ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_UINT64);
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'e':
      ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_HALF_FLOAT);
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'f':
      ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_FLOAT);
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'g':
      ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_DOUBLE);
      *format_end_out = format + 1;
      return NANOARROW_OK;

    // decimal
    case 'd':
      if (format[1] != ':' || format[2] == '\0') {
        ArrowErrorSet(error, "Expected ':precision,scale[,bitwidth]' following 'd'",
                      format + 3);
        return EINVAL;
      }

      parse_start = format + 2;
      schema_view->decimal_precision = strtol(parse_start, &parse_end, 10);
      if (parse_end == parse_start || parse_end[0] != ',') {
        ArrowErrorSet(error, "Expected 'precision,scale[,bitwidth]' following 'd:'");
        return EINVAL;
      }

      parse_start = parse_end + 1;
      schema_view->decimal_scale = strtol(parse_start, &parse_end, 10);
      if (parse_end == parse_start) {
        ArrowErrorSet(error, "Expected 'scale[,bitwidth]' following 'd:precision,'");
        return EINVAL;
      } else if (parse_end[0] != ',') {
        schema_view->decimal_bitwidth = 128;
      } else {
        parse_start = parse_end + 1;
        schema_view->decimal_bitwidth = strtol(parse_start, &parse_end, 10);
        if (parse_start == parse_end) {
          ArrowErrorSet(error, "Expected precision following 'd:precision,scale,'");
          return EINVAL;
        }
      }

      *format_end_out = parse_end;

      switch (schema_view->decimal_bitwidth) {
        case 128:
          ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_DECIMAL128);
          return NANOARROW_OK;
        case 256:
          ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_DECIMAL256);
          return NANOARROW_OK;
        default:
          ArrowErrorSet(error, "Expected decimal bitwidth of 128 or 256 but found %d",
                        (int)schema_view->decimal_bitwidth);
          return EINVAL;
      }

    // validity + data
    case 'w':
      schema_view->data_type = NANOARROW_TYPE_FIXED_SIZE_BINARY;
      schema_view->storage_data_type = NANOARROW_TYPE_FIXED_SIZE_BINARY;
      if (format[1] != ':' || format[2] == '\0') {
        ArrowErrorSet(error, "Expected ':<width>' following 'w'");
        return EINVAL;
      }

      schema_view->fixed_size = strtol(format + 2, (char**)format_end_out, 10);
      return NANOARROW_OK;

    // validity + offset + data
    case 'z':
      schema_view->data_type = NANOARROW_TYPE_BINARY;
      schema_view->storage_data_type = NANOARROW_TYPE_BINARY;
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'u':
      schema_view->data_type = NANOARROW_TYPE_STRING;
      schema_view->storage_data_type = NANOARROW_TYPE_STRING;
      *format_end_out = format + 1;
      return NANOARROW_OK;

    // validity + large_offset + data
    case 'Z':
      schema_view->data_type = NANOARROW_TYPE_LARGE_BINARY;
      schema_view->storage_data_type = NANOARROW_TYPE_LARGE_BINARY;
      *format_end_out = format + 1;
      return NANOARROW_OK;
    case 'U':
      schema_view->data_type = NANOARROW_TYPE_LARGE_STRING;
      schema_view->storage_data_type = NANOARROW_TYPE_LARGE_STRING;
      *format_end_out = format + 1;
      return NANOARROW_OK;

    // nested types
    case '+':
      switch (format[1]) {
        // list has validity + offset or offset
        case 'l':
          schema_view->storage_data_type = NANOARROW_TYPE_LIST;
          schema_view->data_type = NANOARROW_TYPE_LIST;
          *format_end_out = format + 2;
          return NANOARROW_OK;

        // large list has validity + large_offset or large_offset
        case 'L':
          schema_view->storage_data_type = NANOARROW_TYPE_LARGE_LIST;
          schema_view->data_type = NANOARROW_TYPE_LARGE_LIST;
          *format_end_out = format + 2;
          return NANOARROW_OK;

        // just validity buffer
        case 'w':
          if (format[2] != ':' || format[3] == '\0') {
            ArrowErrorSet(error, "Expected ':<width>' following '+w'");
            return EINVAL;
          }

          schema_view->storage_data_type = NANOARROW_TYPE_FIXED_SIZE_LIST;
          schema_view->data_type = NANOARROW_TYPE_FIXED_SIZE_LIST;
          schema_view->fixed_size = strtol(format + 3, (char**)format_end_out, 10);
          return NANOARROW_OK;
        case 's':
          schema_view->storage_data_type = NANOARROW_TYPE_STRUCT;
          schema_view->data_type = NANOARROW_TYPE_STRUCT;
          *format_end_out = format + 2;
          return NANOARROW_OK;
        case 'm':
          schema_view->storage_data_type = NANOARROW_TYPE_MAP;
          schema_view->data_type = NANOARROW_TYPE_MAP;
          *format_end_out = format + 2;
          return NANOARROW_OK;

        // unions
        case 'u':
          switch (format[2]) {
            case 'd':
              schema_view->storage_data_type = NANOARROW_TYPE_DENSE_UNION;
              schema_view->data_type = NANOARROW_TYPE_DENSE_UNION;
              break;
            case 's':
              schema_view->storage_data_type = NANOARROW_TYPE_SPARSE_UNION;
              schema_view->data_type = NANOARROW_TYPE_SPARSE_UNION;
              break;
            default:
              ArrowErrorSet(error,
                            "Expected union format string +us:<type_ids> or "
                            "+ud:<type_ids> but found '%s'",
                            format);
              return EINVAL;
          }

          if (format[3] == ':') {
            schema_view->union_type_ids.data = format + 4;
            schema_view->union_type_ids.n_bytes = strlen(format + 4);
            *format_end_out = format + strlen(format);
            return NANOARROW_OK;
          } else {
            ArrowErrorSet(error,
                          "Expected union format string +us:<type_ids> or +ud:<type_ids> "
                          "but found '%s'",
                          format);
            return EINVAL;
          }
      }

    // date/time types
    case 't':
      switch (format[1]) {
        // date
        case 'd':
          switch (format[2]) {
            case 'D':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT32);
              schema_view->data_type = NANOARROW_TYPE_DATE32;
              *format_end_out = format + 3;
              return NANOARROW_OK;
            case 'm':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT64);
              schema_view->data_type = NANOARROW_TYPE_DATE64;
              *format_end_out = format + 3;
              return NANOARROW_OK;
            default:
              ArrowErrorSet(error, "Expected 'D' or 'm' following 'td' but found '%s'",
                            format + 2);
              return EINVAL;
          }

        // time of day
        case 't':
          switch (format[2]) {
            case 's':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT32);
              schema_view->data_type = NANOARROW_TYPE_TIME32;
              schema_view->time_unit = NANOARROW_TIME_UNIT_SECOND;
              *format_end_out = format + 3;
              return NANOARROW_OK;
            case 'm':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT32);
              schema_view->data_type = NANOARROW_TYPE_TIME32;
              schema_view->time_unit = NANOARROW_TIME_UNIT_MILLI;
              *format_end_out = format + 3;
              return NANOARROW_OK;
            case 'u':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT64);
              schema_view->data_type = NANOARROW_TYPE_TIME64;
              schema_view->time_unit = NANOARROW_TIME_UNIT_MICRO;
              *format_end_out = format + 3;
              return NANOARROW_OK;
            case 'n':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT64);
              schema_view->data_type = NANOARROW_TYPE_TIME64;
              schema_view->time_unit = NANOARROW_TIME_UNIT_NANO;
              *format_end_out = format + 3;
              return NANOARROW_OK;
            default:
              ArrowErrorSet(
                  error, "Expected 's', 'm', 'u', or 'n' following 'tt' but found '%s'",
                  format + 2);
              return EINVAL;
          }

        // timestamp
        case 's':
          switch (format[2]) {
            case 's':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT32);
              schema_view->data_type = NANOARROW_TYPE_TIMESTAMP;
              schema_view->time_unit = NANOARROW_TIME_UNIT_SECOND;
              break;
            case 'm':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT32);
              schema_view->data_type = NANOARROW_TYPE_TIMESTAMP;
              schema_view->time_unit = NANOARROW_TIME_UNIT_MILLI;
              break;
            case 'u':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT64);
              schema_view->data_type = NANOARROW_TYPE_TIMESTAMP;
              schema_view->time_unit = NANOARROW_TIME_UNIT_MICRO;
              break;
            case 'n':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT64);
              schema_view->data_type = NANOARROW_TYPE_TIMESTAMP;
              schema_view->time_unit = NANOARROW_TIME_UNIT_NANO;
              break;
            default:
              ArrowErrorSet(
                  error, "Expected 's', 'm', 'u', or 'n' following 'ts' but found '%s'",
                  format + 2);
              return EINVAL;
          }

          if (format[3] != ':') {
            ArrowErrorSet(error, "Expected ':' following '%.3s' but found '%s'", format,
                          format + 3);
            return EINVAL;
          }

          schema_view->timezone.data = format + 4;
          schema_view->timezone.n_bytes = strlen(format + 4);
          *format_end_out = format + strlen(format);
          return NANOARROW_OK;

        // duration
        case 'D':
          switch (format[2]) {
            case 's':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT32);
              schema_view->data_type = NANOARROW_TYPE_DURATION;
              schema_view->time_unit = NANOARROW_TIME_UNIT_SECOND;
              *format_end_out = format + 3;
              return NANOARROW_OK;
            case 'm':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT32);
              schema_view->data_type = NANOARROW_TYPE_DURATION;
              schema_view->time_unit = NANOARROW_TIME_UNIT_MILLI;
              *format_end_out = format + 3;
              return NANOARROW_OK;
            case 'u':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT64);
              schema_view->data_type = NANOARROW_TYPE_DURATION;
              schema_view->time_unit = NANOARROW_TIME_UNIT_MICRO;
              *format_end_out = format + 3;
              return NANOARROW_OK;
            case 'n':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INT64);
              schema_view->data_type = NANOARROW_TYPE_DURATION;
              schema_view->time_unit = NANOARROW_TIME_UNIT_NANO;
              *format_end_out = format + 3;
              return NANOARROW_OK;
            default:
              ArrowErrorSet(error,
                            "Expected 's', 'm', u', or 'n' following 'tD' but found '%s'",
                            format + 2);
              return EINVAL;
          }

        // interval
        case 'i':
          switch (format[2]) {
            case 'M':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INTERVAL_MONTHS);
              *format_end_out = format + 3;
              return NANOARROW_OK;
            case 'D':
              ArrowSchemaViewSetPrimitive(schema_view, NANOARROW_TYPE_INTERVAL_DAY_TIME);
              *format_end_out = format + 3;
              return NANOARROW_OK;
            case 'n':
              ArrowSchemaViewSetPrimitive(schema_view,
                                          NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
              *format_end_out = format + 3;
              return NANOARROW_OK;
            default:
              ArrowErrorSet(error,
                            "Expected 'M', 'D', or 'n' following 'ti' but found '%s'",
                            format + 2);
              return EINVAL;
          }

        default:
          ArrowErrorSet(
              error, "Expected 'd', 't', 's', 'D', or 'i' following 't' but found '%s'",
              format + 1);
          return EINVAL;
      }

    default:
      ArrowErrorSet(error, "Unknown format: '%s'", format);
      return EINVAL;
  }
}

static ArrowErrorCode ArrowSchemaViewValidateNChildren(
    struct ArrowSchemaView* schema_view, int64_t n_children, struct ArrowError* error) {
  if (n_children != -1 && schema_view->schema->n_children != n_children) {
    ArrowErrorSet(error, "Expected schema with %d children but found %d children",
                  (int)n_children, (int)schema_view->schema->n_children);
    return EINVAL;
  }

  // Don't do a full validation of children but do check that they won't
  // segfault if inspected
  struct ArrowSchema* child;
  for (int64_t i = 0; i < schema_view->schema->n_children; i++) {
    child = schema_view->schema->children[i];
    if (child == NULL) {
      ArrowErrorSet(error, "Expected valid schema at schema->children[%d] but found NULL",
                    i);
      return EINVAL;
    } else if (child->release == NULL) {
      ArrowErrorSet(
          error,
          "Expected valid schema at schema->children[%d] but found a released schema", i);
      return EINVAL;
    }
  }

  return NANOARROW_OK;
}

static ArrowErrorCode ArrowSchemaViewValidateUnion(struct ArrowSchemaView* schema_view,
                                                   struct ArrowError* error) {
  return ArrowSchemaViewValidateNChildren(schema_view, -1, error);
}

static ArrowErrorCode ArrowSchemaViewValidateMap(struct ArrowSchemaView* schema_view,
                                                 struct ArrowError* error) {
  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewValidateNChildren(schema_view, 1, error));

  if (schema_view->schema->children[0]->n_children != 2) {
    ArrowErrorSet(error, "Expected child of map type to have 2 children but found %d",
                  (int)schema_view->schema->children[0]->n_children);
    return EINVAL;
  }

  if (strcmp(schema_view->schema->children[0]->format, "+s") != 0) {
    ArrowErrorSet(error, "Expected format of child of map type to be '+s' but found '%s'",
                  schema_view->schema->children[0]->format);
    return EINVAL;
  }

  return NANOARROW_OK;
}

static ArrowErrorCode ArrowSchemaViewValidateDictionary(
    struct ArrowSchemaView* schema_view, struct ArrowError* error) {
  // check for valid index type
  switch (schema_view->storage_data_type) {
    case NANOARROW_TYPE_UINT8:
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_UINT16:
    case NANOARROW_TYPE_INT16:
    case NANOARROW_TYPE_UINT32:
    case NANOARROW_TYPE_INT32:
    case NANOARROW_TYPE_UINT64:
    case NANOARROW_TYPE_INT64:
      break;
    default:
      ArrowErrorSet(
          error,
          "Expected dictionary schema index type to be an integral type but found '%s'",
          schema_view->schema->format);
      return EINVAL;
  }

  struct ArrowSchemaView dictionary_schema_view;
  return ArrowSchemaViewInit(&dictionary_schema_view, schema_view->schema->dictionary,
                             error);
}

static ArrowErrorCode ArrowSchemaViewValidate(struct ArrowSchemaView* schema_view,
                                              enum ArrowType data_type,
                                              struct ArrowError* error) {
  switch (data_type) {
    case NANOARROW_TYPE_NA:
    case NANOARROW_TYPE_BOOL:
    case NANOARROW_TYPE_UINT8:
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_UINT16:
    case NANOARROW_TYPE_INT16:
    case NANOARROW_TYPE_UINT32:
    case NANOARROW_TYPE_INT32:
    case NANOARROW_TYPE_UINT64:
    case NANOARROW_TYPE_INT64:
    case NANOARROW_TYPE_HALF_FLOAT:
    case NANOARROW_TYPE_FLOAT:
    case NANOARROW_TYPE_DOUBLE:
    case NANOARROW_TYPE_DECIMAL128:
    case NANOARROW_TYPE_DECIMAL256:
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_LARGE_BINARY:
    case NANOARROW_TYPE_DATE32:
    case NANOARROW_TYPE_DATE64:
    case NANOARROW_TYPE_INTERVAL_MONTHS:
    case NANOARROW_TYPE_INTERVAL_DAY_TIME:
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
    case NANOARROW_TYPE_TIMESTAMP:
    case NANOARROW_TYPE_TIME32:
    case NANOARROW_TYPE_TIME64:
    case NANOARROW_TYPE_DURATION:
      return ArrowSchemaViewValidateNChildren(schema_view, 0, error);

    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      if (schema_view->fixed_size <= 0) {
        ArrowErrorSet(error, "Expected size > 0 for fixed size binary but found size %d",
                      schema_view->fixed_size);
        return EINVAL;
      }
      return ArrowSchemaViewValidateNChildren(schema_view, 0, error);

    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST:
    case NANOARROW_TYPE_FIXED_SIZE_LIST:
      return ArrowSchemaViewValidateNChildren(schema_view, 1, error);

    case NANOARROW_TYPE_STRUCT:
      return ArrowSchemaViewValidateNChildren(schema_view, -1, error);

    case NANOARROW_TYPE_SPARSE_UNION:
    case NANOARROW_TYPE_DENSE_UNION:
      return ArrowSchemaViewValidateUnion(schema_view, error);

    case NANOARROW_TYPE_MAP:
      return ArrowSchemaViewValidateMap(schema_view, error);

    case NANOARROW_TYPE_DICTIONARY:
      return ArrowSchemaViewValidateDictionary(schema_view, error);

    default:
      ArrowErrorSet(error, "Expected a valid enum ArrowType value but found %d",
                    (int)schema_view->data_type);
      return EINVAL;
  }

  return NANOARROW_OK;
}

ArrowErrorCode ArrowSchemaViewInit(struct ArrowSchemaView* schema_view,
                                   struct ArrowSchema* schema, struct ArrowError* error) {
  if (schema == NULL) {
    ArrowErrorSet(error, "Expected non-NULL schema");
    return EINVAL;
  }

  if (schema->release == NULL) {
    ArrowErrorSet(error, "Expected non-released schema");
    return EINVAL;
  }

  schema_view->schema = schema;

  const char* format = schema->format;
  if (format == NULL) {
    ArrowErrorSet(
        error,
        "Error parsing schema->format: Expected a null-terminated string but found NULL");
    return EINVAL;
  }

  int format_len = strlen(format);
  if (format_len == 0) {
    ArrowErrorSet(error, "Error parsing schema->format: Expected a string with size > 0");
    return EINVAL;
  }

  const char* format_end_out;
  ArrowErrorCode result =
      ArrowSchemaViewParse(schema_view, format, &format_end_out, error);

  if (result != NANOARROW_OK) {
    char child_error[1024];
    memcpy(child_error, ArrowErrorMessage(error), 1024);
    ArrowErrorSet(error, "Error parsing schema->format: %s", child_error);
    return result;
  }

  if ((format + format_len) != format_end_out) {
    ArrowErrorSet(error, "Error parsing schema->format '%s': parsed %d/%d characters",
                  format, (int)(format_end_out - format), (int)(format_len));
    return EINVAL;
  }

  if (schema->dictionary != NULL) {
    schema_view->data_type = NANOARROW_TYPE_DICTIONARY;
  }

  result = ArrowSchemaViewValidate(schema_view, schema_view->storage_data_type, error);
  if (result != NANOARROW_OK) {
    return result;
  }

  if (schema_view->storage_data_type != schema_view->data_type) {
    result = ArrowSchemaViewValidate(schema_view, schema_view->data_type, error);
    if (result != NANOARROW_OK) {
      return result;
    }
  }

  ArrowLayoutInit(&schema_view->layout, schema_view->storage_data_type);
  if (schema_view->storage_data_type == NANOARROW_TYPE_FIXED_SIZE_BINARY) {
    schema_view->layout.element_size_bits[1] = schema_view->fixed_size * 8;
  } else if (schema_view->storage_data_type == NANOARROW_TYPE_FIXED_SIZE_LIST) {
    schema_view->layout.child_size_elements = schema_view->fixed_size;
  }

  schema_view->extension_name = ArrowCharView(NULL);
  schema_view->extension_metadata = ArrowCharView(NULL);
  ArrowMetadataGetValue(schema->metadata, ArrowCharView("ARROW:extension:name"),
                        &schema_view->extension_name);
  ArrowMetadataGetValue(schema->metadata, ArrowCharView("ARROW:extension:metadata"),
                        &schema_view->extension_metadata);

  return NANOARROW_OK;
}
