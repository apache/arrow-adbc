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

#include "statement.h"

#include <cinttypes>
#include <memory>

#include <adbc.h>
#include <arrow/Message_generated.h>
#include <arrow/Schema_generated.h>
#include <flatbuffers/flatbuffers.h>
#include <google/cloud/bigquery/storage/v1/bigquery_read_client.h>
#include <nanoarrow/nanoarrow.hpp>

#include "common/options.h"
#include "common/utils.h"
#include "connection.h"
#include "database.h"

namespace adbc_bigquery {

static void BigQueryArrowArrayReleaseInternal(struct ArrowArray* array) {
  // Release buffers held by this array
  struct ArrowArrayPrivateData* private_data =
      (struct ArrowArrayPrivateData*)array->private_data;
  if (private_data != NULL) {
    ArrowBitmapReset(&private_data->bitmap);
    ArrowBufferReset(&private_data->buffers[0]);
    ArrowBufferReset(&private_data->buffers[1]);
    ArrowFree(private_data);
  }

  // This object owns the memory for all the children, but those
  // children may have been generated elsewhere and might have
  // their own release() callback.
  if (array->children != NULL) {
    for (int64_t i = 0; i < array->n_children; i++) {
      if (array->children[i] != NULL) {
        if (array->children[i]->release != NULL) {
          ArrowArrayRelease(array->children[i]);
        }

        ArrowFree(array->children[i]);
      }
    }

    ArrowFree(array->children);
  }

  // This object owns the memory for the dictionary but it
  // may have been generated somewhere else and have its own
  // release() callback.
  if (array->dictionary != NULL) {
    if (array->dictionary->release != NULL) {
      ArrowArrayRelease(array->dictionary);
    }

    ArrowFree(array->dictionary);
  }

  // Mark released
  array->release = NULL;
}

static ArrowErrorCode BigQueryArrowArraySetStorageType(struct ArrowArray* array,
                                                       enum ArrowType storage_type) {
  // unhandled types:
  //  - NANOARROW_TYPE_DICTIONARY
  //  - NANOARROW_TYPE_EXTENSION
  switch (storage_type) {
    case NANOARROW_TYPE_UNINITIALIZED:
    case NANOARROW_TYPE_NA:
      array->n_buffers = 0;
      break;

    case NANOARROW_TYPE_FIXED_SIZE_LIST:
    case NANOARROW_TYPE_STRUCT:
    case NANOARROW_TYPE_SPARSE_UNION:
      array->n_buffers = 1;
      break;

    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST:
    case NANOARROW_TYPE_MAP:
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
    case NANOARROW_TYPE_INTERVAL_MONTHS:
    case NANOARROW_TYPE_INTERVAL_DAY_TIME:
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
    case NANOARROW_TYPE_DENSE_UNION:
    case NANOARROW_TYPE_DATE32:
    case NANOARROW_TYPE_DATE64:
    case NANOARROW_TYPE_TIMESTAMP:
    case NANOARROW_TYPE_TIME32:
    case NANOARROW_TYPE_TIME64:
    case NANOARROW_TYPE_DURATION:
      array->n_buffers = 2;
      break;

    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_LARGE_BINARY:
      array->n_buffers = 3;
      break;

    default:
      return EINVAL;

      return NANOARROW_OK;
  }

  struct ArrowArrayPrivateData* private_data =
      (struct ArrowArrayPrivateData*)array->private_data;
  private_data->storage_type = storage_type;
  return NANOARROW_OK;
}

ArrowErrorCode BigQueryArrowArrayInitFromType(struct ArrowArray* array,
                                              enum ArrowType storage_type) {
  array->length = 0;
  array->null_count = 0;
  array->offset = 0;
  array->n_buffers = 0;
  array->n_children = 0;
  array->buffers = NULL;
  array->children = NULL;
  array->dictionary = NULL;
  array->release = &BigQueryArrowArrayReleaseInternal;
  array->private_data = NULL;

  struct ArrowArrayPrivateData* private_data =
      (struct ArrowArrayPrivateData*)ArrowMalloc(sizeof(struct ArrowArrayPrivateData));
  if (private_data == NULL) {
    array->release = NULL;
    return ENOMEM;
  }

  ArrowBitmapInit(&private_data->bitmap);
  ArrowBufferInit(&private_data->buffers[0]);
  ArrowBufferInit(&private_data->buffers[1]);
  private_data->buffer_data[0] = NULL;
  private_data->buffer_data[1] = NULL;
  private_data->buffer_data[2] = NULL;

  array->private_data = private_data;
  array->buffers = (const void**)(&private_data->buffer_data);

  int result = BigQueryArrowArraySetStorageType(array, storage_type);
  if (result != NANOARROW_OK) {
    ArrowArrayRelease(array);
    return result;
  }

  ArrowLayoutInit(&private_data->layout, storage_type);
  // We can only know this not to be true when initializing based on a schema
  // so assume this to be true.
  private_data->union_type_id_is_child_index = 1;
  return NANOARROW_OK;
}

int parse_encapsulated_message(const std::string& data,
                               org::apache::arrow::flatbuf::MessageHeader expected_header,
                               void* out_data, void* private_data) {
  // https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
  if (data.length() == 0) return ADBC_STATUS_OK;

  uintptr_t* data_ptr = (uintptr_t*)data.data();

  // A 32-bit continuation indicator. The value 0xFFFFFFFF indicates a valid message.
  bool continuation = *(uint32_t*)data_ptr == 0xFFFFFFFF;
  if (!continuation) return ADBC_STATUS_INVALID_DATA;
  data_ptr = (uintptr_t*)(((uint64_t)(uint64_t*)data_ptr) + 4);

  // metadata_size:
  // A 32-bit little-endian length prefix indicating the metadata size
  int32_t metadata_size = *(int32_t*)data_ptr;
#if ADBC_DRIVER_BIGQUERY_ENDIAN == 0
  metadata_size = __builtin_bswap32(metadata_size);
#endif
  data_ptr = (uintptr_t*)(((uint64_t)(uint64_t*)data_ptr) + 4);
  // printf("  continuation: %d\r\n", continuation);
  // printf("  metadata_size: %d\r\n", metadata_size);
  auto header = org::apache::arrow::flatbuf::GetMessage(data_ptr);
  auto body_data = (uintptr_t*)(((uint64_t)(uint64_t*)data_ptr) + metadata_size);
  auto header_type = header->header_type();
  // printf("  header_type: %hhu\r\n", header_type);
  if (header_type == expected_header) {
    if (header_type == org::apache::arrow::flatbuf::MessageHeader::Schema) {
      auto schema = header->header_as_Schema();
      auto fields = schema->fields();

      // https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
      struct ArrowSchema* out = (struct ArrowSchema*)out_data;
      ArrowSchemaInit(out);
      out->name = nullptr;
      ArrowSchemaSetTypeStruct(out, fields->size());

      const org::apache::arrow::flatbuf::Int* field_int = nullptr;
      const org::apache::arrow::flatbuf::FloatingPoint* field_fp = nullptr;
      const org::apache::arrow::flatbuf::Decimal* field_decimal = nullptr;
      const org::apache::arrow::flatbuf::Date* field_date = nullptr;
      const org::apache::arrow::flatbuf::Time* field_time = nullptr;
      const org::apache::arrow::flatbuf::Interval* field_interval = nullptr;
      // const org::apache::arrow::flatbuf::List* field_list = nullptr;
      // const org::apache::arrow::flatbuf::Struct_* field_struct = nullptr;
      const org::apache::arrow::flatbuf::Union* field_union = nullptr;
      const org::apache::arrow::flatbuf::FixedSizeBinary* field_fixed_size_binary =
          nullptr;
      const org::apache::arrow::flatbuf::FixedSizeList* field_fixed_size_list = nullptr;
      const flatbuffers::Vector<int32_t>* type_ids;
      for (size_t i = 0; i < fields->size(); i++) {
        auto field = fields->Get(i);
        auto child = out->children[i];
        ArrowSchemaSetName(child, field->name()->str().c_str());
        switch (field->type_type()) {
          case org::apache::arrow::flatbuf::Type::NONE:
            ArrowSchemaSetType(child, NANOARROW_TYPE_UNINITIALIZED);
            break;
          case org::apache::arrow::flatbuf::Type::Null:
            ArrowSchemaSetType(child, NANOARROW_TYPE_NA);
            break;
          case org::apache::arrow::flatbuf::Type::Int:
            field_int = field->type_as_Int();
            if (field_int->is_signed()) {
              if (field_int->bitWidth() == 8) {
                ArrowSchemaSetType(child, NANOARROW_TYPE_INT8);
              } else if (field_int->bitWidth() == 16) {
                ArrowSchemaSetType(child, NANOARROW_TYPE_INT16);
              } else if (field_int->bitWidth() == 32) {
                ArrowSchemaSetType(child, NANOARROW_TYPE_INT32);
              } else {
                ArrowSchemaSetType(child, NANOARROW_TYPE_INT64);
              }
            } else {
              if (field_int->bitWidth() == 8) {
                ArrowSchemaSetType(child, NANOARROW_TYPE_UINT8);
              } else if (field_int->bitWidth() == 16) {
                ArrowSchemaSetType(child, NANOARROW_TYPE_UINT16);
              } else if (field_int->bitWidth() == 32) {
                ArrowSchemaSetType(child, NANOARROW_TYPE_UINT32);
              } else {
                ArrowSchemaSetType(child, NANOARROW_TYPE_UINT64);
              }
            }
            break;
          case org::apache::arrow::flatbuf::Type::FloatingPoint:
            field_fp = field->type_as_FloatingPoint();
            if (field_fp->precision() == org::apache::arrow::flatbuf::Precision::HALF) {
              ArrowSchemaSetType(child, NANOARROW_TYPE_HALF_FLOAT);
            } else if (field_fp->precision() ==
                       org::apache::arrow::flatbuf::Precision::SINGLE) {
              ArrowSchemaSetType(child, NANOARROW_TYPE_FLOAT);
            } else {
              ArrowSchemaSetType(child, NANOARROW_TYPE_DOUBLE);
            }
            break;
          case org::apache::arrow::flatbuf::Type::Binary:
            ArrowSchemaSetType(child, NANOARROW_TYPE_BINARY);
            break;
          case org::apache::arrow::flatbuf::Type::Utf8:
            ArrowSchemaSetType(child, NANOARROW_TYPE_STRING);
            break;
          case org::apache::arrow::flatbuf::Type::Bool:
            ArrowSchemaSetType(child, NANOARROW_TYPE_BOOL);
            break;
          case org::apache::arrow::flatbuf::Type::Decimal:
            field_decimal = field->type_as_Decimal();
            if (field_decimal->bitWidth() == 128) {
              ArrowSchemaSetTypeDecimal(child, NANOARROW_TYPE_DECIMAL128,
                                        field_decimal->precision(),
                                        field_decimal->scale());
            } else {
              ArrowSchemaSetTypeDecimal(child, NANOARROW_TYPE_DECIMAL256,
                                        field_decimal->precision(),
                                        field_decimal->scale());
            }
            break;
          case org::apache::arrow::flatbuf::Type::Date:
            field_date = field->type_as_Date();
            if (field_date->unit() == org::apache::arrow::flatbuf::DateUnit::DAY) {
              ArrowSchemaSetType(child, NANOARROW_TYPE_DATE32);
            } else {
              ArrowSchemaSetType(child, NANOARROW_TYPE_DATE64);
            }
            break;
          case org::apache::arrow::flatbuf::Type::Time:
            field_time = field->type_as_Time();
            if (field_time->bitWidth() == 32) {
              if (field_time->unit() == org::apache::arrow::flatbuf::TimeUnit::SECOND) {
                ArrowSchemaSetTypeDateTime(child, NANOARROW_TYPE_TIME32,
                                           NANOARROW_TIME_UNIT_SECOND, NULL);
              } else if (field_time->unit() ==
                         org::apache::arrow::flatbuf::TimeUnit::MILLISECOND) {
                ArrowSchemaSetTypeDateTime(child, NANOARROW_TYPE_TIME32,
                                           NANOARROW_TIME_UNIT_MILLI, NULL);
              }
            } else if (field_time->bitWidth() == 64) {
              if (field_time->unit() ==
                  org::apache::arrow::flatbuf::TimeUnit::MICROSECOND) {
                ArrowSchemaSetTypeDateTime(child, NANOARROW_TYPE_TIME64,
                                           NANOARROW_TIME_UNIT_MICRO, NULL);
              } else if (field_time->unit() ==
                         org::apache::arrow::flatbuf::TimeUnit::NANOSECOND) {
                ArrowSchemaSetTypeDateTime(child, NANOARROW_TYPE_TIME64,
                                           NANOARROW_TIME_UNIT_NANO, NULL);
              }
            }
            break;
          case org::apache::arrow::flatbuf::Type::Timestamp:
            ArrowSchemaSetType(child, NANOARROW_TYPE_TIMESTAMP);
            break;
          case org::apache::arrow::flatbuf::Type::Interval:
            field_interval = field->type_as_Interval();
            if (field_interval->unit() ==
                org::apache::arrow::flatbuf::IntervalUnit::YEAR_MONTH) {
              ArrowSchemaSetType(child, NANOARROW_TYPE_INTERVAL_MONTHS);
            } else if (field_interval->unit() ==
                       org::apache::arrow::flatbuf::IntervalUnit::DAY_TIME) {
              ArrowSchemaSetType(child, NANOARROW_TYPE_INTERVAL_DAY_TIME);
            } else {
              ArrowSchemaSetType(child, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
            }
            break;
          case org::apache::arrow::flatbuf::Type::List:
            // TODO_BIGQUERY: set its child's type
            ArrowSchemaSetType(child, NANOARROW_TYPE_LIST);
            break;
          case org::apache::arrow::flatbuf::Type::Struct_:
            // TODO_BIGQUERY: how do I get its children?
            // field_struct = field->type_as_Struct_();
            ArrowSchemaSetTypeStruct(child, 1);
            break;
          case org::apache::arrow::flatbuf::Type::Union:
            field_union = field->type_as_Union();

            // TODO_BIGQUERY: figure out the type ids and set their types
            type_ids = field_union->typeIds();
            if (field_union->mode() == org::apache::arrow::flatbuf::UnionMode::Sparse) {
              ArrowSchemaSetTypeUnion(child, NANOARROW_TYPE_SPARSE_UNION,
                                      type_ids->size());
            } else {
              ArrowSchemaSetTypeUnion(child, NANOARROW_TYPE_DENSE_UNION,
                                      type_ids->size());
            }
            break;
          case org::apache::arrow::flatbuf::Type::FixedSizeBinary:
            field_fixed_size_binary = field->type_as_FixedSizeBinary();
            ArrowSchemaSetTypeFixedSize(child, NANOARROW_TYPE_FIXED_SIZE_BINARY,
                                        field_fixed_size_binary->byteWidth());
            break;
          case org::apache::arrow::flatbuf::Type::FixedSizeList:
            // TODO_BIGQUERY: set its child's type
            field_fixed_size_list = field->type_as_FixedSizeList();
            ArrowSchemaSetTypeFixedSize(child, NANOARROW_TYPE_FIXED_SIZE_LIST,
                                        field_fixed_size_list->listSize());
            break;
          case org::apache::arrow::flatbuf::Type::Map:
            ArrowSchemaSetType(child, NANOARROW_TYPE_MAP);
            break;
          case org::apache::arrow::flatbuf::Type::Duration:
            ArrowSchemaSetType(child, NANOARROW_TYPE_DURATION);
            break;
          case org::apache::arrow::flatbuf::Type::LargeBinary:
            ArrowSchemaSetType(child, NANOARROW_TYPE_LARGE_BINARY);
            break;
          case org::apache::arrow::flatbuf::Type::LargeUtf8:
            ArrowSchemaSetType(child, NANOARROW_TYPE_LARGE_STRING);
            break;
          case org::apache::arrow::flatbuf::Type::LargeList:
            ArrowSchemaSetType(child, NANOARROW_TYPE_LARGE_LIST);
            break;
          case org::apache::arrow::flatbuf::Type::RunEndEncoded:
            child->format = "+r";
            break;
          case org::apache::arrow::flatbuf::Type::BinaryView:
            child->format = "vz";
            break;
          case org::apache::arrow::flatbuf::Type::Utf8View:
            child->format = "vu";
            break;
          case org::apache::arrow::flatbuf::Type::ListView:
            child->format = "+vl";
            break;
          case org::apache::arrow::flatbuf::Type::LargeListView:
            child->format = "+vL";
            break;
          default:
            // malformed schema?
            break;
        }
      }
      return ADBC_STATUS_OK;
    } else if (header_type == org::apache::arrow::flatbuf::MessageHeader::RecordBatch) {
      struct ArrowSchema* schema = (struct ArrowSchema*)private_data;
      auto data_header = header->header_as_RecordBatch();
      auto nodes = data_header->nodes();

      auto buffers = data_header->buffers();
      int buffer_index = 0;
      int ret = EINVAL;

      struct ArrowArray* out = (struct ArrowArray*)out_data;
      memset(out, 0, sizeof(struct ArrowArray));
      ret = ArrowArrayInitFromType(out, NANOARROW_TYPE_STRUCT);
      if (ret != NANOARROW_OK) {
        return ret;
      }

      out->n_children = nodes->size();
      out->children =
          (struct ArrowArray**)ArrowMalloc(sizeof(struct ArrowArray*) * out->n_children);
      for (size_t i = 0; i < nodes->size(); i++) {
        out->children[i] = (struct ArrowArray*)ArrowMalloc(sizeof(struct ArrowArray));
        memset(out->children[i], 0, sizeof(struct ArrowArray));
      }

      static std::map<std::string, ArrowType> fixed_size_format_map = {
          {"n", NANOARROW_TYPE_NA},
          {"b", NANOARROW_TYPE_BOOL},
          {"C", NANOARROW_TYPE_UINT8},
          {"c", NANOARROW_TYPE_INT8},
          {"S", NANOARROW_TYPE_UINT16},
          {"s", NANOARROW_TYPE_INT16},
          {"I", NANOARROW_TYPE_UINT32},
          {"i", NANOARROW_TYPE_INT32},
          {"L", NANOARROW_TYPE_UINT64},
          {"l", NANOARROW_TYPE_INT64},
          {"e", NANOARROW_TYPE_HALF_FLOAT},
          {"f", NANOARROW_TYPE_FLOAT},
          {"g", NANOARROW_TYPE_DOUBLE},
          {"u", NANOARROW_TYPE_STRING},
          {"z", NANOARROW_TYPE_BINARY},
          // NANOARROW_TYPE_FIXED_SIZE_BINARY handled below
          {"tdD", NANOARROW_TYPE_DATE32},
          {"tdm", NANOARROW_TYPE_DATE64},
          // NANOARROW_TYPE_TIMESTAMP handled below
          {"tts", NANOARROW_TYPE_TIME32},
          {"ttm", NANOARROW_TYPE_TIME32},
          {"ttu", NANOARROW_TYPE_TIME64},
          {"ttn", NANOARROW_TYPE_TIME64},
          {"tiM", NANOARROW_TYPE_INTERVAL_MONTHS},
          {"tiD", NANOARROW_TYPE_INTERVAL_DAY_TIME},
          // NANOARROW_TYPE_DECIMAL128 handled below
          // NANOARROW_TYPE_DECIMAL256 handled below
          {"+l", NANOARROW_TYPE_LIST},
          {"+s", NANOARROW_TYPE_STRUCT},
          // NANOARROW_TYPE_SPARSE_UNION handled below
          // NANOARROW_TYPE_DENSE_UNION handled below
          // NANOARROW_TYPE_DICTIONARY handled below
          {"+m", NANOARROW_TYPE_MAP},
          // NANOARROW_TYPE_EXTENSION handled below
          // NANOARROW_TYPE_FIXED_SIZE_LIST handled below
          {"tDs", NANOARROW_TYPE_DURATION},
          {"tDm", NANOARROW_TYPE_DURATION},
          {"tDu", NANOARROW_TYPE_DURATION},
          {"tDn", NANOARROW_TYPE_DURATION},
          {"U", NANOARROW_TYPE_LARGE_STRING},
          {"Z", NANOARROW_TYPE_LARGE_BINARY},
          {"+L", NANOARROW_TYPE_LARGE_LIST},
          {"tin", NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO}};

      for (size_t i = 0; i < nodes->size(); i++) {
        auto node = nodes->Get(i);
        auto child_schema = schema->children[i];
        const char* format = child_schema->format;

        auto child = out->children[i];
        auto map_to_type = fixed_size_format_map.find(format);
        int ret = EINVAL;
        if (map_to_type != fixed_size_format_map.end()) {
          ret = BigQueryArrowArrayInitFromType(child, map_to_type->second);
        } else {
          int format_len = strlen(format);
          if (format_len > 2 && format[0] == 'w' && format[1] == ':') {
            // NANOARROW_TYPE_FIXED_SIZE_BINARY
            //   w:42 - fixed-width binary [42 bytes]
            ret = ArrowArrayInitFromType(child, NANOARROW_TYPE_FIXED_SIZE_BINARY);
          } else if (format_len > 4 && format[0] == 't' && format[1] == 's' &&
                     format[3] == ':' &&
                     (format[2] == 't' || format[2] == 's' || format[2] == 'u' ||
                      format[2] == 'n')) {
            // NANOARROW_TYPE_TIMESTAMP
            //   tss:... - timestamp [seconds] with timezone “...”
            //   tsm:... - timestamp [milliseconds] with timezone “...”
            //   tsu:... - timestamp [microseconds] with timezone “...”
            //   tsn:... - timestamp [nanoseconds] with timezone “...”
            ret = ArrowArrayInitFromType(child, NANOARROW_TYPE_TIMESTAMP);
          } else if (format_len > 2 && format[0] == 'd' && format[1] == ':') {
            // NANOARROW_TYPE_DECIMAL128
            //   d:precision,scale - decimal with precision and scale
            // NANOARROW_TYPE_DECIMAL256
            //   d:precision,scale,256 - decimal with precision, scale, and width
          } else if (format_len > 4 && format[0] == '+' && format[1] == 'u' &&
                     format[3] == ':' && (format[2] == 's' || format[2] == 'd')) {
            if (format[2] == 's') {
              // NANOARROW_TYPE_SPARSE_UNION
              //   +us:I,J,... - sparse union with types I, J, ...
              ret = ArrowArrayInitFromType(child, NANOARROW_TYPE_SPARSE_UNION);
            } else {
              // NANOARROW_TYPE_DENSE_UNION
              //   +ud:I,J,... - dense union with types I, J, ...
              ret = ArrowArrayInitFromType(child, NANOARROW_TYPE_DENSE_UNION);
            }
          } else if (format_len > 3 && format[0] == '+' && format[1] == 'w' &&
                     format[2] == ':') {
            // NANOARROW_TYPE_FIXED_SIZE_LIST
            //   +w:123 - fixed-size list with 123 elements
            ret = ArrowArrayInitFromType(child, NANOARROW_TYPE_FIXED_SIZE_LIST);
          } else {
            // NANOARROW_TYPE_DICTIONARY
            //   https://arrow.apache.org/docs/format/CDataInterface.html#dictionary-encoded-arrays
            // NANOARROW_TYPE_EXTENSION
            //   https://arrow.apache.org/docs/format/CDataInterface.html#extension-arrays
            printf("    Unhandled FieldNode %zu: %s %s\r\n", i, child_schema->format,
                   child_schema->name);
          }
        }
        if (ret != NANOARROW_OK) {
          return ret;
        }
        child->length = node->length();
        child->null_count = node->null_count();
        // printf("    FieldNode %zu: %s %s\r\n", i, child_schema->format,
        // child_schema->name); printf("      child->n_buffers %lld\r\n",
        // child->n_buffers);

        // TODO_BIGQUERY:
        //   handle NANOARROW_TYPE_LIST
        //   handle NANOARROW_TYPE_LARGE_LIST
        //   handle NANOARROW_TYPE_FIXED_SIZE_LIST
        //   handle NANOARROW_TYPE_DICTIONARY
        for (int fill_buffer = 0; fill_buffer < child->n_buffers; fill_buffer++) {
          // printf("      buffer %d: ", buffer_index);
          auto buffer = buffers->Get(buffer_index);
          // printf("offset=%lld length=%lld\r\n", buffer->offset(), buffer->length());
          child->buffers[fill_buffer] =
              (const uint8_t*)(((uint64_t)(uint64_t*)body_data) + buffer->offset());
          buffer_index++;
        }
      }

      return ADBC_STATUS_OK;
    } else if (header_type ==
               org::apache::arrow::flatbuf::MessageHeader::DictionaryBatch) {
      std::cout << "dictionary batch\r\n";
      // auto dictionary_batch = header->header_as_DictionaryBatch();
      // auto id = dictionary_batch->id();
      // auto data = dictionary_batch->data();
      return ADBC_STATUS_NOT_IMPLEMENTED;
    } else {
      // The columnar IPC protocol utilizes a one-way stream of binary messages of these
      // types:
      //
      // - Schema
      // - RecordBatch
      // - DictionaryBatch
      printf("unhandled header type: %hhu\r\n", header_type);
      return ADBC_STATUS_INTERNAL;
    }
  } else {
    // error?
    printf("unexpected header type: %hhu\r\n", header_type);
    return ADBC_STATUS_INTERNAL;
  }
}

AdbcStatusCode ReadRowsIterator::init(struct AdbcError* error) {
  connection_ = ::google::cloud::bigquery_storage_v1::MakeBigQueryReadConnection();
  client_ = std::make_shared<::google::cloud::bigquery_storage_v1::BigQueryReadClient>(
      connection_);
  session_ = std::make_shared<::google::cloud::bigquery::storage::v1::ReadSession>();
  session_->set_data_format(::google::cloud::bigquery::storage::v1::DataFormat::ARROW);
  session_->set_table(table_name_);

  constexpr std::int32_t kMaxReadStreams = 1;
  auto session = client_->CreateReadSession(project_name_, *session_, kMaxReadStreams);
  if (!session) {
    auto& status = session.status();
    SetError(error, "%s%" PRId32 ", %s",
             "[bigquery] Cannot create read session: code=", status.code(),
             status.message().c_str());
    return ADBC_STATUS_INVALID_STATE;
  }

  auto response = client_->ReadRows(session->streams(0).name(), 0);
  response_ = std::make_shared<ReadRowsResponse>(std::move(response));
  session_ =
      std::make_shared<::google::cloud::bigquery::storage::v1::ReadSession>(*session);
  current_ = response_->begin();

  return ADBC_STATUS_OK;
}

int ReadRowsIterator::get_next(struct ArrowArrayStream* stream, struct ArrowArray* out) {
  if (!stream || !out) {
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  auto* ptr = reinterpret_cast<std::shared_ptr<ReadRowsIterator>*>(stream->private_data);
  if (!ptr) {
    return ADBC_STATUS_INVALID_STATE;
  }

  std::shared_ptr<ReadRowsIterator>& iterator = *ptr;
  if (iterator->current_ == iterator->response_->end()) {
    return 0;
  }

  auto& row = *iterator->current_;
  if (!row.ok()) {
    return ADBC_STATUS_INTERNAL;
  }

  struct ArrowSchema* parsed_schema = nullptr;
  if (iterator->parsed_schema_ == nullptr) {
    parsed_schema = (struct ArrowSchema*)ArrowMalloc(sizeof(struct ArrowSchema));
    memset(parsed_schema, 0, sizeof(struct ArrowSchema));

    int ret = iterator->get_schema(stream, parsed_schema);
    if (ret != ADBC_STATUS_OK) {
      return ret;
    }
    ArrowFree(parsed_schema);
  }

  auto& serialized_record_batch = row->arrow_record_batch().serialized_record_batch();
  iterator->current_++;
  return parse_encapsulated_message(
      serialized_record_batch, org::apache::arrow::flatbuf::MessageHeader::RecordBatch,
      out, iterator->parsed_schema_);
}

int ReadRowsIterator::get_schema(struct ArrowArrayStream* stream,
                                 struct ArrowSchema* out) {
  if (!stream || !out) {
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  auto* ptr = reinterpret_cast<std::shared_ptr<ReadRowsIterator>*>(stream->private_data);
  if (!ptr) {
    return ADBC_STATUS_INVALID_STATE;
  }

  std::shared_ptr<ReadRowsIterator>& iterator = *ptr;
  auto& session = iterator->session_;
  auto& serialized_schema = session->arrow_schema().serialized_schema();

  if (iterator->parsed_schema_) {
    memcpy(out, iterator->parsed_schema_, sizeof(struct ArrowSchema));
    return ADBC_STATUS_OK;
  }

  int ret = parse_encapsulated_message(serialized_schema,
                                       org::apache::arrow::flatbuf::MessageHeader::Schema,
                                       out, nullptr);
  if (ret != ADBC_STATUS_OK) {
    return ret;
  } else {
    iterator->parsed_schema_ =
        (struct ArrowSchema*)ArrowMalloc(sizeof(struct ArrowSchema));
    memcpy(iterator->parsed_schema_, out, sizeof(struct ArrowSchema));
  }

  return ADBC_STATUS_OK;
}

void ReadRowsIterator::release(struct ArrowArrayStream* stream) {
  if (stream && stream->private_data) {
    auto* ptr =
        reinterpret_cast<std::shared_ptr<ReadRowsIterator>*>(stream->private_data);
    if (ptr) {
      if ((*ptr)->parsed_schema_) {
        ArrowFree((*ptr)->parsed_schema_);
        (*ptr)->parsed_schema_ = nullptr;
      }
      delete ptr;
    }
    stream->private_data = nullptr;
  }
}

AdbcStatusCode BigqueryStatement::Bind(struct ArrowArray* values,
                                       struct ArrowSchema* schema,
                                       struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::Bind(struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::Cancel(struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::ExecuteQuery(struct ::ArrowArrayStream* stream,
                                               int64_t* rows_affected,
                                               struct AdbcError* error) {
  if (stream) {
    auto iterator = std::make_shared<ReadRowsIterator>(
        connection_->database_->project_name_, connection_->database_->table_name_);
    int ret = iterator->init(error);
    if (ret != ADBC_STATUS_OK) {
      return ret;
    }

    stream->private_data = new std::shared_ptr<ReadRowsIterator>(iterator);
    stream->get_next = ReadRowsIterator::get_next;
    stream->get_schema = ReadRowsIterator::get_schema;
    stream->release = ReadRowsIterator::release;

    if (rows_affected) {
      *rows_affected = -1;
    }
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::ExecuteSchema(struct ArrowSchema* schema,
                                                struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetOption(const char* key, char* value, size_t* length,
                                            struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetOptionBytes(const char* key, uint8_t* value,
                                                 size_t* length,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetOptionDouble(const char* key, double* value,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetOptionInt(const char* key, int64_t* value,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::GetParameterSchema(struct ArrowSchema* schema,
                                                     struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::New(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  if (!connection || !connection->private_data) {
    SetError(error, "%s", "[bigquery] Must provide an initialized AdbcConnection");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  connection_ =
      *reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::Prepare(struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::Release(struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryStatement::SetOption(const char* key, const char* value,
                                            struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::SetOptionBytes(const char* key, const uint8_t* value,
                                                 size_t length, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::SetOptionDouble(const char* key, double value,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::SetOptionInt(const char* key, int64_t value,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatement::SetSqlQuery(const char* query,
                                              struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

}  // namespace adbc_bigquery
