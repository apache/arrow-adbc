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

#ifndef NANOARROW_H_INCLUDED
#define NANOARROW_H_INCLUDED

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include "typedefs_inline.h"

#ifdef __cplusplus
extern "C" {
#endif

/// \file Arrow C Implementation
///
/// EXPERIMENTAL. Interface subject to change.

/// \page object-model Object Model
///
/// Except where noted, objects are not thread-safe and clients should
/// take care to serialize accesses to methods.
///
/// Because this library is intended to be vendored, it provides full type
/// definitions and encourages clients to stack or statically allocate
/// where convenient.

/// \defgroup nanoarrow-malloc Memory management
///
/// Non-buffer members of a struct ArrowSchema and struct ArrowArray
/// must be allocated using ArrowMalloc() or ArrowRealloc() and freed
/// using ArrowFree for schemas and arrays allocated here. Buffer members
/// are allocated using an ArrowBufferAllocator.

/// \brief Allocate like malloc()
void* ArrowMalloc(int64_t size);

/// \brief Reallocate like realloc()
void* ArrowRealloc(void* ptr, int64_t size);

/// \brief Free a pointer allocated using ArrowMalloc() or ArrowRealloc().
void ArrowFree(void* ptr);

/// \brief Return the default allocator
///
/// The default allocator uses ArrowMalloc(), ArrowRealloc(), and
/// ArrowFree().
struct ArrowBufferAllocator ArrowBufferAllocatorDefault();

/// \brief Create a custom deallocator
///
/// Creates a buffer allocator with only a free method that can be used to
/// attach a custom deallocator to an ArrowBuffer. This may be used to
/// avoid copying an existing buffer that was not allocated using the
/// infrastructure provided here (e.g., by an R or Python object).
struct ArrowBufferAllocator ArrowBufferDeallocator(
    void (*custom_free)(struct ArrowBufferAllocator* allocator, uint8_t* ptr,
                        int64_t size),
    void* private_data);

/// }@

/// \defgroup nanoarrow-errors Error handling primitives
/// Functions generally return an errno-compatible error code; functions that
/// need to communicate more verbose error information accept a pointer
/// to an ArrowError. This can be stack or statically allocated. The
/// content of the message is undefined unless an error code has been
/// returned.

/// \brief Error type containing a UTF-8 encoded message.
struct ArrowError {
  char message[1024];
};

/// \brief Set the contents of an error using printf syntax
ArrowErrorCode ArrowErrorSet(struct ArrowError* error, const char* fmt, ...);

/// \brief Get the contents of an error
const char* ArrowErrorMessage(struct ArrowError* error);

/// }@

/// \defgroup nanoarrow-utils Utility data structures

/// \brief Initialize a description of buffer arrangements from a storage type
void ArrowLayoutInit(struct ArrowLayout* layout, enum ArrowType storage_type);

/// \brief Create a string view from a null-terminated string
static inline struct ArrowStringView ArrowCharView(const char* value);

/// \brief Arrow time unit enumerator
///
/// These names and values map to the corresponding arrow::TimeUnit::type
/// enumerator.
enum ArrowTimeUnit {
  NANOARROW_TIME_UNIT_SECOND = 0,
  NANOARROW_TIME_UNIT_MILLI = 1,
  NANOARROW_TIME_UNIT_MICRO = 2,
  NANOARROW_TIME_UNIT_NANO = 3
};

/// }@

/// \defgroup nanoarrow-schema Schema producer helpers
/// These functions allocate, copy, and destroy ArrowSchema structures

/// \brief Initialize the fields of a schema
///
/// Initializes the fields and release callback of schema_out. Caller
/// is responsible for calling the schema->release callback if
/// NANOARROW_OK is returned.
ArrowErrorCode ArrowSchemaInit(struct ArrowSchema* schema, enum ArrowType type);

/// \brief Initialize the fields of a fixed-size schema
///
/// Returns EINVAL for fixed_size <= 0 or for data_type that is not
/// NANOARROW_TYPE_FIXED_SIZE_BINARY or NANOARROW_TYPE_FIXED_SIZE_LIST.
ArrowErrorCode ArrowSchemaInitFixedSize(struct ArrowSchema* schema,
                                        enum ArrowType data_type, int32_t fixed_size);

/// \brief Initialize the fields of a decimal schema
///
/// Returns EINVAL for scale <= 0 or for data_type that is not
/// NANOARROW_TYPE_DECIMAL128 or NANOARROW_TYPE_DECIMAL256.
ArrowErrorCode ArrowSchemaInitDecimal(struct ArrowSchema* schema,
                                      enum ArrowType data_type, int32_t decimal_precision,
                                      int32_t decimal_scale);

/// \brief Initialize the fields of a time, timestamp, or duration schema
///
/// Returns EINVAL for data_type that is not
/// NANOARROW_TYPE_TIME32, NANOARROW_TYPE_TIME64,
/// NANOARROW_TYPE_TIMESTAMP, or NANOARROW_TYPE_DURATION. The
/// timezone parameter must be NULL for a non-timestamp data_type.
ArrowErrorCode ArrowSchemaInitDateTime(struct ArrowSchema* schema,
                                       enum ArrowType data_type,
                                       enum ArrowTimeUnit time_unit,
                                       const char* timezone);

/// \brief Make a (recursive) copy of a schema
///
/// Allocates and copies fields of schema into schema_out.
ArrowErrorCode ArrowSchemaDeepCopy(struct ArrowSchema* schema,
                                   struct ArrowSchema* schema_out);

/// \brief Copy format into schema->format
///
/// schema must have been allocated using ArrowSchemaInit or
/// ArrowSchemaDeepCopy.
ArrowErrorCode ArrowSchemaSetFormat(struct ArrowSchema* schema, const char* format);

/// \brief Copy name into schema->name
///
/// schema must have been allocated using ArrowSchemaInit or
/// ArrowSchemaDeepCopy.
ArrowErrorCode ArrowSchemaSetName(struct ArrowSchema* schema, const char* name);

/// \brief Copy metadata into schema->metadata
///
/// schema must have been allocated using ArrowSchemaInit or
/// ArrowSchemaDeepCopy.
ArrowErrorCode ArrowSchemaSetMetadata(struct ArrowSchema* schema, const char* metadata);

/// \brief Allocate the schema->children array
///
/// Includes the memory for each child struct ArrowSchema.
/// schema must have been allocated using ArrowSchemaInit or
/// ArrowSchemaDeepCopy.
ArrowErrorCode ArrowSchemaAllocateChildren(struct ArrowSchema* schema,
                                           int64_t n_children);

/// \brief Allocate the schema->dictionary member
///
/// schema must have been allocated using ArrowSchemaInit or
/// ArrowSchemaDeepCopy.
ArrowErrorCode ArrowSchemaAllocateDictionary(struct ArrowSchema* schema);

/// \brief Reader for key/value pairs in schema metadata
struct ArrowMetadataReader {
  const char* metadata;
  int64_t offset;
  int32_t remaining_keys;
};

/// \brief Initialize an ArrowMetadataReader
ArrowErrorCode ArrowMetadataReaderInit(struct ArrowMetadataReader* reader,
                                       const char* metadata);

/// \brief Read the next key/value pair from an ArrowMetadataReader
ArrowErrorCode ArrowMetadataReaderRead(struct ArrowMetadataReader* reader,
                                       struct ArrowStringView* key_out,
                                       struct ArrowStringView* value_out);

/// \brief The number of bytes in in a key/value metadata string
int64_t ArrowMetadataSizeOf(const char* metadata);

/// \brief Check for a key in schema metadata
char ArrowMetadataHasKey(const char* metadata, struct ArrowStringView key);

/// \brief Extract a value from schema metadata
///
/// If key does not exist in metadata, value_out is unmodified
ArrowErrorCode ArrowMetadataGetValue(const char* metadata, struct ArrowStringView key,
                                     struct ArrowStringView* value_out);

/// \brief Initialize a builder for schema metadata from key/value pairs
///
/// metadata can be an existing metadata string or NULL to initialize
/// an empty metadata string.
ArrowErrorCode ArrowMetadataBuilderInit(struct ArrowBuffer* buffer, const char* metadata);

/// \brief Append a key/value pair to a buffer containing serialized metadata
ArrowErrorCode ArrowMetadataBuilderAppend(struct ArrowBuffer* buffer,
                                          struct ArrowStringView key,
                                          struct ArrowStringView value);

/// \brief Set a key/value pair to a buffer containing serialized metadata
///
/// Ensures that the only entry for key in the metadata is set to value.
/// This function maintains the existing position of (the first instance of)
/// key if present in the data.
ArrowErrorCode ArrowMetadataBuilderSet(struct ArrowBuffer* buffer,
                                       struct ArrowStringView key,
                                       struct ArrowStringView value);

/// \brief Remove a key from a buffer containing serialized metadata
ArrowErrorCode ArrowMetadataBuilderRemove(struct ArrowBuffer* buffer,
                                          struct ArrowStringView key);

/// }@

/// \defgroup nanoarrow-schema-view Schema consumer helpers

/// \brief A non-owning view of a parsed ArrowSchema
///
/// Contains more readily extractable values than a raw ArrowSchema.
/// Clients can stack or statically allocate this structure but are
/// encouraged to use the provided getters to ensure forward
/// compatiblity.
struct ArrowSchemaView {
  /// \brief A pointer to the schema represented by this view
  struct ArrowSchema* schema;

  /// \brief The data type represented by the schema
  ///
  /// This value may be NANOARROW_TYPE_DICTIONARY if the schema has a
  /// non-null dictionary member; datetime types are valid values.
  /// This value will never be NANOARROW_TYPE_EXTENSION (see
  /// extension_name and/or extension_metadata to check for
  /// an extension type).
  enum ArrowType data_type;

  /// \brief The storage data type represented by the schema
  ///
  /// This value will never be NANOARROW_TYPE_DICTIONARY, NANOARROW_TYPE_EXTENSION
  /// or any datetime type. This value represents only the type required to
  /// interpret the buffers in the array.
  enum ArrowType storage_data_type;

  /// \brief The storage layout represented by the schema
  struct ArrowLayout layout;

  /// \brief The extension type name if it exists
  ///
  /// If the ARROW:extension:name key is present in schema.metadata,
  /// extension_name.data will be non-NULL.
  struct ArrowStringView extension_name;

  /// \brief The extension type metadata if it exists
  ///
  /// If the ARROW:extension:metadata key is present in schema.metadata,
  /// extension_metadata.data will be non-NULL.
  struct ArrowStringView extension_metadata;

  /// \brief Format fixed size parameter
  ///
  /// This value is set when parsing a fixed-size binary or fixed-size
  /// list schema; this value is undefined for other types. For a
  /// fixed-size binary schema this value is in bytes; for a fixed-size
  /// list schema this value refers to the number of child elements for
  /// each element of the parent.
  int32_t fixed_size;

  /// \brief Decimal bitwidth
  ///
  /// This value is set when parsing a decimal type schema;
  /// this value is undefined for other types.
  int32_t decimal_bitwidth;

  /// \brief Decimal precision
  ///
  /// This value is set when parsing a decimal type schema;
  /// this value is undefined for other types.
  int32_t decimal_precision;

  /// \brief Decimal scale
  ///
  /// This value is set when parsing a decimal type schema;
  /// this value is undefined for other types.
  int32_t decimal_scale;

  /// \brief Format time unit parameter
  ///
  /// This value is set when parsing a date/time type. The value is
  /// undefined for other types.
  enum ArrowTimeUnit time_unit;

  /// \brief Format timezone parameter
  ///
  /// This value is set when parsing a timestamp type and represents
  /// the timezone format parameter. The ArrowStrintgView points to
  /// data within the schema and the value is undefined for other types.
  struct ArrowStringView timezone;

  /// \brief Union type ids parameter
  ///
  /// This value is set when parsing a union type and represents
  /// type ids parameter. The ArrowStringView points to
  /// data within the schema and the value is undefined for other types.
  struct ArrowStringView union_type_ids;
};

/// \brief Initialize an ArrowSchemaView
ArrowErrorCode ArrowSchemaViewInit(struct ArrowSchemaView* schema_view,
                                   struct ArrowSchema* schema, struct ArrowError* error);

/// }@

/// \defgroup nanoarrow-buffer Owning, growable buffers

/// \brief Initialize an ArrowBuffer
///
/// Initialize a buffer with a NULL, zero-size buffer using the default
/// buffer allocator.
static inline void ArrowBufferInit(struct ArrowBuffer* buffer);

/// \brief Set a newly-initialized buffer's allocator
///
/// Returns EINVAL if the buffer has already been allocated.
static inline ArrowErrorCode ArrowBufferSetAllocator(
    struct ArrowBuffer* buffer, struct ArrowBufferAllocator allocator);

/// \brief Reset an ArrowBuffer
///
/// Releases the buffer using the allocator's free method if
/// the buffer's data member is non-null, sets the data member
/// to NULL, and sets the buffer's size and capacity to 0.
static inline void ArrowBufferReset(struct ArrowBuffer* buffer);

/// \brief Move an ArrowBuffer
///
/// Transfers the buffer data and lifecycle management to another
/// address and resets buffer.
static inline void ArrowBufferMove(struct ArrowBuffer* buffer,
                                   struct ArrowBuffer* buffer_out);

/// \brief Grow or shrink a buffer to a given capacity
///
/// When shrinking the capacity of the buffer, the buffer is only reallocated
/// if shrink_to_fit is non-zero. Calling ArrowBufferResize() does not
/// adjust the buffer's size member except to ensure that the invariant
/// capacity >= size remains true.
static inline ArrowErrorCode ArrowBufferResize(struct ArrowBuffer* buffer,
                                               int64_t new_capacity_bytes,
                                               char shrink_to_fit);

/// \brief Ensure a buffer has at least a given additional capacity
///
/// Ensures that the buffer has space to append at least
/// additional_size_bytes, overallocating when required.
static inline ArrowErrorCode ArrowBufferReserve(struct ArrowBuffer* buffer,
                                                int64_t additional_size_bytes);

/// \brief Write data to buffer and increment the buffer size
///
/// This function does not check that buffer has the required capacity
static inline void ArrowBufferAppendUnsafe(struct ArrowBuffer* buffer, const void* data,
                                           int64_t size_bytes);

/// \brief Write data to buffer and increment the buffer size
///
/// This function writes and ensures that the buffer has the required capacity,
/// possibly by reallocating the buffer. Like ArrowBufferReserve, this will
/// overallocate when reallocation is required.
static inline ArrowErrorCode ArrowBufferAppend(struct ArrowBuffer* buffer,
                                               const void* data, int64_t size_bytes);

/// \brief Write fill to buffer and increment the buffer size
///
/// This function writes the specified number of fill bytes and
/// ensures that the buffer has the required capacity,
static inline ArrowErrorCode ArrowBufferAppendFill(struct ArrowBuffer* buffer,
                                                   uint8_t value, int64_t size_bytes);

/// \brief Write an 8-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendInt8(struct ArrowBuffer* buffer,
                                                   int8_t value);

/// \brief Write an unsigned 8-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendUInt8(struct ArrowBuffer* buffer,
                                                    uint8_t value);

/// \brief Write a 16-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendInt16(struct ArrowBuffer* buffer,
                                                    int16_t value);

/// \brief Write an unsigned 16-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendUInt16(struct ArrowBuffer* buffer,
                                                     uint16_t value);

/// \brief Write a 32-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendInt32(struct ArrowBuffer* buffer,
                                                    int32_t value);

/// \brief Write an unsigned 32-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendUInt32(struct ArrowBuffer* buffer,
                                                     uint32_t value);

/// \brief Write a 64-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendInt64(struct ArrowBuffer* buffer,
                                                    int64_t value);

/// \brief Write an unsigned 64-bit integer to a buffer
static inline ArrowErrorCode ArrowBufferAppendUInt64(struct ArrowBuffer* buffer,
                                                     uint64_t value);

/// \brief Write a double to a buffer
static inline ArrowErrorCode ArrowBufferAppendDouble(struct ArrowBuffer* buffer,
                                                     double value);

/// \brief Write a float to a buffer
static inline ArrowErrorCode ArrowBufferAppendFloat(struct ArrowBuffer* buffer,
                                                    float value);

/// }@

/// \defgroup nanoarrow-bitmap Bitmap utilities

/// \brief Extract a boolean value from a bitmap
static inline int8_t ArrowBitGet(const uint8_t* bits, int64_t i);

/// \brief Set a boolean value to a bitmap to true
static inline void ArrowBitSet(uint8_t* bits, int64_t i);

/// \brief Set a boolean value to a bitmap to false
static inline void ArrowBitClear(uint8_t* bits, int64_t i);

/// \brief Set a boolean value to a bitmap
static inline void ArrowBitSetTo(uint8_t* bits, int64_t i, uint8_t value);

/// \brief Set a boolean value to a range in a bitmap
static inline void ArrowBitsSetTo(uint8_t* bits, int64_t start_offset, int64_t length,
                                  uint8_t bits_are_set);

/// \brief Count true values in a bitmap
static inline int64_t ArrowBitCountSet(const uint8_t* bits, int64_t i_from, int64_t i_to);

/// \brief Initialize an ArrowBitmap
///
/// Initialize the builder's buffer, empty its cache, and reset the size to zero
static inline void ArrowBitmapInit(struct ArrowBitmap* bitmap);

/// \brief Ensure a bitmap builder has at least a given additional capacity
///
/// Ensures that the buffer has space to append at least
/// additional_size_bits, overallocating when required.
static inline ArrowErrorCode ArrowBitmapReserve(struct ArrowBitmap* bitmap,
                                                int64_t additional_size_bits);

/// \brief Grow or shrink a bitmap to a given capacity
///
/// When shrinking the capacity of the bitmap, the bitmap is only reallocated
/// if shrink_to_fit is non-zero. Calling ArrowBitmapResize() does not
/// adjust the buffer's size member except when shrinking new_capacity_bits
/// to a value less than the current number of bits in the bitmap.
static inline ArrowErrorCode ArrowBitmapResize(struct ArrowBitmap* bitmap,
                                               int64_t new_capacity_bits,
                                               char shrink_to_fit);

/// \brief Reserve space for and append zero or more of the same boolean value to a bitmap
static inline ArrowErrorCode ArrowBitmapAppend(struct ArrowBitmap* bitmap,
                                               uint8_t bits_are_set, int64_t length);

/// \brief Append zero or more of the same boolean value to a bitmap
static inline void ArrowBitmapAppendUnsafe(struct ArrowBitmap* bitmap,
                                           uint8_t bits_are_set, int64_t length);

/// \brief Append boolean values encoded as int8_t to a bitmap
///
/// The values must all be 0 or 1.
static inline void ArrowBitmapAppendInt8Unsafe(struct ArrowBitmap* bitmap,
                                               const int8_t* values, int64_t n_values);

/// \brief Append boolean values encoded as int32_t to a bitmap
///
/// The values must all be 0 or 1.
static inline void ArrowBitmapAppendInt32Unsafe(struct ArrowBitmap* bitmap,
                                                const int32_t* values, int64_t n_values);

/// \brief Reset a bitmap builder
///
/// Releases any memory held by buffer, empties the cache, and resets the size to zero
static inline void ArrowBitmapReset(struct ArrowBitmap* bitmap);

/// }@

/// \defgroup nanoarrow-array Array producer helpers
/// These functions allocate, copy, and destroy ArrowArray structures

/// \brief Initialize the fields of an array
///
/// Initializes the fields and release callback of array. Caller
/// is responsible for calling the array->release callback if
/// NANOARROW_OK is returned.
ArrowErrorCode ArrowArrayInit(struct ArrowArray* array, enum ArrowType storage_type);

/// \brief Initialize the contents of an ArrowArray from an ArrowSchema
///
/// Caller is responsible for calling the array->release callback if
/// NANOARROW_OK is returned.
ArrowErrorCode ArrowArrayInitFromSchema(struct ArrowArray* array,
                                        struct ArrowSchema* schema,
                                        struct ArrowError* error);

/// \brief Allocate the array->children array
///
/// Includes the memory for each child struct ArrowArray,
/// whose members are marked as released and may be subsequently initialized
/// with ArrowArrayInit or moved from an existing ArrowArray.
/// schema must have been allocated using ArrowArrayInit.
ArrowErrorCode ArrowArrayAllocateChildren(struct ArrowArray* array, int64_t n_children);

/// \brief Allocate the array->dictionary member
///
/// Includes the memory for the struct ArrowArray, whose contents
/// is marked as released and may be subsequently initialized
/// with ArrowArrayInit or moved from an existing ArrowArray.
/// array must have been allocated using ArrowArrayInit
ArrowErrorCode ArrowArrayAllocateDictionary(struct ArrowArray* array);

/// \brief Set the validity bitmap of an ArrowArray
///
/// array must have been allocated using ArrowArrayInit
void ArrowArraySetValidityBitmap(struct ArrowArray* array, struct ArrowBitmap* bitmap);

/// \brief Set a buffer of an ArrowArray
///
/// array must have been allocated using ArrowArrayInit
ArrowErrorCode ArrowArraySetBuffer(struct ArrowArray* array, int64_t i,
                                   struct ArrowBuffer* buffer);

/// \brief Get the validity bitmap of an ArrowArray
///
/// array must have been allocated using ArrowArrayInit
static inline struct ArrowBitmap* ArrowArrayValidityBitmap(struct ArrowArray* array);

/// \brief Get a buffer of an ArrowArray
///
/// array must have been allocated using ArrowArrayInit
static inline struct ArrowBuffer* ArrowArrayBuffer(struct ArrowArray* array, int64_t i);

/// \brief Start element-wise appending to an ArrowArray
///
/// Initializes any values needed to use ArrowArrayAppend*() functions.
/// All element-wise appenders append by value and return EINVAL if the exact value
/// cannot be represented by the underlying storage type.
/// array must have been allocated using ArrowArrayInit
static inline ArrowErrorCode ArrowArrayStartAppending(struct ArrowArray* array);

/// \brief Reserve space for future appends
///
/// For buffer sizes that can be calculated (i.e., not string data buffers or
/// child array sizes for non-fixed-size arrays), recursively reserve space for
/// additional elements. This is useful for reducing the number of reallocations
/// that occur using the item-wise appenders.
ArrowErrorCode ArrowArrayReserve(struct ArrowArray* array,
                                 int64_t additional_size_elements);

/// \brief Append a null value to an array
static inline ArrowErrorCode ArrowArrayAppendNull(struct ArrowArray* array, int64_t n);

/// \brief Append a signed integer value to an array
///
/// Returns NANOARROW_OK if value can be exactly represented by
/// the underlying storage type or EINVAL otherwise (e.g., value
/// is outside the valid array range).
static inline ArrowErrorCode ArrowArrayAppendInt(struct ArrowArray* array, int64_t value);

/// \brief Append an unsigned integer value to an array
///
/// Returns NANOARROW_OK if value can be exactly represented by
/// the underlying storage type or EINVAL otherwise (e.g., value
/// is outside the valid array range).
static inline ArrowErrorCode ArrowArrayAppendUInt(struct ArrowArray* array,
                                                  uint64_t value);

/// \brief Append a double value to an array
///
/// Returns NANOARROW_OK if value can be exactly represented by
/// the underlying storage type or EINVAL otherwise (e.g., value
/// is outside the valid array range or there is an attempt to append
/// a non-integer to an array with an integer storage type).
static inline ArrowErrorCode ArrowArrayAppendDouble(struct ArrowArray* array,
                                                    double value);

/// \brief Append a string of bytes to an array
///
/// Returns NANOARROW_OK if value can be exactly represented by
/// the underlying storage type or EINVAL otherwise (e.g.,
/// the underlying array is not a binary, string, large binary, large string,
/// or fixed-size binary array, or value is the wrong size for a fixed-size
/// binary array).
static inline ArrowErrorCode ArrowArrayAppendBytes(struct ArrowArray* array,
                                                   struct ArrowBufferView value);

/// \brief Append a string value to an array
/// Returns NANOARROW_OK if value can be exactly represented by
/// the underlying storage type or EINVAL otherwise (e.g.,
/// the underlying array is not a string or large string array).
static inline ArrowErrorCode ArrowArrayAppendString(struct ArrowArray* array,
                                                    struct ArrowStringView value);

/// \brief Finish a nested array element
///
/// Appends a non-null element to the array based on the first child's current
/// length. Returns NANOARROW_OK if the item was successfully added or EINVAL
/// if the underlying storage type is not a struct, list, large list, or fixed-size
/// list, or if there was an attempt to add a struct or fixed-size list element where the
/// length of the child array(s) did not match the expected length.
static inline ArrowErrorCode ArrowArrayFinishElement(struct ArrowArray* array);

/// \brief Shrink buffer capacity to the size required
///
/// Also applies shrinking to any child arrays. array must have been allocated using
/// ArrowArrayInit
static inline ArrowErrorCode ArrowArrayShrinkToFit(struct ArrowArray* array);

/// \brief Finish building an ArrowArray
///
/// Flushes any pointers from internal buffers that may have been reallocated
/// into the array->buffers array and checks the actual size of the buffers
/// against the expected size based on the final length.
/// array must have been allocated using ArrowArrayInit
ArrowErrorCode ArrowArrayFinishBuilding(struct ArrowArray* array,
                                        struct ArrowError* error);

/// }@

/// \defgroup nanoarrow-array Array consumer helpers
/// These functions read and validate the contents ArrowArray structures

/// \brief Initialize the contents of an ArrowArrayView
void ArrowArrayViewInit(struct ArrowArrayView* array_view, enum ArrowType storage_type);

/// \brief Initialize the contents of an ArrowArrayView from an ArrowSchema
ArrowErrorCode ArrowArrayViewInitFromSchema(struct ArrowArrayView* array_view,
                                            struct ArrowSchema* schema,
                                            struct ArrowError* error);

/// \brief Allocate the schema_view->children array
///
/// Includes the memory for each child struct ArrowArrayView
ArrowErrorCode ArrowArrayViewAllocateChildren(struct ArrowArrayView* array_view,
                                              int64_t n_children);

/// \brief Set data-independent buffer sizes from length
void ArrowArrayViewSetLength(struct ArrowArrayView* array_view, int64_t length);

/// \brief Set buffer sizes and data pointers from an ArrowArray
ArrowErrorCode ArrowArrayViewSetArray(struct ArrowArrayView* array_view,
                                      struct ArrowArray* array, struct ArrowError* error);

/// \brief Reset the contents of an ArrowArrayView and frees resources
void ArrowArrayViewReset(struct ArrowArrayView* array_view);

/// }@

// Inline function definitions
#include "array_inline.h"
#include "bitmap_inline.h"
#include "buffer_inline.h"
#include "utils_inline.h"

#ifdef __cplusplus
}
#endif

#endif
