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

#include <exception>
#include <string>
#include <vector>

#include "nanoarrow.h"

#ifndef NANOARROW_HPP_INCLUDED
#define NANOARROW_HPP_INCLUDED

/// \defgroup nanoarrow_hpp Nanoarrow C++ Helpers
///
/// The utilities provided in this file are intended to support C++ users
/// of the nanoarrow C library such that C++-style resource allocation
/// and error handling can be used with nanoarrow data structures.
/// These utilities are not intended to mirror the nanoarrow C API.

namespace nanoarrow {

/// \defgroup nanoarrow_hpp-errors Error handling helpers
///
/// Most functions in the C API return an ArrowErrorCode to communicate
/// possible failure. Except where documented, it is usually not safe to
/// continue after a non-zero value has been returned. While the
/// nanoarrow C++ helpers do not throw any exceptions of their own,
/// these helpers are provided to facilitate using the nanoarrow C++ helpers
/// in frameworks where this is a useful error handling idiom.
///
/// @{

class Exception : public std::exception {
 public:
  Exception(const std::string& msg) : msg_(msg) {}
  const char* what() const noexcept { return msg_.c_str(); }

 private:
  std::string msg_;
};

#if defined(NANOARROW_DEBUG)
#define _NANOARROW_THROW_NOT_OK_IMPL(NAME, EXPR, EXPR_STR)                      \
  do {                                                                          \
    const int NAME = (EXPR);                                                    \
    if (NAME) {                                                                 \
      throw nanoarrow::Exception(                                               \
          std::string(EXPR_STR) + std::string(" failed with errno ") +          \
          std::to_string(NAME) + std::string("\n * ") + std::string(__FILE__) + \
          std::string(":") + std::to_string(__LINE__) + std::string("\n"));     \
    }                                                                           \
  } while (0)
#else
#define _NANOARROW_THROW_NOT_OK_IMPL(NAME, EXPR, EXPR_STR)            \
  do {                                                                \
    const int NAME = (EXPR);                                          \
    if (NAME) {                                                       \
      throw nanoarrow::Exception(std::string(EXPR_STR) +              \
                                 std::string(" failed with errno ") + \
                                 std::to_string(NAME));               \
    }                                                                 \
  } while (0)
#endif

#define NANOARROW_THROW_NOT_OK(EXPR)                                                   \
  _NANOARROW_THROW_NOT_OK_IMPL(_NANOARROW_MAKE_NAME(errno_status_, __COUNTER__), EXPR, \
                               #EXPR)

/// @}

namespace internal {

/// \defgroup nanoarrow_hpp-unique_base Base classes for Unique wrappers
///
/// @{

template <typename T>
static inline void init_pointer(T* data);

template <typename T>
static inline void move_pointer(T* src, T* dst);

template <typename T>
static inline void release_pointer(T* data);

template <>
inline void init_pointer(struct ArrowSchema* data) {
  data->release = nullptr;
}

template <>
inline void move_pointer(struct ArrowSchema* src, struct ArrowSchema* dst) {
  ArrowSchemaMove(src, dst);
}

template <>
inline void release_pointer(struct ArrowSchema* data) {
  if (data->release != nullptr) {
    data->release(data);
  }
}

template <>
inline void init_pointer(struct ArrowArray* data) {
  data->release = nullptr;
}

template <>
inline void move_pointer(struct ArrowArray* src, struct ArrowArray* dst) {
  ArrowArrayMove(src, dst);
}

template <>
inline void release_pointer(struct ArrowArray* data) {
  if (data->release != nullptr) {
    data->release(data);
  }
}

template <>
inline void init_pointer(struct ArrowArrayStream* data) {
  data->release = nullptr;
}

template <>
inline void move_pointer(struct ArrowArrayStream* src, struct ArrowArrayStream* dst) {
  ArrowArrayStreamMove(src, dst);
}

template <>
inline void release_pointer(ArrowArrayStream* data) {
  if (data->release != nullptr) {
    data->release(data);
  }
}

template <>
inline void init_pointer(struct ArrowBuffer* data) {
  ArrowBufferInit(data);
}

template <>
inline void move_pointer(struct ArrowBuffer* src, struct ArrowBuffer* dst) {
  ArrowBufferMove(src, dst);
}

template <>
inline void release_pointer(struct ArrowBuffer* data) {
  ArrowBufferReset(data);
}

template <>
inline void init_pointer(struct ArrowBitmap* data) {
  ArrowBitmapInit(data);
}

template <>
inline void move_pointer(struct ArrowBitmap* src, struct ArrowBitmap* dst) {
  ArrowBitmapMove(src, dst);
}

template <>
inline void release_pointer(struct ArrowBitmap* data) {
  ArrowBitmapReset(data);
}

template <>
inline void init_pointer(struct ArrowArrayView* data) {
  ArrowArrayViewInitFromType(data, NANOARROW_TYPE_UNINITIALIZED);
}

template <>
inline void move_pointer(struct ArrowArrayView* src, struct ArrowArrayView* dst) {
  ArrowArrayViewMove(src, dst);
}

template <>
inline void release_pointer(struct ArrowArrayView* data) {
  ArrowArrayViewReset(data);
}

/// \brief A unique_ptr-like base class for stack-allocatable objects
/// \tparam T The object type
template <typename T>
class Unique {
 public:
  /// \brief Construct an invalid instance of T holding no resources
  Unique() { init_pointer(&data_); }

  /// \brief Move and take ownership of data
  Unique(T* data) { move_pointer(data, &data_); }

  /// \brief Move and take ownership of data wrapped by rhs
  Unique(Unique&& rhs) : Unique(rhs.get()) {}
  Unique& operator=(Unique&& rhs) {
    reset(rhs.get());
    return *this;
  }

  // These objects are not copyable
  Unique(const Unique& rhs) = delete;

  /// \brief Get a pointer to the data owned by this object
  T* get() noexcept { return &data_; }
  const T* get() const noexcept { return &data_; }

  /// \brief Use the pointer operator to access fields of this object
  T* operator->() noexcept { return &data_; }
  const T* operator->() const noexcept { return &data_; }

  /// \brief Call data's release callback if valid
  void reset() { release_pointer(&data_); }

  /// \brief Call data's release callback if valid and move ownership of the data
  /// pointed to by data
  void reset(T* data) {
    reset();
    move_pointer(data, &data_);
  }

  /// \brief Move ownership of this object to the data pointed to by out
  void move(T* out) { move_pointer(&data_, out); }

  ~Unique() { reset(); }

 protected:
  T data_;
};

template <typename T>
static inline void DeallocateWrappedBuffer(struct ArrowBufferAllocator* allocator,
                                           uint8_t* ptr, int64_t size) {
  NANOARROW_UNUSED(ptr);
  NANOARROW_UNUSED(size);
  auto obj = reinterpret_cast<T*>(allocator->private_data);
  delete obj;
}

/// @}

}  // namespace internal

/// \defgroup nanoarrow_hpp-unique Unique object wrappers
///
/// The Arrow C Data interface, the Arrow C Stream interface, and the
/// nanoarrow C library use stack-allocatable objects, some of which
/// require initialization or cleanup.
///
/// @{

/// \brief Class wrapping a unique struct ArrowSchema
using UniqueSchema = internal::Unique<struct ArrowSchema>;

/// \brief Class wrapping a unique struct ArrowArray
using UniqueArray = internal::Unique<struct ArrowArray>;

/// \brief Class wrapping a unique struct ArrowArrayStream
using UniqueArrayStream = internal::Unique<struct ArrowArrayStream>;

/// \brief Class wrapping a unique struct ArrowBuffer
using UniqueBuffer = internal::Unique<struct ArrowBuffer>;

/// \brief Class wrapping a unique struct ArrowBitmap
using UniqueBitmap = internal::Unique<struct ArrowBitmap>;

/// \brief Class wrapping a unique struct ArrowArrayView
using UniqueArrayView = internal::Unique<struct ArrowArrayView>;

/// @}

/// \defgroup nanoarrow_hpp-buffer Buffer helpers
///
/// Helpers to wrap buffer-like C++ objects as ArrowBuffer objects that can
/// be used to build ArrowArray objects.
///
/// @{

/// \brief Initialize a buffer wrapping an arbitrary C++ object
///
/// Initializes a buffer with a release callback that deletes the moved obj
/// when ArrowBufferReset is called. This version is useful for wrapping
/// an object whose .data() member is missing or unrelated to the buffer
/// value that is destined for a the buffer of an ArrowArray. T must be movable.
template <typename T>
static inline void BufferInitWrapped(struct ArrowBuffer* buffer, T obj,
                                     const uint8_t* data, int64_t size_bytes) {
  T* obj_moved = new T(std::move(obj));
  buffer->data = const_cast<uint8_t*>(data);
  buffer->size_bytes = size_bytes;
  buffer->capacity_bytes = 0;
  buffer->allocator =
      ArrowBufferDeallocator(&internal::DeallocateWrappedBuffer<T>, obj_moved);
}

/// \brief Initialize a buffer wrapping a C++ sequence
///
/// Specifically, this uses obj.data() to set the buffer address and
/// obj.size() * sizeof(T::value_type) to set the buffer size. This works
/// for STL containers like std::vector, std::array, and std::string.
/// This function moves obj and ensures it is deleted when ArrowBufferReset
/// is called.
template <typename T>
void BufferInitSequence(struct ArrowBuffer* buffer, T obj) {
  // Move before calling .data() (matters sometimes).
  T* obj_moved = new T(std::move(obj));
  buffer->data =
      const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(obj_moved->data()));
  buffer->size_bytes = obj_moved->size() * sizeof(typename T::value_type);
  buffer->capacity_bytes = 0;
  buffer->allocator =
      ArrowBufferDeallocator(&internal::DeallocateWrappedBuffer<T>, obj_moved);
}

/// @}

/// \defgroup nanoarrow_hpp-array-stream ArrayStream helpers
///
/// These classes provide simple ArrowArrayStream implementations that
/// can be extended to help simplify the process of creating a valid
/// ArrowArrayStream implementation or used as-is for testing.
///
/// @{

/// @brief Export an ArrowArrayStream from a standard C++ class
/// @tparam T A class with methods `int GetSchema(ArrowSchema*)`, `int
/// GetNext(ArrowArray*)`, and `const char* GetLastError()`
///
/// This class allows a standard C++ class to be exported to a generic ArrowArrayStream
/// consumer by mapping C callback invocations to method calls on an instance of the
/// object whose lifecycle is owned by the ArrowArrayStream. See VectorArrayStream for
/// minimal useful example of this pattern.
///
/// The methods must be accessible to the ArrayStreamFactory, either as public methods or
/// by declaring ArrayStreamFactory<ImplClass> a friend. Implementors are encouraged (but
/// not required) to implement a ToArrayStream(ArrowArrayStream*) that creates a new
/// instance owned by the ArrowArrayStream and moves the relevant data to that instance.
///
/// An example implementation might be:
///
/// \code
/// class StreamImpl {
///  public:
///   // Public methods (e.g., constructor) used from C++ to initialize relevant data
///
///   // Idiomatic exporter to move data + lifecycle responsibility to an instance
///   // managed by the ArrowArrayStream callbacks
///   void ToArrayStream(struct ArrowArrayStream* out) {
///     ArrayStreamFactory<StreamImpl>::InitArrayStream(new StreamImpl(...), out);
///   }
///
///  private:
///   // Make relevant methods available to the ArrayStreamFactory
///   friend class ArrayStreamFactory<StreamImpl>;
///
///   // Method implementations (called from C, not normally interacted with from C++)
///   int GetSchema(struct ArrowSchema* schema) { return ENOTSUP; }
///   int GetNext(struct ArrowArray* array) { return ENOTSUP; }
///   const char* GetLastError() { nullptr; }
/// };
/// \endcode
///
/// An example usage might be:
///
/// \code
/// // Call constructor and/or public methods to initialize relevant data
/// StreamImpl impl;
///
/// // Export to ArrowArrayStream after data are finalized
/// UniqueArrayStream stream;
/// impl.ToArrayStream(stream.get());
/// \endcode
template <typename T>
class ArrayStreamFactory {
 public:
  /// \brief Take ownership of instance and populate callbacks of out
  static void InitArrayStream(T* instance, struct ArrowArrayStream* out) {
    out->get_schema = &get_schema_wrapper;
    out->get_next = &get_next_wrapper;
    out->get_last_error = &get_last_error_wrapper;
    out->release = &release_wrapper;
    out->private_data = instance;
  }

 private:
  static int get_schema_wrapper(struct ArrowArrayStream* stream,
                                struct ArrowSchema* schema) {
    return reinterpret_cast<T*>(stream->private_data)->GetSchema(schema);
  }

  static int get_next_wrapper(struct ArrowArrayStream* stream, struct ArrowArray* array) {
    return reinterpret_cast<T*>(stream->private_data)->GetNext(array);
  }

  static const char* get_last_error_wrapper(struct ArrowArrayStream* stream) {
    return reinterpret_cast<T*>(stream->private_data)->GetLastError();
  }

  static void release_wrapper(struct ArrowArrayStream* stream) {
    delete reinterpret_cast<T*>(stream->private_data);
    stream->release = nullptr;
    stream->private_data = nullptr;
  }
};

/// \brief An empty array stream
///
/// This class can be constructed from an struct ArrowSchema and implements a default
/// get_next() method that always marks the output ArrowArray as released.
///
/// DEPRECATED (0.4.0): Early versions of nanoarrow allowed subclasses to override
/// get_schema(), get_next(), and get_last_error(). This functionality will be removed
/// in a future release: use the pattern documented in ArrayStreamFactory to create
/// custom ArrowArrayStream implementations.
class EmptyArrayStream {
 public:
  /// \brief Create an EmptyArrayStream from an ArrowSchema
  ///
  /// Takes ownership of schema.
  EmptyArrayStream(struct ArrowSchema* schema) : schema_(schema) {
    ArrowErrorInit(&error_);
  }

  /// \brief Export to ArrowArrayStream
  void ToArrayStream(struct ArrowArrayStream* out) {
    EmptyArrayStream* impl = new EmptyArrayStream(schema_.get());
    ArrayStreamFactory<EmptyArrayStream>::InitArrayStream(impl, out);
  }

  /// \brief Create an empty UniqueArrayStream from a struct ArrowSchema
  ///
  /// DEPRECATED (0.4.0): Use the constructor + ToArrayStream() to export an
  /// EmptyArrayStream to an ArrowArrayStream consumer.
  static UniqueArrayStream MakeUnique(struct ArrowSchema* schema) {
    UniqueArrayStream stream;
    EmptyArrayStream(schema).ToArrayStream(stream.get());
    return stream;
  }

  virtual ~EmptyArrayStream() {}

 protected:
  UniqueSchema schema_;
  struct ArrowError error_;

  void MakeStream(struct ArrowArrayStream* stream) { ToArrayStream(stream); }

  virtual int get_schema(struct ArrowSchema* schema) {
    return ArrowSchemaDeepCopy(schema_.get(), schema);
  }

  virtual int get_next(struct ArrowArray* array) {
    array->release = nullptr;
    return NANOARROW_OK;
  }

  virtual const char* get_last_error() { return error_.message; }

 private:
  friend class ArrayStreamFactory<EmptyArrayStream>;

  int GetSchema(struct ArrowSchema* schema) { return get_schema(schema); }

  int GetNext(struct ArrowArray* array) { return get_next(array); }

  const char* GetLastError() { return get_last_error(); }
};

/// \brief Implementation of an ArrowArrayStream backed by a vector of UniqueArray objects
class VectorArrayStream {
 public:
  /// \brief Create a VectorArrayStream from an ArrowSchema + vector of UniqueArray
  ///
  /// Takes ownership of schema and moves arrays if possible.
  VectorArrayStream(struct ArrowSchema* schema, std::vector<UniqueArray> arrays)
      : offset_(0), schema_(schema), arrays_(std::move(arrays)) {}

  /// \brief Create a one-shot VectorArrayStream from an ArrowSchema + ArrowArray
  ///
  /// Takes ownership of schema and array.
  VectorArrayStream(struct ArrowSchema* schema, struct ArrowArray* array)
      : offset_(0), schema_(schema) {
    arrays_.emplace_back(array);
  }

  /// \brief Export to ArrowArrayStream
  void ToArrayStream(struct ArrowArrayStream* out) {
    VectorArrayStream* impl = new VectorArrayStream(schema_.get(), std::move(arrays_));
    ArrayStreamFactory<VectorArrayStream>::InitArrayStream(impl, out);
  }

  /// \brief Create a UniqueArrowArrayStream from an existing array
  ///
  /// DEPRECATED (0.4.0): Use the constructors + ToArrayStream() to export a
  /// VectorArrayStream to an ArrowArrayStream consumer.
  static UniqueArrayStream MakeUnique(struct ArrowSchema* schema,
                                      struct ArrowArray* array) {
    UniqueArrayStream stream;
    VectorArrayStream(schema, array).ToArrayStream(stream.get());
    return stream;
  }

  /// \brief Create a UniqueArrowArrayStream from existing arrays
  ///
  /// DEPRECATED (0.4.0): Use the constructor + ToArrayStream() to export a
  /// VectorArrayStream to an ArrowArrayStream consumer.
  static UniqueArrayStream MakeUnique(struct ArrowSchema* schema,
                                      std::vector<UniqueArray> arrays) {
    UniqueArrayStream stream;
    VectorArrayStream(schema, std::move(arrays)).ToArrayStream(stream.get());
    return stream;
  }

 private:
  int64_t offset_;
  UniqueSchema schema_;
  std::vector<UniqueArray> arrays_;

  friend class ArrayStreamFactory<VectorArrayStream>;

  int GetSchema(struct ArrowSchema* schema) {
    return ArrowSchemaDeepCopy(schema_.get(), schema);
  }

  int GetNext(struct ArrowArray* array) {
    if (offset_ < static_cast<int64_t>(arrays_.size())) {
      arrays_[offset_++].move(array);
    } else {
      array->release = nullptr;
    }

    return NANOARROW_OK;
  }

  const char* GetLastError() { return ""; }
};

/// @}

namespace internal {
struct Nothing {};

template <typename T>
class Maybe {
 public:
  Maybe() : nothing_(Nothing()), is_something_(false) {}
  Maybe(Nothing) : Maybe() {}

  Maybe(T something)  // NOLINT(google-explicit-constructor)
      : something_(something), is_something_(true) {}

  explicit constexpr operator bool() const { return is_something_; }

  const T& operator*() const { return something_; }

  friend inline bool operator==(Maybe l, Maybe r) {
    if (l.is_something_ != r.is_something_) return false;
    return l.is_something_ ? l.something_ == r.something_ : true;
  }
  friend inline bool operator!=(Maybe l, Maybe r) { return !(l == r); }

  T value_or(T val) const { return is_something_ ? something_ : val; }

 private:
  // When support for gcc 4.8 is dropped, we should also assert
  // is_trivially_copyable<T>::value
  static_assert(std::is_trivially_destructible<T>::value, "");

  union {
    Nothing nothing_;
    T something_;
  };
  bool is_something_;
};

template <typename Get>
struct RandomAccessRange {
  Get get;
  int64_t size;

  using value_type = decltype(std::declval<Get>()(0));

  struct const_iterator {
    int64_t i;
    const RandomAccessRange* range;
    bool operator==(const_iterator other) const { return i == other.i; }
    bool operator!=(const_iterator other) const { return i != other.i; }
    const_iterator& operator++() { return ++i, *this; }
    value_type operator*() const { return range->get(i); }
  };

  const_iterator begin() const { return {0, this}; }
  const_iterator end() const { return {size, this}; }
};

template <typename Next>
struct InputRange {
  Next next;
  using ValueOrFalsy = decltype(std::declval<Next>()());

  static_assert(std::is_constructible<bool, ValueOrFalsy>::value, "");
  static_assert(std::is_default_constructible<ValueOrFalsy>::value, "");
  using value_type = decltype(*std::declval<ValueOrFalsy>());

  struct iterator {
    InputRange* range;
    ValueOrFalsy stashed;

    bool operator==(iterator other) const {
      return static_cast<bool>(stashed) == static_cast<bool>(other.stashed);
    }
    bool operator!=(iterator other) const { return !(*this == other); }

    iterator& operator++() {
      stashed = range->next();
      return *this;
    }
    value_type operator*() const { return *stashed; }
  };

  iterator begin() { return {this, next()}; }
  iterator end() { return {this, ValueOrFalsy()}; }
};
}  // namespace internal

/// \defgroup nanoarrow_hpp-range_for Range-for helpers
///
/// The Arrow C Data interface and the Arrow C Stream interface represent
/// data which can be iterated through using C++'s range-for statement.
///
/// @{

/// \brief An object convertible to any empty optional
constexpr internal::Nothing NA{};

/// \brief A range-for compatible wrapper for ArrowArray of fixed size type
///
/// Provides a sequence of optional<T> copied from each non-null
/// slot of the wrapped array (null slots result in empty optionals).
template <typename T>
class ViewArrayAs {
 private:
  struct Get {
    const uint8_t* validity;
    const void* values;
    int64_t offset;

    internal::Maybe<T> operator()(int64_t i) const {
      i += offset;
      if (validity == nullptr || ArrowBitGet(validity, i)) {
        if (std::is_same<T, bool>::value) {
          return ArrowBitGet(static_cast<const uint8_t*>(values), i);
        } else {
          return static_cast<const T*>(values)[i];
        }
      }
      return NA;
    }
  };

  internal::RandomAccessRange<Get> range_;

 public:
  ViewArrayAs(const ArrowArrayView* array_view)
      : range_{
            Get{
                array_view->buffer_views[0].data.as_uint8,
                array_view->buffer_views[1].data.data,
                array_view->offset,
            },
            array_view->length,
        } {}

  ViewArrayAs(const ArrowArray* array)
      : range_{
            Get{
                static_cast<const uint8_t*>(array->buffers[0]),
                array->buffers[1],
                /*offset=*/0,
            },
            array->length,
        } {}

  using value_type = typename internal::RandomAccessRange<Get>::value_type;
  using const_iterator = typename internal::RandomAccessRange<Get>::const_iterator;
  const_iterator begin() const { return range_.begin(); }
  const_iterator end() const { return range_.end(); }
  value_type operator[](int64_t i) const { return range_.get(i); }
};

/// \brief A range-for compatible wrapper for ArrowArray of binary or utf8
///
/// Provides a sequence of optional<ArrowStringView> referencing each non-null
/// slot of the wrapped array (null slots result in empty optionals). Large
/// binary and utf8 arrays can be wrapped by specifying 64 instead of 32 for
/// the template argument.
template <int OffsetSize>
class ViewArrayAsBytes {
 private:
  static_assert(OffsetSize == 32 || OffsetSize == 64, "");
  using OffsetType = typename std::conditional<OffsetSize == 32, int32_t, int64_t>::type;

  struct Get {
    const uint8_t* validity;
    const void* offsets;
    const char* data;
    int64_t offset;

    internal::Maybe<ArrowStringView> operator()(int64_t i) const {
      i += offset;
      auto* offsets = static_cast<const OffsetType*>(this->offsets);
      if (validity == nullptr || ArrowBitGet(validity, i)) {
        return ArrowStringView{data + offsets[i], offsets[i + 1] - offsets[i]};
      }
      return NA;
    }
  };

  internal::RandomAccessRange<Get> range_;

 public:
  ViewArrayAsBytes(const ArrowArrayView* array_view)
      : range_{
            Get{
                array_view->buffer_views[0].data.as_uint8,
                array_view->buffer_views[1].data.data,
                array_view->buffer_views[2].data.as_char,
                array_view->offset,
            },
            array_view->length,
        } {}

  ViewArrayAsBytes(const ArrowArray* array)
      : range_{
            Get{
                static_cast<const uint8_t*>(array->buffers[0]),
                array->buffers[1],
                static_cast<const char*>(array->buffers[2]),
                /*offset=*/0,
            },
            array->length,
        } {}

  using value_type = typename internal::RandomAccessRange<Get>::value_type;
  using const_iterator = typename internal::RandomAccessRange<Get>::const_iterator;
  const_iterator begin() const { return range_.begin(); }
  const_iterator end() const { return range_.end(); }
  value_type operator[](int64_t i) const { return range_.get(i); }
};

/// \brief A range-for compatible wrapper for ArrowArray of fixed size binary
///
/// Provides a sequence of optional<ArrowStringView> referencing each non-null
/// slot of the wrapped array (null slots result in empty optionals).
class ViewArrayAsFixedSizeBytes {
 private:
  struct Get {
    const uint8_t* validity;
    const char* data;
    int64_t offset;
    int fixed_size;

    internal::Maybe<ArrowStringView> operator()(int64_t i) const {
      i += offset;
      if (validity == nullptr || ArrowBitGet(validity, i)) {
        return ArrowStringView{data + i * fixed_size, fixed_size};
      }
      return NA;
    }
  };

  internal::RandomAccessRange<Get> range_;

 public:
  ViewArrayAsFixedSizeBytes(const ArrowArrayView* array_view, int fixed_size)
      : range_{
            Get{
                array_view->buffer_views[0].data.as_uint8,
                array_view->buffer_views[1].data.as_char,
                array_view->offset,
                fixed_size,
            },
            array_view->length,
        } {}

  ViewArrayAsFixedSizeBytes(const ArrowArray* array, int fixed_size)
      : range_{
            Get{
                static_cast<const uint8_t*>(array->buffers[0]),
                static_cast<const char*>(array->buffers[1]),
                /*offset=*/0,
                fixed_size,
            },
            array->length,
        } {}

  using value_type = typename internal::RandomAccessRange<Get>::value_type;
  using const_iterator = typename internal::RandomAccessRange<Get>::const_iterator;
  const_iterator begin() const { return range_.begin(); }
  const_iterator end() const { return range_.end(); }
  value_type operator[](int64_t i) const { return range_.get(i); }
};

/// \brief A range-for compatible wrapper for ArrowArrayStream
///
/// Provides a sequence of ArrowArray& referencing the most recent array drawn
/// from the wrapped stream. (Each array may be moved from if necessary.)
/// When streams terminate due to an error, the error code and message are
/// available for inspection through the code() and error() member functions
/// respectively. Failure to inspect the error code will result in
/// an assertion failure. The number of arrays drawn from the stream is also
/// available through the count() member function.
class ViewArrayStream {
 public:
  ViewArrayStream(ArrowArrayStream* stream, ArrowErrorCode* code, ArrowError* error)
      : range_{Next{this, stream, UniqueArray()}}, code_{code}, error_{error} {}

  ViewArrayStream(ArrowArrayStream* stream, ArrowError* error)
      : ViewArrayStream{stream, &internal_code_, error} {}

  ViewArrayStream(ArrowArrayStream* stream)
      : ViewArrayStream{stream, &internal_code_, &internal_error_} {}

  // disable copy/move of this view, since its error references may point into itself
  ViewArrayStream(ViewArrayStream&&) = delete;
  ViewArrayStream& operator=(ViewArrayStream&&) = delete;
  ViewArrayStream(const ViewArrayStream&) = delete;
  ViewArrayStream& operator=(const ViewArrayStream&) = delete;

  // ensure the error code of this stream was accessed at least once
  ~ViewArrayStream() { NANOARROW_DCHECK(code_was_accessed_); }

 private:
  struct Next {
    ViewArrayStream* self;
    ArrowArrayStream* stream;
    UniqueArray array;

    ArrowArray* operator()() {
      array.reset();
      *self->code_ = ArrowArrayStreamGetNext(stream, array.get(), self->error_);

      if (array->release != nullptr) {
        NANOARROW_DCHECK(*self->code_ == NANOARROW_OK);
        ++self->count_;
        return array.get();
      }

      return nullptr;
    }
  };

  internal::InputRange<Next> range_;
  ArrowErrorCode* code_;
  ArrowError* error_;
  ArrowError internal_error_ = {};
  ArrowErrorCode internal_code_;
  bool code_was_accessed_ = false;
  int count_ = 0;

 public:
  using value_type = typename internal::InputRange<Next>::value_type;
  using iterator = typename internal::InputRange<Next>::iterator;
  iterator begin() { return range_.begin(); }
  iterator end() { return range_.end(); }

  /// The error code which caused this stream to terminate, if any.
  ArrowErrorCode code() {
    code_was_accessed_ = true;
    return *code_;
  }
  /// The error message which caused this stream to terminate, if any.
  ArrowError* error() { return error_; }

  /// The number of arrays streamed so far.
  int count() const { return count_; }
};

/// @}

}  // namespace nanoarrow

/// \defgroup nanoarrow_hpp-string_view_helpers ArrowStringView helpers
///
/// Factories and equality comparison for ArrowStringView.
///
/// @{

/// \brief Equality comparison operator between ArrowStringView
inline bool operator==(ArrowStringView l, ArrowStringView r) {
  if (l.size_bytes != r.size_bytes) return false;
  return memcmp(l.data, r.data, l.size_bytes) == 0;
}

/// \brief User literal operator allowing ArrowStringView construction like "str"_sv
inline ArrowStringView operator"" _v(const char* data, std::size_t size_bytes) {
  return {data, static_cast<int64_t>(size_bytes)};
}
/// @}

#endif
