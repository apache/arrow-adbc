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

#include "org_apache_arrow_adbc_driver_jni_impl_NativeAdbc.h"

#include <cassert>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#define ADBC_EXPORT
#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <jni.h>

// We will use exceptions for error handling as that's easier with the JNI
// model.

namespace {

template <typename T>
struct AdbcGuardImpl {};

template <>
struct AdbcGuardImpl<struct AdbcDatabase> {
  static struct AdbcDatabase Init() {
    struct AdbcDatabase value = {};
    std::memset(&value, 0, sizeof(struct AdbcDatabase));
    return value;
  }

  static void Release(struct AdbcDatabase& value) {
    if (value.private_data != nullptr) {
      std::ignore = AdbcDatabaseRelease(&value, nullptr);
    }
  }
};

template <>
struct AdbcGuardImpl<struct AdbcError> {
  static struct AdbcError Init() { return ADBC_ERROR_INIT; }

  static void Release(struct AdbcError& value) {
    if (value.release != nullptr) {
      value.release(&value);
      value.release = nullptr;
    }
  }
};

template <typename T>
struct AdbcGuard {
  T value;

  AdbcGuard() : value{AdbcGuardImpl<T>::Init()} {}
  ~AdbcGuard() { AdbcGuardImpl<T>::Release(value); }

  AdbcGuard(const AdbcGuard&) = delete;
  AdbcGuard& operator=(const AdbcGuard&) = delete;
  AdbcGuard(AdbcGuard&&) = delete;
  AdbcGuard& operator=(AdbcGuard&&) = delete;
};

void ThrowJavaException(JNIEnv* env, const std::string& klass,
                        const std::string& message) {
  jclass exception_klass = env->FindClass(klass.c_str());
  if (exception_klass == nullptr) return;
  jmethodID exception_ctor =
      env->GetMethodID(exception_klass, "<init>", "(Ljava/lang/String;)V");
  if (exception_ctor == nullptr) return;
  jstring message_jni = env->NewStringUTF(message.c_str());
  if (message_jni == nullptr) return;
  auto exc = static_cast<jthrowable>(
      env->NewObject(exception_klass, exception_ctor, message_jni));
  if (exc == nullptr) return;
  env->Throw(exc);
}

/// Internal exception.  Meant to be used with RaiseAdbcException and
///   CHECK_ADBC_ERROR.
struct AdbcException {
  AdbcStatusCode code;
  std::string message;

  void ThrowJavaException(JNIEnv* env) const {
    jclass exception_klass = env->FindClass("org/apache/arrow/adbc/core/AdbcException");
    if (exception_klass == nullptr) return;
    jmethodID exception_ctor =
        env->GetMethodID(exception_klass, "<init>",
                         "(Ljava/lang/String;Ljava/lang/Throwable;"
                         "Lorg/apache/arrow/adbc/core/AdbcStatusCode;"
                         "Ljava/lang/String;I)V");
    if (exception_ctor == nullptr) return;

    jclass status_klass = env->FindClass("org/apache/arrow/adbc/core/AdbcStatusCode");
    if (status_klass == nullptr) return;

    jfieldID status_field;

    const char* sig = "Lorg/apache/arrow/adbc/core/AdbcStatusCode;";
#define CASE(name)                                                  \
  case ADBC_STATUS_##name:                                          \
    status_field = env->GetStaticFieldID(status_klass, #name, sig); \
    break;

    switch (code) {
      CASE(UNKNOWN);
      CASE(NOT_IMPLEMENTED);
      CASE(NOT_FOUND);
      CASE(ALREADY_EXISTS);
      CASE(INVALID_ARGUMENT);
      CASE(INVALID_STATE);
      CASE(INVALID_DATA);
      CASE(INTEGRITY);
      CASE(INTERNAL);
      CASE(IO);
      CASE(CANCELLED);
      CASE(TIMEOUT);
      CASE(UNAUTHENTICATED);
      CASE(UNAUTHORIZED);
      default:
        // uh oh
        status_field = env->GetStaticFieldID(status_klass, "INTERNAL", sig);
        break;
    }
#undef CASE
    if (status_field == nullptr) return;
    jobject status_jni = env->GetStaticObjectField(status_klass, status_field);

    jstring message_jni = env->NewStringUTF(message.c_str());
    if (message_jni == nullptr) return;
    auto exc = static_cast<jthrowable>(env->NewObject(
        exception_klass, exception_ctor, message_jni, /*cause=*/nullptr, status_jni,
        /*sqlState=*/nullptr, /*vendorCode=*/0));
    if (exc == nullptr) return;
    env->Throw(exc);
  }
};

/// Signal an error to Java.
void RaiseAdbcException(AdbcStatusCode code, AdbcError& error) {
  assert(code != ADBC_STATUS_OK);
  AdbcException exception{
      .code = code,
      .message =
          error.message ? std::string(error.message) : std::string("(unknown error)"),
  };
  if (error.release != nullptr) {
    error.release(&error);
    error.release = nullptr;
  }
  throw exception;
}

/// Check the result of an ADBC call and raise an exception to Java if it failed.
#define CHECK_ADBC_ERROR(expr, error)      \
  do {                                     \
    AdbcStatusCode status = (expr);        \
    if (status != ADBC_STATUS_OK) {        \
      ::RaiseAdbcException(status, error); \
    }                                      \
  } while (0)

/// Require that a Java class exists or error.
jclass RequireClass(JNIEnv* env, const std::string& name) {
  jclass klass = env->FindClass(name.c_str());
  if (klass == nullptr) {
    std::string message = "[JNI] Could not find class ";
    message += name;
    throw AdbcException{
        .code = ADBC_STATUS_INTERNAL,
        .message = std::move(message),
    };
  }
  return klass;
}

/// Require that a Java class exists or error.
jclass RequireImplClass(JNIEnv* env, std::string_view name) {
  static std::string kPrefix = "org/apache/arrow/adbc/driver/jni/impl/";
  std::string full_name = kPrefix + std::string(name);
  return RequireClass(env, full_name);
}

/// Require that a Java method exists or error.
jmethodID RequireMethod(JNIEnv* env, jclass klass, std::string_view name,
                        std::string_view signature) {
  jmethodID method = env->GetMethodID(klass, name.data(), signature.data());
  if (method == nullptr) {
    std::string message = "[JNI] Could not find method ";
    message += name;
    message += " with signature ";
    message += signature;
    throw AdbcException{
        .code = ADBC_STATUS_INTERNAL,
        .message = std::move(message),
    };
  }
  return method;
}

std::string GetJniUtf8String(JNIEnv* env, jbyteArray jni_string) {
  if (jni_string == nullptr) {
    throw AdbcException{ADBC_STATUS_INTERNAL, "Java byte array was nullptr"};
  }

  jsize length = env->GetArrayLength(jni_string);
  if (env->ExceptionCheck()) {
    throw AdbcException{ADBC_STATUS_INTERNAL, "Could not get byte array length"};
  }

  std::string result(static_cast<size_t>(length), '\0');
  if (length > 0) {
    env->GetByteArrayRegion(jni_string, 0, length,
                            reinterpret_cast<jbyte*>(result.data()));
    if (env->ExceptionCheck()) {
      throw AdbcException{ADBC_STATUS_INTERNAL, "Could not get byte array contents"};
    }
  }
  return result;
}

std::optional<std::string> MaybeGetJniUtf8String(JNIEnv* env, jbyteArray jni_string) {
  if (jni_string == nullptr) {
    return std::nullopt;
  }
  return GetJniUtf8String(env, jni_string);
}

jbyteArray MakeJniUtf8String(JNIEnv* env, const char* value, size_t length) {
  if (length > 0 && value[length - 1] == '\0') {
    length--;
  }

  jbyteArray result = env->NewByteArray(static_cast<jsize>(length));
  if (result == nullptr || env->ExceptionCheck()) return nullptr;
  if (length > 0) {
    env->SetByteArrayRegion(result, 0, static_cast<jsize>(length),
                            reinterpret_cast<const jbyte*>(value));
    if (env->ExceptionCheck()) return nullptr;
  }
  return result;
}

// Get the contents of a ByteBuffer. We may have to copy, so also take a
// scratch buffer to hold the copy if needed. If possible, will use
// DirectByteBuffer so no additional allocation is necessary.
const uint8_t* GetJniByteBuffer(JNIEnv* env, jobject bytebuffer,
                                std::vector<uint8_t>& scratch,
                                size_t& serialized_length) {
  jclass bb_class = RequireClass(env, "java/nio/ByteBuffer");
  if (!env->IsInstanceOf(bytebuffer, bb_class)) {
    ThrowJavaException(env, "java/lang/IllegalArgumentException",
                       "Argument must be a ByteBuffer");
    return nullptr;
  }

  jmethodID bb_remaining = RequireMethod(env, bb_class, "remaining", "()I");
  jmethodID bb_position = RequireMethod(env, bb_class, "position", "()I");

  jint remaining = env->CallIntMethod(bytebuffer, bb_remaining);
  if (remaining < 0) {
    ThrowJavaException(env, "java/lang/IllegalArgumentException",
                       "ByteBuffer remaining() must be non-negative");
    return nullptr;
  } else if (env->ExceptionCheck()) {
    return nullptr;
  }
  serialized_length = static_cast<size_t>(remaining);

  jint position = env->CallIntMethod(bytebuffer, bb_position);
  if (position < 0) {
    ThrowJavaException(env, "java/lang/IllegalArgumentException",
                       "ByteBuffer position() must be non-negative");
    return nullptr;
  } else if (env->ExceptionCheck()) {
    return nullptr;
  }
  const size_t offset = static_cast<size_t>(position);

  // fast path (if direct buffer)
  void* buf = env->GetDirectBufferAddress(bytebuffer);
  if (buf) {
    return static_cast<const uint8_t*>(buf) + offset;
  }

  // middle path (copy from backing array)
  {
    jmethodID bb_has_array = RequireMethod(env, bb_class, "hasArray", "()Z");
    jmethodID bb_array = RequireMethod(env, bb_class, "array", "()[B");
    jmethodID bb_array_offset = RequireMethod(env, bb_class, "arrayOffset", "()I");
    jboolean has_array = env->CallBooleanMethod(bytebuffer, bb_has_array);
    if (env->ExceptionCheck()) return nullptr;
    if (has_array) {
      jint array_offset = env->CallIntMethod(bytebuffer, bb_array_offset);
      if (env->ExceptionCheck()) return nullptr;

      auto array =
          reinterpret_cast<jbyteArray>(env->CallObjectMethod(bytebuffer, bb_array));
      if (env->ExceptionCheck()) return nullptr;

      assert(serialized_length <= static_cast<size_t>(env->GetArrayLength(array)));
      scratch.resize(serialized_length);
      env->GetByteArrayRegion(array, array_offset + position,
                              static_cast<jsize>(serialized_length),
                              reinterpret_cast<jbyte*>(scratch.data()));
      return scratch.data();
    }
  }

  // slow path (copy via invoking Java code to copy into a temp array)
  {
    jmethodID bb_get = RequireMethod(env, bb_class, "get", "([B)Ljava/nio/ByteBuffer;");
    jbyteArray temp = env->NewByteArray(static_cast<jsize>(serialized_length));
    if (!temp) {
      ThrowJavaException(env, "java/lang/OutOfMemoryError",
                         "Failed to allocate byte array to copy ByteBuffer");
      return nullptr;
    }

    env->CallObjectMethod(bytebuffer, bb_get, temp);
    if (env->ExceptionCheck()) return nullptr;

    scratch.resize(serialized_length);
    env->GetByteArrayRegion(temp, 0, static_cast<jsize>(serialized_length),
                            reinterpret_cast<jbyte*>(scratch.data()));
    return scratch.data();
  }
  // not reachable
}

jobject MakeNativeQueryResult(JNIEnv* env, jlong rows_affected,
                              struct ArrowArrayStream* out) {
  // On any failure, release the C struct so its contents don't leak: the Java
  // side only takes ownership (via snapshot) once construction succeeds.
  try {
    jclass native_result_class = RequireImplClass(env, "NativeQueryResult");
    jmethodID native_result_ctor =
        RequireMethod(env, native_result_class, "<init>", "(JJ)V");
    jobject object =
        env->NewObject(native_result_class, native_result_ctor, rows_affected,
                       static_cast<jlong>(reinterpret_cast<uintptr_t>(out)));
    if (object == nullptr || env->ExceptionCheck()) {
      if (out->release != nullptr) {
        out->release(out);
      }
      return nullptr;
    }
    return object;
  } catch (...) {
    if (out->release != nullptr) {
      out->release(out);
    }
    throw;
  }
}

jobject MakeNativeSchemaResult(JNIEnv* env, struct ArrowSchema* schema) {
  // On any failure, release the C struct so its contents don't leak: the Java
  // side only takes ownership (via snapshot) once construction succeeds.
  try {
    jclass native_result_class = RequireImplClass(env, "NativeSchemaResult");
    jmethodID native_result_ctor =
        RequireMethod(env, native_result_class, "<init>", "(J)V");
    jobject object =
        env->NewObject(native_result_class, native_result_ctor,
                       static_cast<jlong>(reinterpret_cast<uintptr_t>(schema)));
    if (object == nullptr || env->ExceptionCheck()) {
      if (schema->release != nullptr) {
        schema->release(schema);
      }
      return nullptr;
    }
    return object;
  } catch (...) {
    if (schema->release != nullptr) {
      schema->release(schema);
    }
    throw;
  }
}

}  // namespace

extern "C" {

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_openDatabase(
    JNIEnv* env, [[maybe_unused]] jclass self, [[maybe_unused]] jint version,
    jobjectArray parameters) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    jclass nativeHandleKlass = RequireImplClass(env, "NativeDatabaseHandle");
    jmethodID nativeHandleCtor = RequireMethod(env, nativeHandleKlass, "<init>", "(J)V");
    const jsize num_params = env->GetArrayLength(parameters);
    if (env->ExceptionCheck()) return nullptr;
    if (num_params % 2 != 0) {
      throw AdbcException{
          .code = ADBC_STATUS_INVALID_ARGUMENT,
          .message = "[JNI] Must provide even number of parameters",
      };
    }

    AdbcGuard<struct AdbcDatabase> db;

    CHECK_ADBC_ERROR(AdbcDatabaseNew(&db.value, &error_guard.value), error_guard.value);
    CHECK_ADBC_ERROR(AdbcDriverManagerDatabaseSetLoadFlags(
                         &db.value, ADBC_LOAD_FLAG_DEFAULT, &error_guard.value),
                     error_guard.value);

    for (jsize i = 0; i < num_params; i += 2) {
      auto key = reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(parameters, i));
      if (env->ExceptionCheck()) return nullptr;
      auto value =
          reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(parameters, i + 1));
      if (env->ExceptionCheck()) return nullptr;

      std::string key_str = GetJniUtf8String(env, key);
      std::string value_str = GetJniUtf8String(env, value);
      env->DeleteLocalRef(key);
      env->DeleteLocalRef(value);
      CHECK_ADBC_ERROR(AdbcDatabaseSetOption(&db.value, key_str.c_str(),
                                             value_str.c_str(), &error_guard.value),
                       error_guard.value);
    }
    CHECK_ADBC_ERROR(AdbcDatabaseInit(&db.value, &error_guard.value), error_guard.value);

    // Copy db to a unique_ptr and then "leak" it (JNI side will release it later)
    auto handle = std::make_unique<struct AdbcDatabase>(db.value);
    std::memset(&db.value, 0, sizeof(struct AdbcDatabase));  // prevent double release
    jobject object =
        env->NewObject(nativeHandleKlass, nativeHandleCtor,
                       static_cast<jlong>(reinterpret_cast<uintptr_t>(handle.get())));
    if (object == nullptr || env->ExceptionCheck()) {
      // Failed to construct Java object: try to release ADBC handle
      std::ignore = AdbcDatabaseRelease(handle.get(), nullptr);
      return nullptr;
    }
    // Don't release until after we've constructed the object
    handle.release();
    return object;
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return nullptr;
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_closeDatabase(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* ptr = reinterpret_cast<struct AdbcDatabase*>(static_cast<uintptr_t>(handle));
    CHECK_ADBC_ERROR(AdbcDatabaseRelease(ptr, &error_guard.value), error_guard.value);
    delete ptr;
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_openConnection(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong database_handle) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    jclass native_handle_class = RequireImplClass(env, "NativeConnectionHandle");
    jmethodID native_handle_ctor =
        RequireMethod(env, native_handle_class, "<init>", "(J)V");

    auto conn = std::make_unique<struct AdbcConnection>();
    std::memset(conn.get(), 0, sizeof(struct AdbcConnection));

    auto* db =
        reinterpret_cast<struct AdbcDatabase*>(static_cast<uintptr_t>(database_handle));

    CHECK_ADBC_ERROR(AdbcConnectionNew(conn.get(), &error_guard.value),
                     error_guard.value);
    auto result = AdbcConnectionInit(conn.get(), db, &error_guard.value);
    if (result != ADBC_STATUS_OK) {
      std::ignore = AdbcConnectionRelease(conn.get(), nullptr);
    }
    CHECK_ADBC_ERROR(result, error_guard.value);

    jobject object =
        env->NewObject(native_handle_class, native_handle_ctor,
                       static_cast<jlong>(reinterpret_cast<uintptr_t>(conn.get())));
    if (object == nullptr || env->ExceptionCheck()) {
      // Failed to construct Java object: try to release ADBC handle
      std::ignore = AdbcConnectionRelease(conn.get(), nullptr);
      return nullptr;
    }
    // Don't release until after we've constructed the object
    conn.release();
    return object;
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return nullptr;
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_closeConnection(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* ptr = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
    CHECK_ADBC_ERROR(AdbcConnectionRelease(ptr, &error_guard.value), error_guard.value);
    delete ptr;
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_openStatement(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong connection_handle) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    jclass native_handle_class = RequireImplClass(env, "NativeStatementHandle");
    jmethodID native_handle_ctor =
        RequireMethod(env, native_handle_class, "<init>", "(J)V");

    auto stmt = std::make_unique<struct AdbcStatement>();
    std::memset(stmt.get(), 0, sizeof(struct AdbcStatement));

    auto* conn = reinterpret_cast<struct AdbcConnection*>(
        static_cast<uintptr_t>(connection_handle));

    CHECK_ADBC_ERROR(AdbcStatementNew(conn, stmt.get(), &error_guard.value),
                     error_guard.value);

    jobject object =
        env->NewObject(native_handle_class, native_handle_ctor,
                       static_cast<jlong>(reinterpret_cast<uintptr_t>(stmt.get())));
    if (object == nullptr || env->ExceptionCheck()) {
      // Failed to construct Java object: try to release ADBC handle
      std::ignore = AdbcStatementRelease(stmt.get(), nullptr);
      return nullptr;
    }
    // Don't release until after we've constructed the object
    stmt.release();
    return object;
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return nullptr;
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_closeStatement(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    CHECK_ADBC_ERROR(AdbcStatementRelease(ptr, &error_guard.value), error_guard.value);
    delete ptr;
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementCancel(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
  try {
    CHECK_ADBC_ERROR(AdbcStatementCancel(ptr, &error_guard.value), error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementGetParameterSchema(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
  struct ArrowSchema schema = {};
  try {
    CHECK_ADBC_ERROR(AdbcStatementGetParameterSchema(ptr, &schema, &error_guard.value),
                     error_guard.value);
    return MakeNativeSchemaResult(env, &schema);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementExecutePartitions(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
  struct ArrowSchema schema = {};
  struct AdbcPartitions partitions = {};
  int64_t rows_affected = 0;
  jobject result = nullptr;

  try {
    jclass native_result_class = RequireImplClass(env, "NativePartitionResult");
    jmethodID native_result_ctor =
        RequireMethod(env, native_result_class, "<init>", "(JJ)V");
    jmethodID native_result_add_partition =
        RequireMethod(env, native_result_class, "addPartition", "([B)V");

    CHECK_ADBC_ERROR(AdbcStatementExecutePartitions(ptr, &schema, &partitions,
                                                    &rows_affected, &error_guard.value),
                     error_guard.value);

    result = env->NewObject(native_result_class, native_result_ctor, rows_affected,
                            static_cast<jlong>(reinterpret_cast<uintptr_t>(&schema)));
    if (env->ExceptionCheck()) goto cleanupall;

    for (size_t i = 0; i < partitions.num_partitions; i++) {
      size_t length = partitions.partition_lengths[i];
      jbyteArray partition = env->NewByteArray(static_cast<jsize>(length));
      if (partition == nullptr || env->ExceptionCheck()) goto cleanupall;
      env->SetByteArrayRegion(partition, 0, static_cast<jsize>(length),
                              reinterpret_cast<const jbyte*>(partitions.partitions[i]));
      if (env->ExceptionCheck()) goto cleanupall;
      env->CallVoidMethod(result, native_result_add_partition, partition);
      if (env->ExceptionCheck()) goto cleanupall;
      // The Java side has a reference now, so free the per-iteration local
      // reference to avoid overflowing the local reference table
      env->DeleteLocalRef(partition);
    }
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }

  // We can't release schema, but we copied out the partitions
  if (partitions.release != nullptr) {
    partitions.release(&partitions);
  }
  return result;

cleanupall:
  if (schema.release != nullptr) {
    schema.release(&schema);
  }
  if (partitions.release != nullptr) {
    partitions.release(&partitions);
  }
  return nullptr;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementExecuteQuery(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    struct ArrowArrayStream out = {};
    int64_t rows_affected = 0;
    CHECK_ADBC_ERROR(
        AdbcStatementExecuteQuery(ptr, &out, &rows_affected, &error_guard.value),
        error_guard.value);

    return MakeNativeQueryResult(env, rows_affected, &out);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementSetSqlQuery(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray query) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
  try {
    std::string query_str = GetJniUtf8String(env, query);
    CHECK_ADBC_ERROR(AdbcStatementSetSqlQuery(ptr, query_str.c_str(), &error_guard.value),
                     error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementSetSubstraitPlan(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jobject plan) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
  std::vector<uint8_t> allocated_plan;
  size_t plan_length = 0;
  try {
    const uint8_t* plan_ptr = GetJniByteBuffer(env, plan, allocated_plan, plan_length);
    if (!plan_ptr || env->ExceptionCheck()) {
      return;  // GetJniByteBuffer failed
    }
    assert(plan_ptr != nullptr);
    CHECK_ADBC_ERROR(
        AdbcStatementSetSubstraitPlan(ptr, plan_ptr, plan_length, &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementBind(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jlong values, jlong schema) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    auto* c_batch = reinterpret_cast<struct ArrowArray*>(static_cast<uintptr_t>(values));
    auto* c_schema =
        reinterpret_cast<struct ArrowSchema*>(static_cast<uintptr_t>(schema));
    CHECK_ADBC_ERROR(AdbcStatementBind(ptr, c_batch, c_schema, &error_guard.value),
                     error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementBindStream(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jlong stream) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    auto* c_stream =
        reinterpret_cast<struct ArrowArrayStream*>(static_cast<uintptr_t>(stream));
    CHECK_ADBC_ERROR(AdbcStatementBindStream(ptr, c_stream, &error_guard.value),
                     error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT jlong JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementExecuteUpdate(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    int64_t rows_affected = 0;
    CHECK_ADBC_ERROR(AdbcStatementExecuteQuery(ptr, /*out=*/nullptr, &rows_affected,
                                               &error_guard.value),
                     error_guard.value);
    return static_cast<jlong>(rows_affected);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return -1;
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementPrepare(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    CHECK_ADBC_ERROR(AdbcStatementPrepare(ptr, &error_guard.value), error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementExecuteSchema(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    struct ArrowSchema schema = {};
    CHECK_ADBC_ERROR(AdbcStatementExecuteSchema(ptr, &schema, &error_guard.value),
                     error_guard.value);

    return MakeNativeSchemaResult(env, &schema);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementGetOptionBytes(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* stmt = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));

  std::vector<uint8_t> buf(1024, '\0');
  size_t length = buf.size();
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(AdbcStatementGetOptionBytes(stmt, key_str.c_str(),
                                                 const_cast<uint8_t*>(buf.data()),
                                                 &length, &error_guard.value),
                     error_guard.value);
    while (length > buf.size()) {
      // Buffer was too small, resize and try again
      buf.resize(length);
      CHECK_ADBC_ERROR(AdbcStatementGetOptionBytes(stmt, key_str.c_str(),
                                                   const_cast<uint8_t*>(buf.data()),
                                                   &length, &error_guard.value),
                       error_guard.value);
    }
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return nullptr;
  }
  jbyteArray result = env->NewByteArray(static_cast<jsize>(length));
  if (result == nullptr || env->ExceptionCheck()) return nullptr;
  env->SetByteArrayRegion(result, 0, static_cast<jsize>(length),
                          reinterpret_cast<const jbyte*>(buf.data()));
  if (env->ExceptionCheck()) return nullptr;
  return result;
}

JNIEXPORT jdouble JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementGetOptionDouble(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* stmt = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
  double value = 0.0;
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcStatementGetOptionDouble(stmt, key_str.c_str(), &value, &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return 0.0;
  }
  return static_cast<jdouble>(value);
}

JNIEXPORT jlong JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementGetOptionLong(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* stmt = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
  int64_t value = 0;
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcStatementGetOptionInt(stmt, key_str.c_str(), &value, &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return 0;
  }
  return static_cast<jlong>(value);
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementGetOptionString(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* stmt = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));

  std::vector<char> buf(1024, '\0');
  size_t length = buf.size();
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcStatementGetOption(stmt, key_str.c_str(), const_cast<char*>(buf.data()),
                               &length, &error_guard.value),
        error_guard.value);
    while (length > buf.size()) {
      // Buffer was too small, resize and try again
      buf.resize(length);
      CHECK_ADBC_ERROR(
          AdbcStatementGetOption(stmt, key_str.c_str(), const_cast<char*>(buf.data()),
                                 &length, &error_guard.value),
          error_guard.value);
    }
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return nullptr;
  }
  return MakeJniUtf8String(env, buf.data(), length);
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementSetOptionBytes(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key,
    jbyteArray value) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* stmt = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
  try {
    std::string key_str = GetJniUtf8String(env, key);
    jsize value_length = env->GetArrayLength(value);
    if (env->ExceptionCheck()) return;
    std::vector<uint8_t> value_buf(static_cast<size_t>(value_length));
    env->GetByteArrayRegion(value, 0, value_length,
                            reinterpret_cast<jbyte*>(value_buf.data()));
    if (env->ExceptionCheck()) return;
    CHECK_ADBC_ERROR(AdbcStatementSetOptionBytes(stmt, key_str.c_str(), value_buf.data(),
                                                 value_buf.size(), &error_guard.value),
                     error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementSetOptionDouble(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key,
    jdouble value) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* stmt = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcStatementSetOptionDouble(stmt, key_str.c_str(), static_cast<double>(value),
                                     &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementSetOptionLong(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key,
    jlong value) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* stmt = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcStatementSetOptionInt(stmt, key_str.c_str(), static_cast<int64_t>(value),
                                  &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementSetOptionString(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key,
    jbyteArray value) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* stmt = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
  try {
    std::string key_str = GetJniUtf8String(env, key);
    if (value == nullptr) {
      CHECK_ADBC_ERROR(
          AdbcStatementSetOption(stmt, key_str.c_str(), nullptr, &error_guard.value),
          error_guard.value);
      return;
    }
    std::string value_str = GetJniUtf8String(env, value);
    CHECK_ADBC_ERROR(AdbcStatementSetOption(stmt, key_str.c_str(), value_str.c_str(),
                                            &error_guard.value),
                     error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionCancel(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* ptr = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
  try {
    CHECK_ADBC_ERROR(AdbcConnectionCancel(ptr, &error_guard.value), error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetObjects(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jint depth,
    jbyteArray catalog, jbyteArray db_schema, jbyteArray table_name,
    jobjectArray table_types, jbyteArray column_name) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));

    auto catalog_str = MaybeGetJniUtf8String(env, catalog);
    auto db_schema_str = MaybeGetJniUtf8String(env, db_schema);
    auto table_name_str = MaybeGetJniUtf8String(env, table_name);
    auto column_name_str = MaybeGetJniUtf8String(env, column_name);

    std::vector<std::string> table_type_strings;
    std::vector<const char*> table_type_ptrs;
    const char** c_table_types = nullptr;
    if (table_types != nullptr) {
      jsize len = env->GetArrayLength(table_types);
      if (env->ExceptionCheck()) return nullptr;
      table_type_strings.reserve(len);
      table_type_ptrs.reserve(len + 1);
      for (jsize i = 0; i < len; i++) {
        auto element =
            reinterpret_cast<jbyteArray>(env->GetObjectArrayElement(table_types, i));
        if (env->ExceptionCheck()) return nullptr;
        table_type_strings.push_back(GetJniUtf8String(env, element));
        env->DeleteLocalRef(element);
        table_type_ptrs.push_back(table_type_strings.back().c_str());
      }
      table_type_ptrs.push_back(nullptr);  // NULL terminator
      c_table_types = table_type_ptrs.data();
    }

    struct ArrowArrayStream out = {};

    CHECK_ADBC_ERROR(
        AdbcConnectionGetObjects(
            conn, static_cast<int>(depth), catalog_str ? catalog_str->c_str() : nullptr,
            db_schema_str ? db_schema_str->c_str() : nullptr,
            table_name_str ? table_name_str->c_str() : nullptr, c_table_types,
            column_name_str ? column_name_str->c_str() : nullptr, &out,
            &error_guard.value),
        error_guard.value);

    return MakeNativeQueryResult(env, -1, &out);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetInfo(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jintArray info_codes) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));

    // Convert jintArray to uint32_t* + length (or NULL + 0 if array is null)
    const uint32_t* c_info_codes = nullptr;
    size_t info_codes_length = 0;
    std::vector<uint32_t> info_codes_vec;
    if (info_codes != nullptr) {
      jsize len = env->GetArrayLength(info_codes);
      if (env->ExceptionCheck()) return nullptr;
      info_codes_vec.resize(len);
      env->GetIntArrayRegion(info_codes, 0, len,
                             reinterpret_cast<jint*>(info_codes_vec.data()));
      if (env->ExceptionCheck()) return nullptr;
      c_info_codes = info_codes_vec.data();
      info_codes_length = static_cast<size_t>(len);
    }

    struct ArrowArrayStream out = {};

    CHECK_ADBC_ERROR(AdbcConnectionGetInfo(conn, c_info_codes, info_codes_length, &out,
                                           &error_guard.value),
                     error_guard.value);

    return MakeNativeQueryResult(env, -1, &out);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetTableSchema(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray catalog,
    jbyteArray db_schema, jbyteArray table_name) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));

    auto catalog_str = MaybeGetJniUtf8String(env, catalog);
    auto db_schema_str = MaybeGetJniUtf8String(env, db_schema);
    std::string table_name_str = GetJniUtf8String(env, table_name);

    struct ArrowSchema schema = {};

    CHECK_ADBC_ERROR(
        AdbcConnectionGetTableSchema(conn, catalog_str ? catalog_str->c_str() : nullptr,
                                     db_schema_str ? db_schema_str->c_str() : nullptr,
                                     table_name_str.c_str(), &schema, &error_guard.value),
        error_guard.value);

    return MakeNativeSchemaResult(env, &schema);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetTableTypes(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  try {
    auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));

    struct ArrowArrayStream out = {};

    CHECK_ADBC_ERROR(AdbcConnectionGetTableTypes(conn, &out, &error_guard.value),
                     error_guard.value);

    return MakeNativeQueryResult(env, -1, &out);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetOptionBytes(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));

  std::vector<uint8_t> buf(1024, '\0');
  size_t length = buf.size();
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(AdbcConnectionGetOptionBytes(conn, key_str.c_str(),
                                                  const_cast<uint8_t*>(buf.data()),
                                                  &length, &error_guard.value),
                     error_guard.value);
    while (length > buf.size()) {
      // Buffer was too small, resize and try again
      buf.resize(length);
      CHECK_ADBC_ERROR(AdbcConnectionGetOptionBytes(conn, key_str.c_str(),
                                                    const_cast<uint8_t*>(buf.data()),
                                                    &length, &error_guard.value),
                       error_guard.value);
    }
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return nullptr;
  }
  jbyteArray result = env->NewByteArray(static_cast<jsize>(length));
  if (result == nullptr || env->ExceptionCheck()) return nullptr;
  env->SetByteArrayRegion(result, 0, static_cast<jsize>(length),
                          reinterpret_cast<const jbyte*>(buf.data()));
  if (env->ExceptionCheck()) return nullptr;
  return result;
}

JNIEXPORT jdouble JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetOptionDouble(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
  double value = 0.0;
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcConnectionGetOptionDouble(conn, key_str.c_str(), &value, &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return 0.0;
  }
  return static_cast<jdouble>(value);
}

JNIEXPORT jlong JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetOptionLong(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
  int64_t value = 0;
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcConnectionGetOptionInt(conn, key_str.c_str(), &value, &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return 0;
  }
  return static_cast<jlong>(value);
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetOptionString(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));

  std::vector<char> buf(1024, '\0');
  size_t length = buf.size();
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcConnectionGetOption(conn, key_str.c_str(), const_cast<char*>(buf.data()),
                                &length, &error_guard.value),
        error_guard.value);
    while (length > buf.size()) {
      // Buffer was too small, resize and try again
      buf.resize(length);
      CHECK_ADBC_ERROR(
          AdbcConnectionGetOption(conn, key_str.c_str(), const_cast<char*>(buf.data()),
                                  &length, &error_guard.value),
          error_guard.value);
    }
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return nullptr;
  }
  return MakeJniUtf8String(env, buf.data(), length);
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionSetOptionBytes(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key,
    jbyteArray value) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
  try {
    std::string key_str = GetJniUtf8String(env, key);
    jsize value_length = env->GetArrayLength(value);
    if (env->ExceptionCheck()) return;
    std::vector<uint8_t> value_buf(static_cast<size_t>(value_length));
    env->GetByteArrayRegion(value, 0, value_length,
                            reinterpret_cast<jbyte*>(value_buf.data()));
    if (env->ExceptionCheck()) return;
    CHECK_ADBC_ERROR(AdbcConnectionSetOptionBytes(conn, key_str.c_str(), value_buf.data(),
                                                  value_buf.size(), &error_guard.value),
                     error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionSetOptionDouble(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key,
    jdouble value) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcConnectionSetOptionDouble(conn, key_str.c_str(), static_cast<double>(value),
                                      &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionSetOptionLong(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key,
    jlong value) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcConnectionSetOptionInt(conn, key_str.c_str(), static_cast<int64_t>(value),
                                   &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionSetOptionString(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key,
    jbyteArray value) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
  try {
    std::string key_str = GetJniUtf8String(env, key);
    if (value == nullptr) {
      CHECK_ADBC_ERROR(
          AdbcConnectionSetOption(conn, key_str.c_str(), nullptr, &error_guard.value),
          error_guard.value);
      return;
    }
    std::string value_str = GetJniUtf8String(env, value);
    CHECK_ADBC_ERROR(AdbcConnectionSetOption(conn, key_str.c_str(), value_str.c_str(),
                                             &error_guard.value),
                     error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionCommit(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
  try {
    CHECK_ADBC_ERROR(AdbcConnectionCommit(conn, &error_guard.value), error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionRollback(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
  try {
    CHECK_ADBC_ERROR(AdbcConnectionRollback(conn, &error_guard.value), error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionReadPartition(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jobject partition) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
  struct ArrowArrayStream out = {};
  size_t serialized_length = 0;
  std::vector<uint8_t> allocated_partition;

  try {
    const uint8_t* serialized_partition =
        GetJniByteBuffer(env, partition, allocated_partition, serialized_length);
    if (!serialized_partition || env->ExceptionCheck()) {
      return nullptr;  // GetJniByteBuffer failed
    }
    assert(serialized_partition != nullptr);
    CHECK_ADBC_ERROR(
        AdbcConnectionReadPartition(conn, serialized_partition, serialized_length, &out,
                                    &error_guard.value),
        error_guard.value);
    return MakeNativeQueryResult(env, -1, &out);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetStatisticNames(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
  struct ArrowArrayStream out = {};
  try {
    CHECK_ADBC_ERROR(AdbcConnectionGetStatisticNames(conn, &out, &error_guard.value),
                     error_guard.value);
    return MakeNativeQueryResult(env, -1, &out);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetStatistics(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray catalog,
    jbyteArray schema, jbyteArray table, jboolean approximate) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
  struct ArrowArrayStream out = {};
  try {
    std::optional<std::string> catalog_str = MaybeGetJniUtf8String(env, catalog);
    std::optional<std::string> schema_str = MaybeGetJniUtf8String(env, schema);
    std::optional<std::string> table_str = MaybeGetJniUtf8String(env, table);
    CHECK_ADBC_ERROR(
        AdbcConnectionGetStatistics(conn, catalog_str ? catalog_str->c_str() : nullptr,
                                    schema_str ? schema_str->c_str() : nullptr,
                                    table_str ? table_str->c_str() : nullptr, approximate,
                                    &out, &error_guard.value),
        error_guard.value);
    return MakeNativeQueryResult(env, -1, &out);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_databaseGetOptionBytes(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* db = reinterpret_cast<struct AdbcDatabase*>(static_cast<uintptr_t>(handle));

  std::vector<uint8_t> buf(1024, '\0');
  size_t length = buf.size();
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcDatabaseGetOptionBytes(db, key_str.c_str(), const_cast<uint8_t*>(buf.data()),
                                   &length, &error_guard.value),
        error_guard.value);
    while (length > buf.size()) {
      // Buffer was too small, resize and try again
      buf.resize(length);
      CHECK_ADBC_ERROR(AdbcDatabaseGetOptionBytes(db, key_str.c_str(),
                                                  const_cast<uint8_t*>(buf.data()),
                                                  &length, &error_guard.value),
                       error_guard.value);
    }
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return nullptr;
  }
  jbyteArray result = env->NewByteArray(static_cast<jsize>(length));
  if (result == nullptr || env->ExceptionCheck()) return nullptr;
  env->SetByteArrayRegion(result, 0, static_cast<jsize>(length),
                          reinterpret_cast<const jbyte*>(buf.data()));
  if (env->ExceptionCheck()) return nullptr;
  return result;
}

JNIEXPORT jdouble JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_databaseGetOptionDouble(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* db = reinterpret_cast<struct AdbcDatabase*>(static_cast<uintptr_t>(handle));
  double value = 0.0;
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcDatabaseGetOptionDouble(db, key_str.c_str(), &value, &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return 0.0;
  }
  return static_cast<jdouble>(value);
}

JNIEXPORT jlong JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_databaseGetOptionLong(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* db = reinterpret_cast<struct AdbcDatabase*>(static_cast<uintptr_t>(handle));
  int64_t value = 0;
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcDatabaseGetOptionInt(db, key_str.c_str(), &value, &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return 0;
  }
  return static_cast<jlong>(value);
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_databaseGetOptionString(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* db = reinterpret_cast<struct AdbcDatabase*>(static_cast<uintptr_t>(handle));

  std::vector<char> buf(1024, '\0');
  size_t length = buf.size();
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcDatabaseGetOption(db, key_str.c_str(), const_cast<char*>(buf.data()), &length,
                              &error_guard.value),
        error_guard.value);
    while (length > buf.size()) {
      // Buffer was too small, resize and try again
      buf.resize(length);
      CHECK_ADBC_ERROR(
          AdbcDatabaseGetOption(db, key_str.c_str(), const_cast<char*>(buf.data()),
                                &length, &error_guard.value),
          error_guard.value);
    }
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return nullptr;
  }
  return MakeJniUtf8String(env, buf.data(), length);
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_databaseSetOptionBytes(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key,
    jbyteArray value) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* db = reinterpret_cast<struct AdbcDatabase*>(static_cast<uintptr_t>(handle));
  try {
    std::string key_str = GetJniUtf8String(env, key);
    jsize value_length = env->GetArrayLength(value);
    if (env->ExceptionCheck()) return;
    std::vector<uint8_t> value_buf(static_cast<size_t>(value_length));
    env->GetByteArrayRegion(value, 0, value_length,
                            reinterpret_cast<jbyte*>(value_buf.data()));
    if (env->ExceptionCheck()) return;
    CHECK_ADBC_ERROR(AdbcDatabaseSetOptionBytes(db, key_str.c_str(), value_buf.data(),
                                                value_buf.size(), &error_guard.value),
                     error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_databaseSetOptionDouble(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key,
    jdouble value) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* db = reinterpret_cast<struct AdbcDatabase*>(static_cast<uintptr_t>(handle));
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcDatabaseSetOptionDouble(db, key_str.c_str(), static_cast<double>(value),
                                    &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_databaseSetOptionLong(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key,
    jlong value) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* db = reinterpret_cast<struct AdbcDatabase*>(static_cast<uintptr_t>(handle));
  try {
    std::string key_str = GetJniUtf8String(env, key);
    CHECK_ADBC_ERROR(
        AdbcDatabaseSetOptionInt(db, key_str.c_str(), static_cast<int64_t>(value),
                                 &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_databaseSetOptionString(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jbyteArray key,
    jbyteArray value) {
  AdbcGuard<struct AdbcError> error_guard;
  auto* db = reinterpret_cast<struct AdbcDatabase*>(static_cast<uintptr_t>(handle));
  try {
    std::string key_str = GetJniUtf8String(env, key);
    if (value == nullptr) {
      CHECK_ADBC_ERROR(
          AdbcDatabaseSetOption(db, key_str.c_str(), nullptr, &error_guard.value),
          error_guard.value);
      return;
    }
    std::string value_str = GetJniUtf8String(env, value);
    CHECK_ADBC_ERROR(
        AdbcDatabaseSetOption(db, key_str.c_str(), value_str.c_str(), &error_guard.value),
        error_guard.value);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

// wrapper around GetJniByteBuffer for direct unit testing
JNIEXPORT jbyteArray JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_internalGetByteBuffer(
    JNIEnv* env, [[maybe_unused]] jclass self, jobject input) {
  std::vector<uint8_t> scratch;
  size_t length = 0;
  try {
    const uint8_t* raw = GetJniByteBuffer(env, input, scratch, length);
    if (env->ExceptionCheck()) return nullptr;
    // valid for raw to be nullptr if length == 0
    jbyteArray result = env->NewByteArray(static_cast<jsize>(length));
    if (result == nullptr || env->ExceptionCheck()) return nullptr;
    if (length > 0) {
      env->SetByteArrayRegion(result, 0, static_cast<jsize>(length),
                              reinterpret_cast<const jbyte*>(raw));
      if (env->ExceptionCheck()) return nullptr;
    }
    return result;
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return nullptr;
  }
  return nullptr;
}
}
