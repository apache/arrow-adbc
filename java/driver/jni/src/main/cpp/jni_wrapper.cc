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

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <jni.h>

// We will use exceptions for error handling as that's easier with the JNI
// model.

namespace {

/// Internal exception.  Meant to be used with RaiseAdbcException and
///   CHECK_ADBC_ERROR.
struct AdbcException {
  AdbcStatusCode code;
  std::string message;

  void ThrowJavaException(JNIEnv* env) const {
    jclass exception_klass = env->FindClass("org/apache/arrow/adbc/core/AdbcException");
    assert(exception_klass != nullptr);
    jmethodID exception_ctor =
        env->GetMethodID(exception_klass, "<init>",
                         "(Ljava/lang/String;Ljava/lang/Throwable;"
                         "Lorg/apache/arrow/adbc/core/AdbcStatusCode;"
                         "Ljava/lang/String;I)V");
    assert(exception_ctor != nullptr);

    jclass status_klass = env->FindClass("org/apache/arrow/adbc/core/AdbcStatusCode");
    assert(status_klass != nullptr);

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
    jobject status_jni = env->GetStaticObjectField(status_klass, status_field);

    jstring message_jni = env->NewStringUTF(message.c_str());
    auto exc = static_cast<jthrowable>(env->NewObject(
        exception_klass, exception_ctor, message_jni, /*cause=*/nullptr, status_jni,
        /*sqlState=*/nullptr, /*vendorCode=*/0));
    env->Throw(exc);
  }
};

/// Signal an error to Java.
void RaiseAdbcException(AdbcStatusCode code, const AdbcError& error) {
  assert(code != ADBC_STATUS_OK);
  throw AdbcException{
      .code = code,
      .message = std::string(error.message),
  };
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
jclass RequireImplClass(JNIEnv* env, std::string_view name) {
  static std::string kPrefix = "org/apache/arrow/adbc/driver/jni/impl/";
  std::string full_name = kPrefix + std::string(name);
  jclass klass = env->FindClass(full_name.c_str());
  if (klass == nullptr) {
    throw AdbcException{
        .code = ADBC_STATUS_INTERNAL,
        .message = "[JNI] Could not find class " + full_name,
    };
  }
  return klass;
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

struct JniStringView {
  JNIEnv* env;
  jstring jni_string;
  const char* value;

  explicit JniStringView(JNIEnv* env, jstring jni_string)
      : env(env), jni_string(jni_string), value(nullptr) {
    if (jni_string == nullptr) {
      throw AdbcException{ADBC_STATUS_INTERNAL, "Java string was nullptr"};
    }
    value = env->GetStringUTFChars(jni_string, nullptr);
    if (value == nullptr) {
      throw AdbcException{ADBC_STATUS_INTERNAL,
                          "Java string was nullptr (could not get string contents)"};
    }
  }

  ~JniStringView() {
    if (jni_string == nullptr) {
      return;
    }

    env->ReleaseStringUTFChars(jni_string, value);
    jni_string = nullptr;
  }
};

std::string GetJniString(JNIEnv* env, jstring jni_string) {
  JniStringView view(env, jni_string);
  return std::string(view.value);
}

std::optional<std::string> MaybeGetJniString(JNIEnv* env, jstring jni_string) {
  if (jni_string == nullptr) {
    return std::nullopt;
  }
  JniStringView view(env, jni_string);
  return std::string(view.value);
}

template <typename Callable>
auto WithJniString(JNIEnv* env, jstring jni_string, Callable&& callable) {
  JniStringView view(env, jni_string);
  return callable(view.value);
}

}  // namespace

extern "C" {

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_openDatabase(
    JNIEnv* env, [[maybe_unused]] jclass self, jint version, jobjectArray parameters) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto db = std::make_unique<struct AdbcDatabase>();
    std::memset(db.get(), 0, sizeof(struct AdbcDatabase));

    CHECK_ADBC_ERROR(AdbcDatabaseNew(db.get(), &error), error);
    CHECK_ADBC_ERROR(
        AdbcDriverManagerDatabaseSetLoadFlags(db.get(), ADBC_LOAD_FLAG_DEFAULT, &error),
        error);

    const jsize num_params = env->GetArrayLength(parameters);
    if (num_params % 2 != 0) {
      throw AdbcException{
          .code = ADBC_STATUS_INVALID_ARGUMENT,
          .message = "[JNI] Must provide even number of parameters",
      };
    }
    for (jsize i = 0; i < num_params; i += 2) {
      // N.B. assuming String because Java side is typed as String[]
      auto key = reinterpret_cast<jstring>(env->GetObjectArrayElement(parameters, i));
      auto value =
          reinterpret_cast<jstring>(env->GetObjectArrayElement(parameters, i + 1));

      JniStringView key_str(env, key);
      JniStringView value_str(env, value);
      CHECK_ADBC_ERROR(
          AdbcDatabaseSetOption(db.get(), key_str.value, value_str.value, &error), error);
    }

    CHECK_ADBC_ERROR(AdbcDatabaseInit(db.get(), &error), error);

    jclass nativeHandleKlass = RequireImplClass(env, "NativeDatabaseHandle");
    jmethodID nativeHandleCtor = RequireMethod(env, nativeHandleKlass, "<init>", "(J)V");
    jobject object =
        env->NewObject(nativeHandleKlass, nativeHandleCtor,
                       static_cast<jlong>(reinterpret_cast<uintptr_t>(db.get())));
    // Don't release until after we've constructed the object
    db.release();
    return object;
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
    return nullptr;
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_closeDatabase(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* ptr = reinterpret_cast<struct AdbcDatabase*>(static_cast<uintptr_t>(handle));
    CHECK_ADBC_ERROR(AdbcDatabaseRelease(ptr, &error), error);
    delete ptr;
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_openConnection(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong database_handle) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto conn = std::make_unique<struct AdbcConnection>();
    std::memset(conn.get(), 0, sizeof(struct AdbcConnection));

    auto* db =
        reinterpret_cast<struct AdbcDatabase*>(static_cast<uintptr_t>(database_handle));

    CHECK_ADBC_ERROR(AdbcConnectionNew(conn.get(), &error), error);
    CHECK_ADBC_ERROR(AdbcConnectionInit(conn.get(), db, &error), error);

    jclass native_handle_class = RequireImplClass(env, "NativeConnectionHandle");
    jmethodID native_handle_ctor =
        RequireMethod(env, native_handle_class, "<init>", "(J)V");
    jobject object =
        env->NewObject(native_handle_class, native_handle_ctor,
                       static_cast<jlong>(reinterpret_cast<uintptr_t>(conn.get())));
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
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* ptr = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));
    CHECK_ADBC_ERROR(AdbcConnectionRelease(ptr, &error), error);
    delete ptr;
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_openStatement(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong connection_handle) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto stmt = std::make_unique<struct AdbcStatement>();
    std::memset(stmt.get(), 0, sizeof(struct AdbcStatement));

    auto* conn = reinterpret_cast<struct AdbcConnection*>(
        static_cast<uintptr_t>(connection_handle));

    CHECK_ADBC_ERROR(AdbcStatementNew(conn, stmt.get(), &error), error);

    jclass native_handle_class = RequireImplClass(env, "NativeStatementHandle");
    jmethodID native_handle_ctor =
        RequireMethod(env, native_handle_class, "<init>", "(J)V");
    jobject object =
        env->NewObject(native_handle_class, native_handle_ctor,
                       static_cast<jlong>(reinterpret_cast<uintptr_t>(stmt.get())));
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
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    CHECK_ADBC_ERROR(AdbcStatementRelease(ptr, &error), error);
    delete ptr;
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

jobject MakeNativeQueryResult(JNIEnv* env, jlong rows_affected,
                              struct ArrowArrayStream* out) {
  jclass native_result_class = RequireImplClass(env, "NativeQueryResult");
  jmethodID native_result_ctor =
      RequireMethod(env, native_result_class, "<init>", "(JJ)V");
  return env->NewObject(native_result_class, native_result_ctor, rows_affected,
                        static_cast<jlong>(reinterpret_cast<uintptr_t>(out)));
}

jobject MakeNativeSchemaResult(JNIEnv* env, struct ArrowSchema* schema) {
  jclass native_result_class = RequireImplClass(env, "NativeSchemaResult");
  jmethodID native_result_ctor =
      RequireMethod(env, native_result_class, "<init>", "(J)V");
  return env->NewObject(native_result_class, native_result_ctor,
                        static_cast<jlong>(reinterpret_cast<uintptr_t>(schema)));
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementExecuteQuery(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    struct ArrowArrayStream out = {};
    int64_t rows_affected = 0;
    CHECK_ADBC_ERROR(AdbcStatementExecuteQuery(ptr, &out, &rows_affected, &error), error);

    return MakeNativeQueryResult(env, rows_affected, &out);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementSetSqlQuery(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jstring query) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    JniStringView query_str(env, query);
    CHECK_ADBC_ERROR(AdbcStatementSetSqlQuery(ptr, query_str.value, &error), error);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementBind(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jlong values, jlong schema) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    auto* c_batch = reinterpret_cast<struct ArrowArray*>(static_cast<uintptr_t>(values));
    auto* c_schema =
        reinterpret_cast<struct ArrowSchema*>(static_cast<uintptr_t>(schema));
    CHECK_ADBC_ERROR(AdbcStatementBind(ptr, c_batch, c_schema, &error), error);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementBindStream(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jlong stream) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    auto* c_stream =
        reinterpret_cast<struct ArrowArrayStream*>(static_cast<uintptr_t>(stream));
    CHECK_ADBC_ERROR(AdbcStatementBindStream(ptr, c_stream, &error), error);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT jlong JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementExecuteUpdate(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    int64_t rows_affected = 0;
    CHECK_ADBC_ERROR(
        AdbcStatementExecuteQuery(ptr, /*out=*/nullptr, &rows_affected, &error), error);
    return static_cast<jlong>(rows_affected);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return -1;
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementPrepare(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    CHECK_ADBC_ERROR(AdbcStatementPrepare(ptr, &error), error);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT void JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementSetOption(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jstring key, jstring value) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    JniStringView key_str(env, key);
    JniStringView value_str(env, value);
    CHECK_ADBC_ERROR(AdbcStatementSetOption(ptr, key_str.value, value_str.value, &error),
                     error);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetObjects(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jint depth, jstring catalog,
    jstring db_schema, jstring table_name, jobjectArray table_types,
    jstring column_name) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));

    // Nullable string parameters: null jstring → NULL for C API (meaning "no filter")
    auto catalog_str = MaybeGetJniString(env, catalog);
    auto db_schema_str = MaybeGetJniString(env, db_schema);
    auto table_name_str = MaybeGetJniString(env, table_name);
    auto column_name_str = MaybeGetJniString(env, column_name);

    // Convert String[] table_types to const char** (NULL-terminated) or NULL
    std::vector<std::string> table_type_strings;
    std::vector<const char*> table_type_ptrs;
    const char** c_table_types = nullptr;
    if (table_types != nullptr) {
      jsize len = env->GetArrayLength(table_types);
      table_type_strings.reserve(len);
      table_type_ptrs.reserve(len + 1);
      for (jsize i = 0; i < len; i++) {
        auto element =
            reinterpret_cast<jstring>(env->GetObjectArrayElement(table_types, i));
        table_type_strings.push_back(GetJniString(env, element));
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
            column_name_str ? column_name_str->c_str() : nullptr, &out, &error),
        error);

    return MakeNativeQueryResult(env, -1, &out);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetInfo(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jintArray info_codes) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));

    // Convert jintArray to uint32_t* + length (or NULL + 0 if array is null)
    const uint32_t* c_info_codes = nullptr;
    size_t info_codes_length = 0;
    std::vector<uint32_t> info_codes_vec;
    if (info_codes != nullptr) {
      jsize len = env->GetArrayLength(info_codes);
      info_codes_vec.resize(len);
      env->GetIntArrayRegion(info_codes, 0, len,
                             reinterpret_cast<jint*>(info_codes_vec.data()));
      c_info_codes = info_codes_vec.data();
      info_codes_length = static_cast<size_t>(len);
    }

    struct ArrowArrayStream out = {};

    CHECK_ADBC_ERROR(
        AdbcConnectionGetInfo(conn, c_info_codes, info_codes_length, &out, &error),
        error);

    return MakeNativeQueryResult(env, -1, &out);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetTableSchema(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle, jstring catalog,
    jstring db_schema, jstring table_name) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));

    auto catalog_str = MaybeGetJniString(env, catalog);
    auto db_schema_str = MaybeGetJniString(env, db_schema);
    JniStringView table_name_str(env, table_name);

    struct ArrowSchema schema = {};

    CHECK_ADBC_ERROR(
        AdbcConnectionGetTableSchema(conn, catalog_str ? catalog_str->c_str() : nullptr,
                                     db_schema_str ? db_schema_str->c_str() : nullptr,
                                     table_name_str.value, &schema, &error),
        error);

    return MakeNativeSchemaResult(env, &schema);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_connectionGetTableTypes(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* conn = reinterpret_cast<struct AdbcConnection*>(static_cast<uintptr_t>(handle));

    struct ArrowArrayStream out = {};

    CHECK_ADBC_ERROR(AdbcConnectionGetTableTypes(conn, &out, &error), error);

    return MakeNativeQueryResult(env, -1, &out);
  } catch (const AdbcException& e) {
    e.ThrowJavaException(env);
  }
  return nullptr;
}
}
