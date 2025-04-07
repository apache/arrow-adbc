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

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#include <jni.h>

// We will use exceptions for error handling as that's easier with the JNI
// model.

namespace {

struct AdbcException {
  AdbcStatusCode code;
  std::string message;

  void ThrowJavaException(JNIEnv* env) const {
    jclass exception_klass =
        env->FindClass("org/apache/arrow/adbc/driver/jni/impl/NativeAdbcException");
    assert(exception_klass != nullptr);
    env->ThrowNew(exception_klass, message.c_str());
  }
};

void RaiseAdbcException(AdbcStatusCode code, const AdbcError& error) {
  assert(code != ADBC_STATUS_OK);
  throw AdbcException{
      .code = code,
      .message = std::string(error.message),
  };
}

#define CHECK_ADBC_ERROR(expr, error)      \
  do {                                     \
    AdbcStatusCode status = (expr);        \
    if (status != ADBC_STATUS_OK) {        \
      ::RaiseAdbcException(status, error); \
    }                                      \
  } while (0)

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
      // TODO:
    }
    value = env->GetStringUTFChars(jni_string, nullptr);
    if (value == nullptr) {
      // TODO:
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

JNIEXPORT jobject JNICALL
Java_org_apache_arrow_adbc_driver_jni_impl_NativeAdbc_statementExecuteQuery(
    JNIEnv* env, [[maybe_unused]] jclass self, jlong handle) {
  try {
    struct AdbcError error = ADBC_ERROR_INIT;
    auto* ptr = reinterpret_cast<struct AdbcStatement*>(static_cast<uintptr_t>(handle));
    auto out = std::make_unique<struct ArrowArrayStream>();
    std::memset(out.get(), 0, sizeof(struct ArrowArrayStream));
    int64_t rows_affected = 0;
    CHECK_ADBC_ERROR(AdbcStatementExecuteQuery(ptr, out.get(), &rows_affected, &error),
                     error);

    jclass native_result_class = RequireImplClass(env, "NativeQueryResult");
    jmethodID native_result_ctor =
        RequireMethod(env, native_result_class, "<init>", "(JJ)V");
    jobject object =
        env->NewObject(native_result_class, native_result_ctor, rows_affected,
                       static_cast<jlong>(reinterpret_cast<uintptr_t>(out.get())));
    // Don't release until after we've constructed the object
    out.release();
    return object;
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
}
