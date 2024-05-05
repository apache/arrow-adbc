
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

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include <deque>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <adbc.h>

#include "radbc.h"

enum class RAdbcAsyncTaskStatus { NOT_STARTED, STARTED, READY };

struct RAdbcAsyncTask {
  AdbcError* return_error{nullptr};
  int* return_code{nullptr};
  double* rows_affected{nullptr};
  void* return_value_ptr{nullptr};

  std::string error_message;
  RAdbcAsyncTaskStatus status;
  std::future<void> result;
};

template <>
inline const char* adbc_xptr_class<RAdbcAsyncTask>() {
  return "adbc_async_task";
}

static void FinalizeTaskXptr(SEXP task_xptr) {
  auto task = reinterpret_cast<RAdbcAsyncTask*>(R_ExternalPtrAddr(task_xptr));
  if (task != nullptr) {
    delete task;
  }
}

static void error_for_started_task(RAdbcAsyncTask* task) {
  if (task->result.valid()) {
    Rf_error("adbc_async_task is already in use");
  }
}

extern "C" SEXP RAdbcAsyncTaskNew(SEXP error_xptr) {
  const char* names[] = {"error_xptr",  "return_code", "rows_affected",
                         "result_xptr", "user_data",   ""};
  SEXP task_prot = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(task_prot, 0, error_xptr);
  SET_VECTOR_ELT(task_prot, 1, Rf_allocVector(INTSXP, 1));
  SET_VECTOR_ELT(task_prot, 2, Rf_allocVector(REALSXP, 1));

  SEXP new_env = PROTECT(adbc_new_env());
  SET_VECTOR_ELT(task_prot, 4, new_env);
  UNPROTECT(1);

  auto task = new RAdbcAsyncTask();
  SEXP task_xptr = PROTECT(R_MakeExternalPtr(task, R_NilValue, task_prot));
  R_RegisterCFinalizer(task_xptr, &FinalizeTaskXptr);

  task->return_error = adbc_from_xptr<AdbcError>(error_xptr);
  task->return_code = INTEGER(VECTOR_ELT(task_prot, 1));
  task->rows_affected = REAL(VECTOR_ELT(task_prot, 2));

  *(task->return_code) = NA_INTEGER;
  *(task->rows_affected) = NA_REAL;
  task->status = RAdbcAsyncTaskStatus::NOT_STARTED;

  UNPROTECT(2);
  return task_xptr;
}

extern "C" SEXP RAdbcAsyncTaskData(SEXP task_xptr) {
  adbc_from_xptr<RAdbcAsyncTask>(task_xptr);
  return R_ExternalPtrProtected(task_xptr);
}

extern "C" SEXP RAdbcAsyncTaskWait(SEXP task_xptr, SEXP duration_ms_sexp) {
  auto task = adbc_from_xptr<RAdbcAsyncTask>(task_xptr);
  int duration_ms = adbc_as_int(duration_ms_sexp);

  if (duration_ms < 0) {
    Rf_error("duration_ms must be >= 0");
  }

  switch (task->status) {
    case RAdbcAsyncTaskStatus::NOT_STARTED:
      return Rf_mkString("not_started");
    case RAdbcAsyncTaskStatus::READY:
      return Rf_mkString("ready");
    default:
      break;
  }

  std::future_status status =
      task->result.wait_for(std::chrono::milliseconds(duration_ms));
  switch (status) {
    case std::future_status::timeout:
      return Rf_mkString("timeout");
    case std::future_status::ready:

      return Rf_mkString("ready");
    default:
      Rf_error("Unknown status returned from future::wait_for()");
  }
}

extern "C" SEXP RAdbcAsyncTaskLaunchSleep(SEXP task_xptr, SEXP duration_ms_sexp) {
  auto task = adbc_from_xptr<RAdbcAsyncTask>(task_xptr);
  error_for_started_task(task);

  int duration_ms = adbc_as_int(duration_ms_sexp);

  task->result = std::async(std::launch::async, [task, duration_ms] {
    std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
    *(task->return_code) = ADBC_STATUS_OK;
  });

  task->status = RAdbcAsyncTaskStatus::STARTED;
  return R_NilValue;
}

// A thin wrapper around a std::thread() that ensures that the thread
// does not leak. This could also maybe just be an external pointer to
// a std::thread*. The external pointer to the Task holds a strong
// reference to the external pointer to the CallbackQueue that is
// released just before the callback is run.
class Task {
 public:
  Task() : worker(nullptr) {}

  std::thread* worker;

  static void FinalizeXptr(SEXP xptr) {
    Task* task = reinterpret_cast<Task*>(R_ExternalPtrAddr(xptr));
    if (task->worker != nullptr) {
      // TODO: check task->worker->joinable()?
      task->worker->join();
      delete task->worker;
    }

    delete task;
  }

  static SEXP MakeXptr(SEXP shelter = R_NilValue) {
    SEXP xptr = PROTECT(R_MakeExternalPtr(new Task(), R_NilValue, shelter));
    R_RegisterCFinalizer(xptr, &FinalizeXptr);
    UNPROTECT(1);
    return xptr;
  }
};

// A thread-safe queue of callbacks to execute.
class CallbackQueue {
 public:
  // Because the RCallback is used in C++ frames where a longjmp may occur,
  // the members of this struct must be trivially destructible:
  // Any destruction that needs to occur must occur via an SEXP
  // finalizer set on the return_value_xptr. This struct is intentionally
  // copyable and is passed by value.
  struct RCallback {
    // An environment containing a function named "callback". This callback
    // is executed as callback(return_code, error_xptr, return_value_xptr). The
    // environment may contain other items that need to stay valid for the lifetime of the
    // task (e.g., inputs).
    SEXP env_sexp;

    // An external pointer to an AdbcError*
    SEXP error_xptr;

    // An external pointer to return_value_ptr (with proper finalizer set as applicable)
    SEXP return_value_xptr;

    // A return code (e.g., AdbcStatusCode). A return code of 0 indicates success.
    int return_code;

    // The external pointer address of error_xptr
    AdbcError* return_error;

    // The external pointer address of return_value_xptr
    void* return_value_ptr;
  };

  // Initialize a callback and preserve its SEXP members. This must
  // be called from the main R thread.
  RCallback InitCallback(SEXP callback_env, SEXP return_value_xptr = R_NilValue,
                         SEXP error_xptr = R_NilValue) {
    RCallback out{callback_env, error_xptr, return_value_xptr,
                  NA_INTEGER,   nullptr,    nullptr};
    if (error_xptr != R_NilValue) {
      out.return_error = reinterpret_cast<AdbcError*>(R_ExternalPtrAddr(error_xptr));
    }

    if (return_value_xptr != R_NilValue) {
      out.return_value_ptr = R_ExternalPtrAddr(return_value_xptr);
    }

    R_PreserveObject(out.env_sexp);
    R_PreserveObject(out.error_xptr);
    R_PreserveObject(out.return_value_xptr);

    return out;
  }

  // Add a callback to the queue with thread safety. This can
  // (should) be called from another thread.
  void AddCallback(RCallback callback) {
    std::lock_guard<std::mutex> lock(callbacks_lock_);
    pending_callbacks_.push_back(callback);
  }

  // Try to run all callbacks. Callbacks should be written such that
  // they do not error; however, this function is written such that
  // an erroring callback will simply result in a (potentially)
  // incompletely executed callback queue.
  int64_t RunPending() {
    int64_t n_run = 0;
    while (!pending_callbacks_.empty()) {
      // RunCallback() may may longjmp
      RCallback callback = GetNextCallback();
      RunCallback(callback);
      n_run++;
    }

    return n_run;
  }

  // Pop a callback from the end of the queue with thread safety.
  RCallback GetNextCallback() {
    std::lock_guard<std::mutex> lock(callbacks_lock_);
    RCallback callback = pending_callbacks_.front();
    pending_callbacks_.pop_front();
    return callback;
  }

  // Transfer SEXP ownership of callback members to the stack and run
  // the callback. This must be called from the main R thread.
  void RunCallback(RCallback callback) {
    // Transfer responsibility of releasing SEXPs to the stack such that
    // they will be garbage collected if any of the R calls below error
    SEXP env_sexp = PROTECT(callback.env_sexp);
    SEXP error_xptr = PROTECT(callback.error_xptr);
    SEXP return_value_xptr = PROTECT(callback.return_value_xptr);
    R_ReleaseObject(env_sexp);
    R_ReleaseObject(error_xptr);
    R_ReleaseObject(return_value_xptr);

    // Release the dependence of the task on this callback queue
    SEXP task_sym = PROTECT(Rf_install("task"));
    SEXP task_xptr = PROTECT(Rf_findVarInFrame(env_sexp, task_sym));
    R_SetExternalPtrProtected(task_xptr, R_NilValue);
    UNPROTECT(2);

    // Set up the call to run the callback
    SEXP callback_sym = PROTECT(Rf_install("callback"));
    SEXP return_code_sexp = PROTECT(Rf_ScalarInteger(callback.return_code));
    SEXP callback_call =
        PROTECT(Rf_lang4(callback_sym, return_code_sexp, error_xptr, return_value_xptr));

    // Run the callback
    Rf_eval(callback_call, env_sexp);

    UNPROTECT(6);
  }

  static void FinalizeXptr(SEXP xptr) {
    CallbackQueue* queue = reinterpret_cast<CallbackQueue*>(R_ExternalPtrAddr(xptr));
    delete queue;
  }

  static SEXP MakeXptr(SEXP shelter = R_NilValue) {
    SEXP xptr = PROTECT(R_MakeExternalPtr(new CallbackQueue(), R_NilValue, shelter));
    R_RegisterCFinalizer(xptr, &FinalizeXptr);
    UNPROTECT(1);
    return xptr;
  }

 private:
  std::deque<RCallback> pending_callbacks_;
  std::mutex callbacks_lock_;
};

// TODO: unlike other parts of this package, these functions make use of
// C++ standard library functions that might throw exceptions in functions
// that are directly called from R. These will segfault if an exception is
// thrown. cpp11 handles this using BEGIN_CPP11 an END_CPP11...we would need
// similar for safety here.

extern "C" SEXP RAdbcNewCallbackQueue(void) { return CallbackQueue::MakeXptr(); }

extern "C" SEXP RAdbcCallbackQueueRunPending(SEXP callback_queue_xptr) {
  // TODO: Check callback_queue_xptr class
  auto queue = reinterpret_cast<CallbackQueue*>(R_ExternalPtrAddr(callback_queue_xptr));
  return Rf_ScalarReal(queue->RunPending());
}

extern "C" SEXP RAdbcStatementExecuteQueryAsync(SEXP callback_queue_xptr,
                                                SEXP statement_xptr, SEXP out_stream_xptr,
                                                SEXP error_xptr, SEXP callback_env) {
  // TODO: check array_stream/array/callback queue classes using utils in radbc.h
  auto queue = reinterpret_cast<CallbackQueue*>(R_ExternalPtrAddr(callback_queue_xptr));
  auto statement = adbc_from_xptr<AdbcStatement>(statement_xptr);
  auto out_stream = adbc_from_xptr<ArrowArrayStream>(out_stream_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  // Task handle to ensure the thread pointer is cleaned up
  SEXP task_xptr = PROTECT(Task::MakeXptr(callback_queue_xptr));
  SEXP task_symbol = PROTECT(Rf_install("task"));
  Rf_setVar(task_symbol, task_xptr, callback_env);
  UNPROTECT(1);

  auto task = reinterpret_cast<Task*>(R_ExternalPtrAddr(task_xptr));
  CallbackQueue::RCallback callback =
      queue->InitCallback(callback_env, out_stream_xptr, error_xptr);
  task->worker = new std::thread([statement, out_stream, error, callback, queue] {
    CallbackQueue::RCallback callback_out = callback;
    int64_t rows_affected = -1;
    callback_out.return_code =
        AdbcStatementExecuteQuery(statement, out_stream, &rows_affected, error);
    queue->AddCallback(callback_out);
  });

  UNPROTECT(1);
  return R_NilValue;
}

extern "C" SEXP RAdbcArrayStreamGetNextAsync(SEXP callback_queue_xptr,
                                             SEXP array_stream_xptr, SEXP array_xptr,
                                             SEXP callback_env) {
  // TODO: check array_stream/array/callback queue classes using utils in radbc.h
  auto queue = reinterpret_cast<CallbackQueue*>(R_ExternalPtrAddr(callback_queue_xptr));
  auto array_stream =
      reinterpret_cast<struct ArrowArrayStream*>(R_ExternalPtrAddr(array_stream_xptr));
  auto array = reinterpret_cast<struct ArrowArray*>(R_ExternalPtrAddr(array_xptr));

  // Task handle to ensure the thread pointer is cleaned up
  SEXP task_xptr = PROTECT(Task::MakeXptr(callback_queue_xptr));
  SEXP task_symbol = PROTECT(Rf_install("task"));
  Rf_setVar(task_symbol, task_xptr, callback_env);
  UNPROTECT(1);

  auto task = reinterpret_cast<Task*>(R_ExternalPtrAddr(task_xptr));
  CallbackQueue::RCallback callback = queue->InitCallback(callback_env, array_xptr);
  task->worker = new std::thread([array_stream, array, callback, queue] {
    CallbackQueue::RCallback callback_out = callback;
    callback_out.return_code = array_stream->get_next(array_stream, array);
    queue->AddCallback(callback_out);
  });

  UNPROTECT(1);
  return R_NilValue;
}
