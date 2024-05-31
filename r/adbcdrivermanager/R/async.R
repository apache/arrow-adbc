# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


adbc_async_task <- function(subclass = character()) {
  structure(
    .Call(RAdbcAsyncTaskNew, adbc_allocate_error()),
    class = union(subclass, "adbc_async_task")
  )
}

adbc_async_task_status <- function(task) {
  .Call(RAdbcAsyncTaskWaitFor, task, 0)
}

adbc_async_task_set_callback <- function(task, resolve, reject = NULL,
                                         loop = later::current_loop()) {
  # If the task is completed, run the callback (or else the callback
  # will not run)
  if (adbc_async_task_status(task) == "ready") {
    adbc_async_task_run_callback(task, resolve, reject)
  } else {
    .Call(RAdbcAsyncTaskSetCallback, task, resolve, reject, loop$id)
  }

  invisible(task)
}

adbc_async_task_run_callback <- function(task, resolve = task$resolve,
                                         reject = task$reject) {
  tryCatch({
      result <- adbc_async_task_result(task)
      resolve(result)
    },
    error = function(e) {
      if (is.null(reject)) {
        stop(e)
      } else {
        reject(e)
      }
    }
  )

  invisible(task)
}

adbc_async_task_wait_non_cancellable <- function(task, resolution = 0.1) {
  .Call(RAdbcAsyncTaskWaitFor, task, round(resolution * 1000))
}

adbc_async_task_wait <- function(task, resolution = 0.1) {
  withCallingHandlers(
    status <- .Call(RAdbcAsyncTaskWait, task, round(resolution * 1000)),
    interrupt = function(e) {
      adbc_async_task_cancel(task)
    }
  )

  if (status != "ready") {
    stop(sprintf("Expected status ready but got %s", status))
  }

  adbc_async_task_result(task)
}

as.promise.adbc_async_task <- function(task) {
  force(task)
  promises::promise(function(resolve, reject) {
    adbc_async_task_set_callback(task, resolve, reject)
  })
}

adbc_async_task_cancel <- function(task) {
  UseMethod("adbc_async_task_cancel")
}

#' @export
adbc_async_task_cancel.default <- function(task) {
  FALSE
}

adbc_async_task_result <- function(task) {
  UseMethod("adbc_async_task_result")
}

#' @export
names.adbc_async_task <- function(x) {
  names(.Call(RAdbcAsyncTaskData, x))
}

#' @export
`[[.adbc_async_task` <- function(x, i) {
  .Call(RAdbcAsyncTaskData, x)[[i]]
}

#' @export
`$.adbc_async_task` <- function(x, name) {
  .Call(RAdbcAsyncTaskData, x)[[name]]
}

adbc_async_sleep <- function(duration_ms, error_message = NULL) {
  task <- adbc_async_task("adbc_async_sleep")
  .Call(RAdbcAsyncTaskLaunchSleep, task, duration_ms)

  user_data <- task$user_data
  user_data$duration_ms <- duration_ms
  user_data$error_message <- error_message

  task
}

#' @export
adbc_async_task_result.adbc_async_sleep <- function(task) {
  if (!is.null(task$user_data$error_message)) {
    cnd <- simpleError(task$user_data$error_message)
    class(cnd) <- c("adbc_async_sleep_error", class(cnd))
    stop(cnd)
  }

  task$user_data$duration_ms
}

#' @export
adbc_async_task_cancel.adbc_async_statement_cancellable <- function(task) {
  adbc_statement_cancel(task$user_data$statement)
  TRUE
}

adbc_statement_prepare_async <- function(statement) {
  task <- adbc_async_task(
    c("adbc_async_prepare", "adbc_async_statement_cancellable")
  )

  user_data <- task$user_data
  user_data$statement <- statement
  .Call(RAdbcAsyncTaskLaunchPrepare, task, statement)

  task
}

#' @export
adbc_async_task_result.adbc_async_prepare <- function(task) {
  if (!identical(task$return_code, 0L)) {
    stop_for_error(task$return_code, task$error_xptr)
  }

  task$user_data$statement
}

adbc_statement_execute_query_async <- function(statement, stream = NULL) {
  task <- adbc_async_task(
    c("adbc_async_execute_query", "adbc_async_statement_cancellable")
  )

  user_data <- task$user_data
  user_data$statement <- statement
  user_data$stream <- stream

  user_data$rows_affected <- .Call(
    RAdbcAsyncTaskLaunchExecuteQuery,
    task,
    statement,
    stream
  )

  task
}

#' @export
adbc_async_task_result.adbc_async_execute_query <- function(task) {
  if (!identical(task$return_code, 0L)) {
    stop_for_error(task$return_code, task$error_xptr)
  }

  list(
    statement = task$user_data$statement,
    stream = task$user_data$stream,
    rows_affected = task$user_data$rows_affected
  )
}

adbc_statement_stream_get_schema_async <- function(statement, stream) {
  task <- adbc_async_task(
    c("adbc_async_statement_stream_get_next", "adbc_async_statement_cancellable")
  )

  user_data <- task$user_data
  user_data$statement <- statement
  user_data$stream <- stream
  user_data$schema <- nanoarrow::nanoarrow_allocate_schema()

  user_data$rows_affected <- .Call(
    RAdbcAsyncTaskLaunchStreamGetSchema,
    task,
    stream,
    user_data$schema
  )

  task
}


#' @export
adbc_async_task_result.adbc_async_statement_stream_schema <- function(task) {
  if (!identical(task$return_code, 0L)) {
    adbc_statement_release(task$user_data$statement)
    stop(task$user_data$stream$get_last_error())
  }

  list(
    statement = task$user_data$statement,
    array = task$user_data$schema
  )
}

adbc_statement_stream_get_next_async <- function(statement, stream) {
  task <- adbc_async_task(
    c("adbc_async_statement_stream_get_next", "adbc_async_statement_cancellable")
  )

  user_data <- task$user_data
  user_data$statement <- statement
  user_data$stream <- stream
  user_data$array <- nanoarrow::nanoarrow_allocate_array()

  user_data$rows_affected <- .Call(
    RAdbcAsyncTaskLaunchStreamGetNext,
    task,
    stream,
    user_data$array
  )

  task
}

#' @export
adbc_async_task_result.adbc_async_statement_stream_get_next <- function(task) {
  if (!identical(task$return_code, 0L)) {
    adbc_statement_release(task$user_data$statement)
    stop(task$user_data$stream$get_last_error())
  }

  list(
    statement = task$user_data$statement,
    array = task$user_data$array
  )
}
