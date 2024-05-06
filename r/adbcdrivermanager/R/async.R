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

adbc_async_task_wait_for <- function(task, duration_ms) {
  .Call(RAdbcAsyncTaskWaitFor, task, duration_ms)
}

adbc_async_task_wait <- function(task, resolution_ms = 100) {
  status <- "started"
  while (status != "ready") {
    status <- adbc_async_task_wait_for(task, resolution_ms)
  }

  adbc_async_task_result(task)
}

later_loop_schedule_task_callback <- function(task, resolve, reject,
                                              loop = later::current_loop(),
                                              delay = 0) {
  force(task)
  force(resolve)
  force(reject)

  later::later(function() {
    status <- adbc_async_task_wait_for(task, 0)
    if (status == "timeout") {
      later_loop_schedule_task_callback(
        task,
        resolve,
        reject,
        loop = loop,
        delay = delay
      )
    } else {
      tryCatch(
        resolve(adbc_async_task_result(task)),
        error = function(e) reject(e)
      )
    }
  }, delay = delay, loop = loop)
}

as.promise.adbc_async_task <- function(task) {
  force(task)
  promises::promise(function(resolve, reject) {
    later_loop_schedule_task_callback(task, resolve, reject)
  })
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
