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

adbc_callback_queue <- function() {
  .Call(RAdbcNewCallbackQueue)
}

adbc_callback_queue_run_pending <- function(queue) {
  .Call(RAdbcCallbackQueueRunPending, queue)
}

adbc_array_stream_get_next_async <- function(stream, callback,
                                             schema = stream$get_schema(),
                                             queue = adbc_callback_queue()) {
  callback <- as_adbc_callback(callback)
  callback$args <- list(stream)
  array <- nanoarrow::nanoarrow_allocate_array()
  nanoarrow::nanoarrow_array_set_schema(array, schema, validate = FALSE)
  .Call(RAdbcArrayStreamGetNextAsync, queue, stream, array, callback)

  invisible(queue)
}

adbc_array_stream_get_next_promise <- function(stream,
                                               schema = stream$get_schema(),
                                               loop = NULL, delay = 0) {
  if (is.null(loop)) {
    loop <- later::current_loop()
  }

  queue <- callback_queue_from_later_loop(loop)

  later_loop_schedule_run_pending(loop, delay = delay)

  promises::promise(function(resolve, reject) {
    callback <- adbc_callback(
      on_success = resolve,
      on_error = function(status, error) {
        reject(adbc_error_message(status, error))
      }
    )

    adbc_array_stream_get_next_async(
      stream,
      callback,
      schema = schema,
      queue = queue
    )
  })
}

later_loop_schedule_run_pending <- function(loop, delay = 0) {
  later::later(function() {
    queue <- callback_queue_from_later_loop(loop)
    n_run <- adbc_callback_queue_run_pending(queue)
    if (n_run > 0) {
      later_loop_schedule_run_pending(loop, delay = delay)
    } else {
      string_id <- as.character(loop$id)
      callback_queue_cache[[string_id]] <- adbc_callback_queue()
    }
  }, delay = delay, loop = loop)
}

callback_queue_from_later_loop <- function(loop) {
  string_id <- as.character(loop$id)
  if (is.null(callback_queue_cache[[string_id]])) {
    callback_queue_cache[[string_id]] <- adbc_callback_queue()
  }

  callback_queue_cache[[string_id]]
}

callback_queue_cache <- new.env(parent = emptyenv())

# test helper to force a promise to resolve
later_loop_wait <- function(loop = NULL, timeout = 10, delay = 0.01) {
  if (is.null(loop)) {
    loop <- later::current_loop()
  }

  start <- Sys.time()
  while (!later::loop_empty(loop)) {
    elapsed <- difftime(Sys.time(), start, units = "secs")
    if (elapsed > timeout) {
      stop(sprintf("later_loop_wait() timed out at %s seconds", elapsed))
    }

    later::run_now(loop = loop)
    Sys.sleep(delay)
  }

  invisible(loop)
}

as_adbc_callback <- function(x) {
  if (inherits(x, "adbc_callback")) {
    x
  } else if (is.function(x)) {
    adbc_callback(x)
  } else {
    stop(sprintf("Can't create adbc_callback() from object of type '%s'", class(x)[1]))
  }
}

adbc_callback <- function(on_success, ..., on_error = stop_for_error) {
  callback_env <- new.env(parent = emptyenv())
  callback_env$sheltered_objects = list(...)

  force(on_success)
  force(on_error)
  callback_env$callback <- function(status, error, return_value_xptr) {
    if (!identical(status, 0L)) {
      try(on_error(status, error))
    } else {
      try(on_success(return_value_xptr))
    }
  }

  class(callback_env) <- "adbc_callback"
  callback_env
}
