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

#' S4 Compatibility
#'
#' While wrappers around ADBC objects are implemented as S3 objects, these
#' wrappers should also be compatible with the S4 object system. This ensures
#' compatibility with S4 frameworks like DBI that define specific types for
#' slot values.
#'
#' @param driver An [adbc_driver()]
#' @param ... Database parameters passed to [adbc_database_init()]
#'
#' @return A S4 object of class "AdbcS4CompatTest" with slots "driver",
#'   "database", "connection", and "statement".
#' @export
#' @importFrom methods setOldClass setMethod new
#'
#' @examples
#' adbc_test_s4_compat(adbc_driver_void())
#'
adbc_test_s4_compat <- function(driver, ...) {
  new("AdbcS4CompatTest", driver, ...)
}

setOldClass(c("adbc_driver", "adbc_xptr"))
setOldClass(c("adbc_database", "adbc_xptr"))
setOldClass(c("adbc_connection", "adbc_xptr"))
setOldClass(c("adbc_statement", "adbc_xptr"))

setOldClass(c("adbc_driver_void", "adbc_driver", "adbc_xptr"))

setOldClass(c("adbc_driver_monkey", "adbc_driver", "adbc_xptr"))
setOldClass(c("adbc_database_monkey", "adbc_database", "adbc_xptr"))
setOldClass(c("adbc_connection_monkey", "adbc_connection", "adbc_xptr"))
setOldClass(c("adbc_statement_monkey", "adbc_statement", "adbc_xptr"))

setOldClass(c("adbc_driver_log", "adbc_driver", "adbc_xptr"))
setOldClass(c("adbc_database_log", "adbc_database", "adbc_xptr"))
setOldClass(c("adbc_connection_log", "adbc_connection", "adbc_xptr"))
setOldClass(c("adbc_statement_log", "adbc_statement", "adbc_xptr"))

setClass("AdbcS4CompatTest",
  slots = list(
    driver = "adbc_driver",
    database = "adbc_database",
    connection = "adbc_connection",
    statement = "adbc_statement"
  )
)

initialize_AdbcS4CompatTest <- function(.Object, driver, ...) {
  .Object@driver <- driver
  .Object@database <- adbc_database_init(.Object@driver, ...)
  .Object@connection <- adbc_connection_init(.Object@database)
  .Object@statement <- adbc_statement_init(.Object@connection)
  .Object
}

setMethod("initialize", "AdbcS4CompatTest", initialize_AdbcS4CompatTest)
