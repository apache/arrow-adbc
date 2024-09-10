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

test_that("The log driver logs", {
  expect_snapshot({
    db <- adbc_database_init(adbc_driver_log(), key = "value")
    try(adbc_database_get_option(db, "key"))

    con <- adbc_connection_init(db, key = "value")
    try(adbc_connection_get_option(con, "key"))
    try(adbc_connection_commit(con))
    try(adbc_connection_get_info(con))
    try(adbc_connection_get_objects(con))
    try(adbc_connection_get_table_schema(con, NULL, NULL, "table_name"))
    try(adbc_connection_get_table_types(con))
    try(adbc_connection_read_partition(con, raw()))
    try(adbc_connection_rollback(con))
    try(adbc_connection_cancel(con))
    try(adbc_connection_get_statistics(con, NULL, NULL, "table_name"))
    try(adbc_connection_get_statistic_names(con))

    stmt <- adbc_statement_init(con, key = "value")
    try(adbc_statement_get_option(stmt, "key"))

    try(adbc_statement_execute_query(stmt))
    try(adbc_statement_execute_schema(stmt))
    try(adbc_statement_prepare(stmt))
    try(adbc_statement_set_sql_query(stmt, ""))
    try(adbc_statement_set_substrait_plan(stmt, raw()))
    try(adbc_statement_bind(stmt, data.frame()))
    try(adbc_statement_bind_stream(stmt, data.frame()))
    try(adbc_statement_cancel(stmt))

    adbc_statement_release(stmt)
    adbc_connection_release(con)
    adbc_database_release(db)
  })
})
