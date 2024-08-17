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

if (identical(Sys.getenv("NOT_CRAN"), "true") &&
    packageVersion("DBItest") >= "1.7.2") {
  DBItest::make_context(
    adbi::adbi("adbcsqlite"),
    list(
      uri = tempfile("DBItest", fileext = ".sqlite"),
      rows_affected_callback = function() function(x) {
        if (x == -1) testthat::skip("unknown number of `rows_affected`") else x
      }
    ),
    tweaks = suppressWarnings(
      DBItest::tweaks(
        dbitest_version = "1.7.2",
        constructor_relax_args = TRUE,
        placeholder_pattern = c("?", "$1", "$name", ":name"),
        date_cast = function(x) paste0("'", x, "'"),
        time_cast = function(x) paste0("'", x, "'"),
        timestamp_cast = function(x) paste0("'", x, "'"),
        logical_return = function(x) as.integer(x),
        date_typed = FALSE,
        time_typed = FALSE,
        timestamp_typed = FALSE,
        temporary_tables = FALSE, # apache/arrow-adbc#1141
        strict_identifier = TRUE
      )
    ),
    name = "adbcsqlite"
  )

  DBItest::test_all(
    skip = c(

      "package_name",

      # options(adbi.allow_multiple_results = FALSE)
      "send_query_only_one_result_set",
      "send_statement_only_one_result_set",
      "arrow_send_query_only_one_result_set",

      # options(adbi.force_close_results = TRUE)
      "send_query_stale_warning",
      "send_statement_stale_warning",
      "arrow_send_query_stale_warning",

      # int/int64 https://github.com/r-dbi/DBItest/issues/311
      "data_64_bit_numeric",
      "data_64_bit_numeric_warning",
      "data_64_bit_lossless",
      "arrow_read_table_arrow",

      # `field.types` https://github.com/r-dbi/adbi/issues/14
      "append_roundtrip_64_bit_roundtrip",
      "roundtrip_64_bit_numeric",
      "roundtrip_64_bit_character",
      "roundtrip_64_bit_roundtrip",
      "roundtrip_field_types",

      # bind zero length https://github.com/apache/arrow-adbc/issues/1365
      "bind_multi_row_zero_length",
      "arrow_bind_multi_row_zero_length",
      "arrow_stream_bind_multi_row_zero_length",
      "stream_bind_multi_row_zero_length",

      # misc issues with well understood causes
      "connect_bigint_character", # apache/arrow-nanoarrow#324
      "data_logical", # r-dbi/DBItest#308
      "create_table_visible_in_other_connection", # r-dbi/DBItest#297
      "quote_identifier_string", # apache/arrow-adbc#1395
      "read_table_empty", # apache/arrow-adbc#1400
      "list_objects_features", # r-dbi/DBItest#339

      # misc issues with poorly understood causes
      "append_table_new",
      "begin_write_commit",

      # cause segfaults
      "begin_write_disconnect",

      if (getRversion() < "4.0") {
        c(
          "column_info",
          "column_info_consistent_keywords",
          "column_info_consistent_unnamed",
          "column_info_consistent",
          "column_info_row_names"
        )
      }
    )
  )
}
