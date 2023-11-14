/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <stdlib.h>

#include <adbc-glib/adbc-glib.h>
#include <arrow-glib/arrow-glib.h>

int main(int argc, char** argv) {
  int exit_code = EXIT_FAILURE;
  GADBCDatabase* database = NULL;
  GADBCConnection* connection = NULL;
  GADBCStatement* statement = NULL;
  GError* error = NULL;

  database = gadbc_database_new(&error);
  if (!database) {
    g_print("Failed to create a database: %s", error->message);
    goto exit;
  }

  if (!gadbc_database_set_option(database, "driver", "adbc_driver_sqlite", &error)) {
    g_print("Failed to set driver: %s", error->message);
    goto exit;
  }
  if (!gadbc_database_set_option(database, "uri", ":memory:", &error)) {
    g_print("Failed to set database URI: %s", error->message);
    goto exit;
  }
  if (!gadbc_database_init(database, &error)) {
    g_print("Failed to initialize a database: %s", error->message);
    goto exit;
  }

  connection = gadbc_connection_new(&error);
  if (!connection) {
    g_print("Failed to create a connection: %s", error->message);
    goto exit;
  }
  if (!gadbc_connection_init(connection, database, &error)) {
    g_print("Failed to initialize a connection: %s", error->message);
    goto exit;
  }

  statement = gadbc_statement_new(connection, &error);
  if (!statement) {
    g_print("Failed to create a statement: %s", error->message);
    goto exit;
  }
  if (!gadbc_statement_set_sql_query(statement, "select sqlite_version() as version",
                                     &error)) {
    g_print("Failed to set a query: %s", error->message);
    goto exit;
  }

  gpointer c_abi_array_stream;
  gint64 n_rows_affected;
  if (!gadbc_statement_execute(statement, TRUE, &c_abi_array_stream, &n_rows_affected,
                               &error)) {
    g_print("Failed to execute a query: %s", error->message);
    goto exit;
  }

  GArrowRecordBatchReader* reader =
      garrow_record_batch_reader_import(c_abi_array_stream, &error);
  g_free(c_abi_array_stream);
  if (!reader) {
    g_print("Failed to import a result: %s", error->message);
    goto exit;
  }

  GArrowTable* table = garrow_record_batch_reader_read_all(reader, &error);
  g_object_unref(reader);
  if (!table) {
    g_print("Failed to read a result: %s", error->message);
    goto exit;
  }
  gchar* table_content = garrow_table_to_string(table, &error);
  g_object_unref(table);
  if (!table_content) {
    g_print("Failed to stringify a result: %s", error->message);
    goto exit;
  }
  g_print("Result:\n%s\n", table_content);
  g_free(table_content);

  exit_code = EXIT_SUCCESS;

exit:
  if (error) {
    g_error_free(error);
    error = NULL;
  }
  if (statement) {
    if (!gadbc_statement_release(statement, &error)) {
      g_print("Failed to release a statement: %s", error->message);
      g_error_free(error);
      error = NULL;
    }
    g_object_unref(statement);
  }
  if (connection) {
    if (!gadbc_connection_release(connection, &error)) {
      g_print("Failed to release a connection: %s", error->message);
      g_error_free(error);
      error = NULL;
    }
    g_object_unref(connection);
  }
  if (database) {
    if (!gadbc_database_release(database, &error)) {
      g_print("Failed to release a database: %s", error->message);
      g_error_free(error);
      error = NULL;
    }
    g_object_unref(database);
  }
  return exit_code;
}
