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
  GADBCDatabase* database;
  GADBCConnection* conn;
  GError* error = NULL;
  database = gadbc_database_new(&error);
  if (!database) {
    g_print("Error creating a Database: %s", error->message);
    g_error_free(error);
    g_object_unref(database);
    return EXIT_FAILURE;
  }

  if (!gadbc_database_set_option(database, "driver", "adbc_driver_sqlite", &error)) {
    g_print("Error initializing the database: %s", error->message);
    g_error_free(error);
    g_object_unref(database);
    return EXIT_FAILURE;
  }
  if (!gadbc_database_set_option(database, "uri", "test.db", &error)) {
    g_print("Error initializing the database: %s", error->message);
    g_error_free(error);
    g_object_unref(database);
    return EXIT_FAILURE;
  }
  if (!gadbc_database_init(database, &error)) {
    g_print("Error initializing the database: %s", error->message);
    g_error_free(error);
    g_object_unref(database);
    return EXIT_FAILURE;
  }

  conn = gadbc_connection_new(&error);
  if (!conn) {
    g_print("Error creating a Connection: %s", error->message);
    g_error_free(error);
    g_object_unref(database);
    g_object_unref(conn);
    return EXIT_FAILURE;
  }
  if (!gadbc_connection_init(conn, database, &error)) {
    g_print("Error initializing a Connection: %s", error->message);
    g_error_free(error);
    g_object_unref(database);
    g_object_unref(conn);
    return EXIT_FAILURE;
  }

  GADBCStatement* statement = gadbc_statement_new(conn, &error);
  if (!statement) {
    g_print("Error initializing a statement: %s", error->message);
    g_error_free(error);
    g_object_unref(conn);
    g_object_unref(database);
    return EXIT_FAILURE;
  }
  if (!gadbc_statement_set_sql_query(statement, "select sqlite_version() as version",
                                     &error)) {
    g_print("Error setting a query: %s", error->message);
    g_error_free(error);
    g_object_unref(statement);
    g_object_unref(conn);
    g_object_unref(database);
    return EXIT_FAILURE;
  }

  gint64 n_rows_affected;
  if (!gadbc_statement_execute_query(statement, &n_rows_affected,
                               &error)) {
    g_print("Error executing a query ingnoring results: %s", error->message);
    g_error_free(error);
    g_object_unref(statement);
    g_object_unref(conn);
    g_object_unref(database);
    return EXIT_FAILURE;
  }

  GArrowRecordBatchReader* reader = gadbc_statement_execute_reader(statement, &error);
  if (!reader) {
    g_print("Error importing a result: %s", error->message);
    g_error_free(error);
    g_object_unref(statement);
    g_object_unref(conn);
    g_object_unref(database);
    return EXIT_FAILURE;
  }

  GArrowTable* table = garrow_record_batch_reader_read_all(reader, &error);
  g_object_unref(reader);
  if (!table) {
    g_print("Error reading a result: %s", error->message);
    g_error_free(error);
    g_object_unref(statement);
    g_object_unref(conn);
    g_object_unref(database);
    return EXIT_FAILURE;
  }
  gchar* table_content = garrow_table_to_string(table, &error);
  g_object_unref(table);
  if (!table_content) {
    g_print("Error stringify a result: %s", error->message);
    g_error_free(error);
    g_object_unref(statement);
    g_object_unref(conn);
    g_object_unref(database);
    return EXIT_FAILURE;
  }
  g_print("Result:\n%s\n", table_content);
  g_free(table_content);
  gadbc_statement_release(statement, &error);
  if (error) {
    g_print("Error releasing a statement: %s", error->message);
    g_error_free(error);
    error = NULL;
  }
  g_object_unref(statement);
  g_object_unref(conn);
  g_object_unref(database);

  return EXIT_SUCCESS;
}
