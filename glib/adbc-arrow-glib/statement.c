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

#include <adbc-arrow-glib/statement.h>

/**
 * SECTION: statement
 * @title: GADBCArrowStatement
 * @include: adbc-arrow-glib/adbc-arrow-glib.h
 *
 * #GADBCArrowStatement is a class for Arrow GLib integrated
 * statement.
 */

G_DEFINE_TYPE(GADBCArrowStatement, gadbc_arrow_statement, GADBC_TYPE_STATEMENT)

static void gadbc_arrow_statement_init(GADBCArrowStatement* statement) {}

static void gadbc_arrow_statement_class_init(GADBCArrowStatementClass* klass) {}

/**
 * gadbc_arrow_statement_new:
 * @connection: A #GADBCConnection.
 * @error: (nullable): Return location for a #GError or %NULL.
 *
 * Returns: A newly created #GADBCArrowStatement for @connection on
 * success, %NULL otherwise.
 *
 * Since: 0.10.0
 */
GADBCArrowStatement* gadbc_arrow_statement_new(GADBCConnection* connection,
                                               GError** error) {
  GADBCArrowStatement* statement = g_object_new(GADBC_ARROW_TYPE_STATEMENT, NULL);
  if (gadbc_statement_initialize(GADBC_STATEMENT(statement), connection,
                                 "[adbc-arrow][statement][new]", error)) {
    return statement;
  } else {
    g_object_unref(statement);
    return NULL;
  }
}

/**
 * gadbc_arrow_statement_bind:
 * @statement: A #GADBCArrowStatement.
 * @record_batch: A #GArrowRecordBatch.
 * @error: (out) (optional): Return location for a #GError or %NULL.
 *
 * Bind Arrow data. This can be used for bulk inserts or prepared
 * statements.
 *
 * Returns: %TRUE if binding is done successfully, %FALSE
 *   otherwise.
 *
 * Since: 0.10.0
 */
gboolean gadbc_arrow_statement_bind(GADBCArrowStatement* statement,
                                    GArrowRecordBatch* record_batch, GError** error) {
  gpointer c_abi_array = NULL;
  gpointer c_abi_schema = NULL;
  if (!garrow_record_batch_export(record_batch, &c_abi_array, &c_abi_schema, error)) {
    return FALSE;
  }
  gboolean success =
      gadbc_statement_bind(GADBC_STATEMENT(statement), c_abi_array, c_abi_schema, error);
  g_free(c_abi_array);
  g_free(c_abi_schema);
  return success;
}

/**
 * gadbc_arrow_statement_bind_stream:
 * @statement: A #GADBCArrowStatement.
 * @reader: A #GArrowRecordBatchReader.
 * @error: (out) (optional): Return location for a #GError or %NULL.
 *
 * Bind Arrow data stream. This can be used for bulk inserts or prepared
 * statements.
 *
 * Returns: %TRUE if binding is done successfully, %FALSE
 *   otherwise.
 *
 * Since: 0.10.0
 */
gboolean gadbc_arrow_statement_bind_stream(GADBCArrowStatement* statement,
                                           GArrowRecordBatchReader* reader,
                                           GError** error) {
  gpointer c_abi_array_stream = garrow_record_batch_reader_export(reader, error);
  if (!c_abi_array_stream) {
    return FALSE;
  }
  gboolean success =
      gadbc_statement_bind_stream(GADBC_STATEMENT(statement), c_abi_array_stream, error);
  g_free(c_abi_array_stream);
  return success;
}

/**
 * gadbc_arrow_statement_execute:
 * @statement: A #GADBCStatement.
 * @need_result: Whether the results are received by @reader or not.
 * @reader: (out) (optional): Return location for the execution.
 * @n_rows_affected: (out) (optional): Return location for the number of rows
 *   affected if known.
 * @error: (out) (optional): Return location for a #GError or %NULL.
 *
 * Execute a statement and get the results.
 *
 * This invalidates any prior result sets.
 *
 * Returns: %TRUE if execution is done successfully, %FALSE otherwise.
 *
 * Since: 0.10.0
 */
gboolean gadbc_arrow_statement_execute(GADBCArrowStatement* statement,
                                       gboolean need_result,
                                       GArrowRecordBatchReader** reader,
                                       gint64* n_rows_affected, GError** error) {
  gpointer c_abi_array_stream = NULL;
  if (!gadbc_statement_execute(GADBC_STATEMENT(statement), need_result,
                               reader ? &c_abi_array_stream : NULL, n_rows_affected,
                               error)) {
    return FALSE;
  }
  if (need_result && reader) {
    *reader = garrow_record_batch_reader_import(c_abi_array_stream, error);
    g_free(c_abi_array_stream);
  }
  return TRUE;
}
