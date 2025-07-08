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

#pragma once

#include <adbc-glib/adbc-glib.h>
#include <arrow-glib/arrow-glib.h>

#include <adbc-arrow-glib/version.h>

G_BEGIN_DECLS

#define GADBC_ARROW_TYPE_STATEMENT (gadbc_arrow_statement_get_type())
G_DECLARE_DERIVABLE_TYPE(GADBCArrowStatement, gadbc_arrow_statement, GADBCArrow,
                         STATEMENT, GADBCStatement)
struct _GADBCArrowStatementClass {
  GADBCStatementClass parent_class;
};

GADBC_ARROW_AVAILABLE_IN_0_10
GADBCArrowStatement* gadbc_arrow_statement_new(GADBCConnection* connection,
                                               GError** error);
GADBC_ARROW_AVAILABLE_IN_1_8
GArrowSchema* gadbc_arrow_statement_get_parameter_schema(GADBCArrowStatement* statement,
                                                         GError** error);
GADBC_ARROW_AVAILABLE_IN_0_10
gboolean gadbc_arrow_statement_bind(GADBCArrowStatement* statement,
                                    GArrowRecordBatch* record_batch, GError** error);
GADBC_ARROW_AVAILABLE_IN_0_10
gboolean gadbc_arrow_statement_bind_stream(GADBCArrowStatement* statement,
                                           GArrowRecordBatchReader* reader,
                                           GError** error);
GADBC_ARROW_AVAILABLE_IN_0_10
gboolean gadbc_arrow_statement_execute(GADBCArrowStatement* statement,
                                       gboolean need_result,
                                       GArrowRecordBatchReader** reader,
                                       gint64* n_rows_affected, GError** error);

G_END_DECLS
