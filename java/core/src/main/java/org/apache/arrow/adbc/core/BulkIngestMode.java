/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adbc.core;

/** How to handle already-existing/nonexistent tables for bulk ingest operations. */
public enum BulkIngestMode {
  /** Create the table and insert data; error if the table exists. */
  CREATE,
  /**
   * Do not create the table and append data; error if the table does not exist ({@link
   * AdbcStatusCode#NOT_FOUND}) or does not match the schema of the data to append ({@link
   * AdbcStatusCode#ALREADY_EXISTS}).
   */
  APPEND,
  /**
   * Create the table and insert data; drop the original table if it already exists.
   *
   * @since ADBC API revision 1.1.0
   */
  REPLACE,
  /**
   * Insert data; create the table if it does not exist, or error ({@link
   * AdbcStatusCode#ALREADY_EXISTS}) if the table exists, but the schema does not match the schema
   * of the data to append.
   */
  CREATE_APPEND,
  ;
}
