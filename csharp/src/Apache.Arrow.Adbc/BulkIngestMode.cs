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

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// How to handle already-existing/nonexistent tables for bulk ingest
    /// operations.
    /// </summary>
    public enum BulkIngestMode
    {
        /// <summary>
        /// Create the table and insert data; error if the table exists.
        /// </summary>
        Create,

        /// <summary>
        /// Do not create the table and append data; error if the table
        /// does not exist (<see cref="AdbcStatusCode.NotFound"/>) or
        /// does not match the schema of the data to append
        /// (<see cref="AdbcStatusCode.AlreadyExists"/>).
        /// </summary>
        Append,

        /// <summary>
        /// Create the table and insert data; drop the original table if it already exists.
        ///
        /// Added as part of API version 1.1.0
        /// </summary>
        Replace,

        /// <summary>
        /// Insert data; create the table if it does not exist or error
        /// (<see cref="AdbcStatusCode.AlreadyExists"/>) if the table exists but the schema does not
        /// match the schema of the data to append.
        ///
        /// Added as part of API version 1.1.0
        /// </summary>
        CreateAppend,
    }
}
