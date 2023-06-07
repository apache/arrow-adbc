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
    /// ADBC uses integer error codes to signal errors. To provide more
    /// detail about errors, functions may also return an AdbcError via an
    /// optional out parameter, which can be inspected.
    /// 
    /// Generally, error codes may indicate a driver-side or database-side
    /// error.
    /// </summary>
    public enum AdbcStatusCode : byte
    {
        /// <summary>
        /// No error
        /// </summary>
        Success = 0,

        /// <summary>
        /// An unknown error occurred.
        /// </summary>
        UnknownError = 1,

        /// <summary>
        /// The operation is not implemented or supported.
        /// </summary>
        NotImplemented = 2,

        /// <summary>
        /// A requested resource was not found.
        /// </summary>
        NotFound = 3,

        /// <summary>
        /// A requested resource already exists.
        /// </summary>
        AlreadyExists = 4,

        /// <summary>
        /// The arguments are invalid, likely a programming error.
        /// For instance, they may be of the wrong format, or out of range.
        /// </summary>
        InvalidArgument = 5,

        /// <summary>
        /// The preconditions for the operation are not met, likely a
        /// programming error. For instance, the object may be uninitialized,
        /// or may have not been fully configured.
        /// </summary>
        InvalidState = 6,

        /// <summary>
        /// Invalid data was processed (not a programming error). For
        /// instance, a division by zero may have occurred during
        /// query execution.
        /// 
        /// Indicates a database-side error only.
        /// </summary>
        InvalidData = 7,

        /// <summary>
        /// The database's integrity was affected. For instance, a foreign
        /// key check may have failed, or a uniqueness constraint may have
        /// been violated.
        /// 
        /// Indicates a database-side error only.
        /// </summary>
        IntegrityError = 8,

        /// <summary>
        /// An error internal to the driver or database occurred.
        /// </summary>
        InternalError = 9,

        /// <summary>
        /// An I/O error occurred. For instance, a remote service may be
        /// unavailable.
        /// </summary>
        IOError = 10,

        /// <summary>
        /// The operation was cancelled, not due to a timeout.
        /// </summary>
        Cancelled = 11,

        /// <summary>
        /// The operation was cancelled due to a timeout.
        /// </summary>
        Timeout = 12,

        /// <summary>
        /// Authentication failed.
        /// 
        /// Indicates a database-side error only.
        /// </summary>
        Unauthenticated = 13,

        /// <summary>
        /// The client is not authorized to perform the given operation.
        /// 
        /// Indicates a database-side error only.
        /// </summary>
        Unauthorized = 14,
    }
}
