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

using System;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// The root exception when working with Adbc drivers.
    /// </summary>
    public class AdbcException : Exception
    {
        private AdbcStatusCode _statusCode = AdbcStatusCode.UnknownError;

        public AdbcException()
        {
        }

        public AdbcException(string message)
            : base(message)
        {
        }

        public AdbcException(string message, AdbcStatusCode statusCode)
            : base(message)
        {
            _statusCode = statusCode;
        }

        public AdbcException(string message, AdbcStatusCode statusCode, Exception innerException)
           : base(message, innerException)
        {
            _statusCode = statusCode;
        }

        public AdbcException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public static AdbcException NotImplemented(string message)
        {
            return new AdbcException(message, AdbcStatusCode.NotImplemented);
        }

        internal static AdbcException Missing(string name)
        {
            return new AdbcException($"Driver does not implement required function Adbc{name}", AdbcStatusCode.InternalError);
        }

        /// <summary>
        /// For database providers which support it, contains a standard
        /// SQL 5-character return code indicating the success or failure
        /// of the database operation. The first 2 characters represent the
        /// class of the return code (e.g. error, success), while the
        /// last 3 characters represent the subclass, allowing detection
        /// of error scenarios in a database-portable way.
        /// For database providers which don't support it, or for
        /// inapplicable error scenarios, contains null.
        /// </summary>
        public virtual string? SqlState
        {
            get => null;
        }

        /// <summary>
        /// Gets or sets the <see cref="AdbcStatusCode"/> for the error.
        /// </summary>
        public AdbcStatusCode Status
        {
            get => _statusCode;
        }

        /// <summary>
        /// Gets a native error number.
        /// </summary>
        public virtual int NativeError
        {
            get => 0;
        }
    }
}
