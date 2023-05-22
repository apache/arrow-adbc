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
using Apache.Arrow.Adbc.Core;

/// <summary>
/// The root exception when working with Adbc drivers.
/// </summary>
public class AdbcException : Exception
{
    private AdbcStatusCode statusCode = AdbcStatusCode.UnknownError;

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
        this.statusCode = statusCode;
    }

    public AdbcException(string message, AdbcStatusCode statusCode, Exception innerException)
       : base(message, innerException)
    {
    }

    public AdbcException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public static AdbcException NotImplemented(string message)
    {
        return new AdbcException(message, AdbcStatusCode.NotImplemented);
    }

    //
    // Summary:
    //     For database providers which support it, contains a standard SQL 5-character
    //     return code indicating the success or failure of the database operation. The
    //     first 2 characters represent the class of the return code (e.g. error, success),
    //     while the last 3 characters represent the subclass, allowing detection of error
    //     scenarios in a database-portable way.
    //     For database providers which don't support it, or for inapplicable error scenarios,
    //     contains null.
    //
    // Returns:
    //     A standard SQL 5-character return code, or null.
    public virtual string SqlState
    {
        get { return null; }
        protected set { throw new NotImplementedException(); }
    }

    public AdbcStatusCode Status => this.statusCode;

    public virtual int NativeError
    {
        get { return 0; }
        protected set {  throw new NotImplementedException(); }
    }
}
