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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Apache.Arrow.Ipc;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Validate the support for adbc.h for the current version
    /// of ADBC found in the build project.
    /// </summary>
    public class AdbcTests
    {
        [Theory]
        [InlineData(AdbcStatusCode.Success, "ADBC_STATUS_OK", 0)]
        [InlineData(AdbcStatusCode.UnknownError, "ADBC_STATUS_UNKNOWN", 1)]
        [InlineData(AdbcStatusCode.NotImplemented, "ADBC_STATUS_NOT_IMPLEMENTED", 2)]
        [InlineData(AdbcStatusCode.NotFound, "ADBC_STATUS_NOT_FOUND", 3)]
        [InlineData(AdbcStatusCode.AlreadyExists, "ADBC_STATUS_ALREADY_EXISTS", 4)]
        [InlineData(AdbcStatusCode.InvalidArgument, "ADBC_STATUS_INVALID_ARGUMENT", 5)]
        [InlineData(AdbcStatusCode.InvalidState, "ADBC_STATUS_INVALID_STATE", 6)]
        [InlineData(AdbcStatusCode.InvalidData, "ADBC_STATUS_INVALID_DATA", 7)]
        [InlineData(AdbcStatusCode.IntegrityError, "ADBC_STATUS_INTEGRITY", 8)]
        [InlineData(AdbcStatusCode.InternalError, "ADBC_STATUS_INTERNAL", 9)]
        [InlineData(AdbcStatusCode.IOError, "ADBC_STATUS_IO", 10)]
        [InlineData(AdbcStatusCode.Cancelled, "ADBC_STATUS_CANCELLED", 11)]
        [InlineData(AdbcStatusCode.Timeout, "ADBC_STATUS_TIMEOUT", 12)]
        [InlineData(AdbcStatusCode.Unauthenticated, "ADBC_STATUS_UNAUTHENTICATED", 13)]
        [InlineData(AdbcStatusCode.Unauthorized, "ADBC_STATUS_UNAUTHORIZED", 14)]
        public void ValidateStatusCodes(AdbcStatusCode code, string adbcName, int value)
        {
            ValidateEnumValue((int)code, adbcName, value);
        }

        [Theory]
        [InlineData(AdbcInfoCode.VendorName, "ADBC_INFO_VENDOR_NAME", 0)]
        [InlineData(AdbcInfoCode.VendorVersion, "ADBC_INFO_VENDOR_VERSION", 1)]
        [InlineData(AdbcInfoCode.VendorArrowVersion, "ADBC_INFO_VENDOR_ARROW_VERSION", 2)]
        [InlineData(AdbcInfoCode.DriverName, "ADBC_INFO_DRIVER_NAME", 100)]
        [InlineData(AdbcInfoCode.DriverVersion, "ADBC_INFO_DRIVER_VERSION", 101)]
        [InlineData(AdbcInfoCode.DriverArrowVersion, "ADBC_INFO_DRIVER_ARROW_VERSION", 102)]
        public void ValidateInfoCodes(AdbcInfoCode code, string adbcName, int value)
        {
            ValidateEnumValue((int)code, adbcName, value);
        }

        /// <summary>
        /// Validates that a defined enum value matches the corresponding value in `adbc.h`.
        /// </summary>
        /// <param name="enumValue">The enum value.</param>
        /// <param name="adbcName">The name of the ADBC value.</param>
        /// <param name="value">The value of the ADBC value.</param>
        private void ValidateEnumValue(int enumValue, string adbcName, int value)
        {
            Assert.Equal(enumValue, value);

            // find the corresponding value in adbc.h and validate it
            string path = GetPathForAdbcH();

            string pattern = "#define " + adbcName;

            string? line = File.ReadAllLines(path).Where(x => x.StartsWith(pattern)).FirstOrDefault();

            Assert.False(string.IsNullOrEmpty(line));

            string definedValue = line.Replace(pattern, "").Trim();

            Assert.Equal(value, Convert.ToInt32(definedValue));
        }

        // C# is designed to match Java's AdbcDriver
        [Theory]
        [InlineData("Open", new string[] { "parameters" }, new Type[] { typeof(IReadOnlyDictionary<string, string>) })]
        public void ValidateAdbcDriverMethods(string name, string[]? parameterNames = null, Type[]? parameterTypes = null)
        {
            ValidateMethod(typeof(AdbcDriver), name, parameterNames, parameterTypes);
        }

        // C# is designed to match Java's AdbcDatabase
        [Theory]
        [InlineData("Connect", new string[] { "options" }, new Type[] { typeof(IReadOnlyDictionary<string, string>) })]
        public void ValidateAdbcDatabaseMethods(string name, string[]? parameterNames = null, Type[]? parameterTypes = null)
        {
            ValidateMethod(typeof(AdbcDatabase), name, parameterNames, parameterTypes);
        }

        [Theory]
        [InlineData("Commit")]
        [InlineData("CreateStatement")]
        [InlineData("GetInfo", new string[] { "codes" }, new Type[] { typeof(IReadOnlyList<AdbcInfoCode>) })]
        [InlineData("GetObjects",
                    new string[] { "depth", "catalogPattern", "dbSchemaPattern", "tableNamePattern", "tableTypes", "columnNamePattern" },
                    new Type[] { typeof(AdbcConnection.GetObjectsDepth), typeof(string), typeof(string), typeof(string), typeof(IReadOnlyList<string>), typeof(string) })]
        [InlineData("GetTableSchema",
                    new string[] { "catalog", "dbSchema", "tableName" },
                    new Type[] { typeof(string), typeof(string), typeof(string) })]
        [InlineData("GetTableTypes")]
        [InlineData("ReadPartition", new string[] { "partition" }, new Type[] { typeof(PartitionDescriptor) })]
        [InlineData("Rollback")]
        [InlineData("SetOption", new string[] { "key", "value" }, new Type[] { typeof(string), typeof(string) })]
        public void ValidateAdbcConnectionMethods(string name, string[]? parameterNames = null, Type[]? parameterTypes = null)
        {
            ValidateMethod(typeof(AdbcConnection), name, parameterNames, parameterTypes);
        }

        [Theory]
        [InlineData("IsolationLevel", typeof(IsolationLevel))]
        [InlineData("ReadOnly", typeof(bool))]
        public void ValidateAdbcConnectionProperties(string name, Type type)
        {
            ValidateProperty(typeof(AdbcConnection), name, type);
        }

        [Theory]
        // TODO: Bind is defined differently to take a batch rather than an array
        [InlineData("Bind", new string[] { "batch", "schema" }, new Type[] { typeof(RecordBatch), typeof(Schema) })]
        [InlineData("BindStream", new string[] { "stream" }, new Type[] { typeof(IArrowArrayStream) })]
        [InlineData("ExecuteQuery")]
        [InlineData("ExecuteUpdate")]
        [InlineData("ExecutePartitioned")] // C# matches Java here
        [InlineData("GetParameterSchema")]
        [InlineData("Prepare")]
        [InlineData("SetOption", new string[] { "key", "value" }, new Type[] { typeof(string), typeof(string) })]
        public void ValidateAdbcStatementMethods(string name, string[]? parameterNames = null, Type[]? parameterTypes = null)
        {
            ValidateMethod(typeof(AdbcStatement), name, parameterNames, parameterTypes);
        }

        [Theory]
        [InlineData("SqlQuery", typeof(string))]
        [InlineData("SubstraitPlan", typeof(byte[]))]
        public void ValidateAdbcAdbcStatementProperties(string name, Type type)
        {
            ValidateProperty(typeof(AdbcStatement), name, type);
        }

        /// <summary>
        /// Validate the methods of the type (in accordance to the adbc.h file)
        /// </summary>
        /// <param name="t">The type to verify</param>
        /// <param name="methodName">The name of the method</param>
        /// <param name="parameterNames">The parameter names (in order)</param>
        /// <param name="parameterTypes">The parameter types (in order)</param>
        private void ValidateMethod(Type t, string methodName, string[]? parameterNames = null, Type[]? parameterTypes = null)
        {
            MethodInfo? mi;

            if (parameterTypes != null)
                mi = t.GetMethod(methodName, parameterTypes);
            else
                mi = t.GetMethod(methodName);

            if (parameterNames != null)
            {
                Assert.NotNull(mi);
                Assert.True(parameterNames.Length > 0);
                Assert.True(parameterTypes != null);
                Assert.Equal(parameterNames.Length, parameterTypes.Length);

                ParameterInfo[] parameters = mi.GetParameters();

                for (int i = 0; i < parameters.Length; i++)
                {
                    ParameterInfo parameter = parameters[i];

                    Assert.Equal(parameter.Name, parameterNames[i]);
                    Assert.Equal(parameter.ParameterType, parameterTypes[i]);
                }
            }
        }

        /// <summary>
        /// Validate the methods of the type (in accordance to the adbc.h file)
        /// </summary>
        /// <param name="t">The type to verify</param>
        /// <param name="methodName">The name of the method</param>
        /// <param name="parameterNames">The parameter names (in order)</param>
        /// <param name="parameterTypes">The parameter types (in order)</param>
        private void ValidateProperty(Type t, string propertyName, Type propertyType)
        {
            PropertyInfo? pi = t.GetProperty(propertyName, propertyType);

            Assert.NotNull(pi);
        }

        private string GetPathForAdbcH()
        {
            // find the adbc.h file from the repo

            string path = Path.Combine(new string[] { "..", "..", "..", "..", "..", "c", "include", "arrow-adbc", "adbc.h"});

            Assert.True(File.Exists(path));

            return path;
        }
    }
}
