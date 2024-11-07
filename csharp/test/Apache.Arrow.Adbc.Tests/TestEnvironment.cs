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
using System.Text;

namespace Apache.Arrow.Adbc.Tests
{
    public abstract class TestEnvironment<TConfig> where TConfig : TestConfiguration
    {
        private readonly Func<AdbcConnection> _getConnection;

        public abstract class Factory<TEnv> where TEnv : TestEnvironment<TConfig>
        {
            public abstract TEnv Create(Func<AdbcConnection> getConnection);
        }

        protected TestEnvironment(Func<AdbcConnection> getConnection)
        {
            _getConnection = getConnection;
        }

        public abstract string TestConfigVariable { get; }

        public abstract string VendorVersion { get; }

        public abstract string SqlDataResourceLocation { get; }

        public abstract int ExpectedColumnCount { get; }

        public virtual bool SupportsDelete => true;

        public virtual bool SupportsUpdate => true;

        public virtual bool SupportCatalogName => true;

        public virtual bool ValidateAffectedRows => true;

        public abstract AdbcDriver CreateNewDriver();

        public abstract SampleDataBuilder GetSampleDataBuilder();

        public abstract Dictionary<string, string> GetDriverParameters(TConfig testConfiguration);

        public virtual string GetCreateTemporaryTableStatement(string tableName, string columns)
        {
            return string.Format("CREATE TEMPORARY IF NOT EXISTS TABLE {0} ({1})", tableName, columns);
        }

        public virtual string Delimiter => "\"";

        public virtual string GetInsertStatement(string tableName, string columnName, string? value) =>
            string.Format("INSERT INTO {0} ({1}) VALUES ({2});", tableName, columnName, value ?? "NULL");

        public virtual string GetDeleteValueStatement(string tableName, string whereClause) =>
            string.Format("DELETE FROM {0} {1};", tableName, whereClause);

        public string GetInsertStatementWithIndexColumn(string tableName, string columnName, string indexColumnName, object?[] values, string?[]? formattedValues)
        {
            var completeValues = new StringBuilder();
            if (values.Length == 0) throw new ArgumentOutOfRangeException(nameof(values), values.Length, "Must provide a non-zero length array of test values.");
            for (int i = 0; i < values.Length; i++)
            {
                object? value = values[i];
                string? formattedValue = formattedValues?[i];
                string separator = (completeValues.Length != 0) ? ", " : "";
                completeValues.AppendLine($"{separator}({i}, {formattedValue ?? value?.ToString() ?? "NULL"})");
            }

            string insertStatement = $"INSERT INTO {tableName} ({indexColumnName}, {columnName}) VALUES {completeValues}";
            return insertStatement;
        }

        public Version VendorVersionAsVersion => new Lazy<Version>(() => new Version(VendorVersion)).Value;

        public AdbcConnection Connection => _getConnection();
    }
}
