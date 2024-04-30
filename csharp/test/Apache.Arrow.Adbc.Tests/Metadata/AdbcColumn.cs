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

namespace Apache.Arrow.Adbc.Tests.Metadata
{
    /// <summary>
    /// Represents the column in the metadata.
    /// </summary>
    public class AdbcColumn
    {
        /// <summary>
        /// Column name
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// Column ordinal position
        /// </summary>
        public int? OrdinalPosition { get; set; }

        /// <summary>
        /// Column remarks
        /// </summary>
        public string? Remarks { get; set; }

        /// <summary>
        /// Column XDBC data type
        /// </summary>
        public short? XdbcDataType { get; set; }

        /// <summary>
        /// Column XDBC type name
        /// </summary>
        public string? XdbcTypeName { get; set; }

        /// <summary>
        /// Column XDBC column size
        /// </summary>
        public int? XdbcColumnSize { get; set; }

        /// <summary>
        /// Column XDBC data type
        /// </summary>
        public short? XdbcDecimalDigits { get; set; }

        /// <summary>
        /// Column XDBC numeric precision radix
        /// </summary>
        public short? XdbcNumPrecRadix { get; set; }

        /// <summary>
        /// Column XDBC nullable
        /// </summary>
        public short? XdbcNullable { get; set; }

        /// <summary>
        /// Column XDBC column definition
        /// </summary>
        public string? XdbcColumnDef { get; set; }

        /// <summary>
        /// Column XDBC SQL data type
        /// </summary>
        public short? XdbcSqlDataType { get; set; }

        /// <summary>
        /// Column XDBC datetime sub
        /// </summary>
        public short? XdbcDatetimeSub { get; set; }

        /// <summary>
        /// Column XDBC char octet length
        /// </summary>
        public int? XdbcCharOctetLength { get; set; }

        /// <summary>
        /// Column XDBC is nullable (YES/NO)
        /// </summary>
        public string? XdbcIsNullable { get; set; }

        /// <summary>
        /// Column XDBC scope catalog
        /// </summary>
        public string? XdbcScopeCatalog { get; set; }

        /// <summary>
        /// Column XDBC scope schema
        /// </summary>
        public string? XdbcScopeSchema { get; set; }

        /// <summary>
        /// Column XDBC scope table
        /// </summary>
        public string? XdbcScopeTable { get; set; }

        /// <summary>
        /// Column XDBC is auto increment
        /// </summary>
        public bool? XdbcIsAutoIncrement { get; set; }

        /// <summary>
        /// Column XDBC is generated column
        /// </summary>
        public bool? XdbcIsGeneratedColumn { get; set; }

    }
}
