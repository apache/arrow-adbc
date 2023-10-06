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
using System.Data.Common;
using Apache.Arrow.Types;

namespace Apache.Arrow.Adbc.Client
{
    /// <summary>
    /// An ADBC-specific implementation of <see cref="DbColumn"/>
    /// </summary>
    public class AdbcColumn : DbColumn
    {
        /// <summary>
        /// Initializes an AdbcColumn.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="dataType">The type of column.</param>
        /// <param name="nullable">
        /// Indicates whether the column allows DbNull values.
        /// </param>
        public AdbcColumn(string name, Type dataType, IArrowType arrowType, bool nullable)
            : this(name, dataType, arrowType, nullable, null, null)
        {

        }

        /// <summary>
        /// Initializes an AdbcColumn.
        /// </summary>
        /// <param name="name">The column name.</param>
        /// <param name="dataType">The type of column.</param>
        /// <param name="nullable">
        /// Indicates whether the column allows DbNull values.
        /// </param>
        /// <param name="precision">
        /// The decimal precision, if required.
        /// </param>
        /// <param name="scale">The decimal scale, if required.</param>
        public AdbcColumn(string name, Type dataType, IArrowType arrowType, bool nullable, int? precision, int? scale)
        {
            this.ColumnName = name;
            this.AllowDBNull = nullable;
            this.DataType = dataType;
            this.ArrowType = arrowType;

            if(precision.HasValue && scale.HasValue)
            {
                this.NumericScale = scale.Value;
                this.NumericPrecision = precision.Value;
            }
        }

        public IArrowType ArrowType { get; set; }
    }
}
