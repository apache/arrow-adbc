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
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    internal abstract class SchemaParser
    {
        internal Schema GetArrowSchema(TTableSchema thriftSchema, DataTypeConversion dataTypeConversion)
        {
            Field[] fields = new Field[thriftSchema.Columns.Count];
            for (int i = 0; i < thriftSchema.Columns.Count; i++)
            {
                TColumnDesc column = thriftSchema.Columns[i];
                // Note: no nullable metadata is returned from the Thrift interface.
                fields[i] = new Field(column.ColumnName, GetArrowType(column.TypeDesc.Types[0], dataTypeConversion), nullable: true /* assumed */);
            }
            return new Schema(fields, null);
        }

        IArrowType GetArrowType(TTypeEntry thriftType, DataTypeConversion dataTypeConversion)
        {
            if (thriftType.PrimitiveEntry != null)
            {
                return GetArrowType(thriftType.PrimitiveEntry, dataTypeConversion);
            }
            throw new InvalidOperationException();
        }

        public abstract IArrowType GetArrowType(TPrimitiveTypeEntry thriftType, DataTypeConversion dataTypeConversion);

        protected static Decimal128Type NewDecima128Type(TPrimitiveTypeEntry thriftType) =>
            new(thriftType.TypeQualifiers.Qualifiers["precision"].I32Value, thriftType.TypeQualifiers.Qualifiers["scale"].I32Value);
    }
}
