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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal abstract class SparkConnection : HiveServer2Connection
    {
        internal static readonly string s_userAgent = $"{InfoDriverName.Replace(" ", "")}/{ProductVersionDefault}";

        readonly AdbcInfoCode[] infoSupportedCodes = new[] {
            AdbcInfoCode.DriverName,
            AdbcInfoCode.DriverVersion,
            AdbcInfoCode.DriverArrowVersion,
            AdbcInfoCode.VendorName,
            AdbcInfoCode.VendorSql,
            AdbcInfoCode.VendorVersion,
        };

        const string ProductVersionDefault = "1.0.0";
        const string InfoDriverName = "ADBC Spark Driver";
        const string InfoDriverArrowVersion = "1.0.0";
        const bool InfoVendorSql = true;

        private readonly Lazy<string> _productVersion;

        protected override string GetProductVersionDefault() => ProductVersionDefault;

        internal static TSparkGetDirectResults sparkGetDirectResults = new TSparkGetDirectResults(1000);

        internal static readonly Dictionary<string, string> timestampConfig = new Dictionary<string, string>
        {
            { "spark.thriftserver.arrowBasedRowSet.timestampAsString", "false" }
        };

        internal SparkConnection(IReadOnlyDictionary<string, string> properties)
            : base(properties)
        {
            ValidateProperties();
            _productVersion = new Lazy<string>(() => GetProductVersion(), LazyThreadSafetyMode.PublicationOnly);
        }

        private void ValidateProperties()
        {
            ValidateAuthentication();
            ValidateConnection();
            ValidateOptions();
        }

        protected string ProductVersion => _productVersion.Value;

        public override AdbcStatement CreateStatement()
        {
            return new SparkStatement(this);
        }

        public override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
        {
            const int strValTypeID = 0;
            const int boolValTypeId = 1;

            UnionType infoUnionType = new UnionType(
                new Field[]
                {
                    new Field("string_value", StringType.Default, true),
                    new Field("bool_value", BooleanType.Default, true),
                    new Field("int64_value", Int64Type.Default, true),
                    new Field("int32_bitmask", Int32Type.Default, true),
                    new Field(
                        "string_list",
                        new ListType(
                            new Field("item", StringType.Default, true)
                        ),
                        false
                    ),
                    new Field(
                        "int32_to_int32_list_map",
                        new ListType(
                            new Field("entries", new StructType(
                                new Field[]
                                {
                                    new Field("key", Int32Type.Default, false),
                                    new Field("value", Int32Type.Default, true),
                                }
                                ), false)
                        ),
                        true
                    )
                },
                new int[] { 0, 1, 2, 3, 4, 5 },
                UnionMode.Dense);

            if (codes.Count == 0)
            {
                codes = infoSupportedCodes;
            }

            UInt32Array.Builder infoNameBuilder = new UInt32Array.Builder();
            ArrowBuffer.Builder<byte> typeBuilder = new ArrowBuffer.Builder<byte>();
            ArrowBuffer.Builder<int> offsetBuilder = new ArrowBuffer.Builder<int>();
            StringArray.Builder stringInfoBuilder = new StringArray.Builder();
            BooleanArray.Builder booleanInfoBuilder = new BooleanArray.Builder();

            int nullCount = 0;
            int arrayLength = codes.Count;
            int offset = 0;

            foreach (AdbcInfoCode code in codes)
            {
                switch (code)
                {
                    case AdbcInfoCode.DriverName:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.Append(InfoDriverName);
                        booleanInfoBuilder.AppendNull();
                        break;
                    case AdbcInfoCode.DriverVersion:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.Append(ProductVersion);
                        booleanInfoBuilder.AppendNull();
                        break;
                    case AdbcInfoCode.DriverArrowVersion:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.Append(InfoDriverArrowVersion);
                        booleanInfoBuilder.AppendNull();
                        break;
                    case AdbcInfoCode.VendorName:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        string vendorName = VendorName;
                        stringInfoBuilder.Append(vendorName);
                        booleanInfoBuilder.AppendNull();
                        break;
                    case AdbcInfoCode.VendorVersion:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        string? vendorVersion = VendorVersion;
                        stringInfoBuilder.Append(vendorVersion);
                        booleanInfoBuilder.AppendNull();
                        break;
                    case AdbcInfoCode.VendorSql:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(boolValTypeId);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.AppendNull();
                        booleanInfoBuilder.Append(InfoVendorSql);
                        break;
                    default:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.AppendNull();
                        booleanInfoBuilder.AppendNull();
                        nullCount++;
                        break;
                }
            }

            StructType entryType = new StructType(
                new Field[] {
                    new Field("key", Int32Type.Default, false),
                    new Field("value", Int32Type.Default, true)});

            StructArray entriesDataArray = new StructArray(entryType, 0,
                new[] { new Int32Array.Builder().Build(), new Int32Array.Builder().Build() },
                new ArrowBuffer.BitmapBuilder().Build());

            IArrowArray[] childrenArrays = new IArrowArray[]
            {
                stringInfoBuilder.Build(),
                booleanInfoBuilder.Build(),
                new Int64Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new ListArray.Builder(StringType.Default).Build(),
                new List<IArrowArray?>(){ entriesDataArray }.BuildListArrayForType(entryType)
            };

            DenseUnionArray infoValue = new DenseUnionArray(infoUnionType, arrayLength, childrenArrays, typeBuilder.Build(), offsetBuilder.Build(), nullCount);

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                infoNameBuilder.Build(),
                infoValue
            };
            StandardSchemas.GetInfoSchema.Validate(dataArrays);

            return new HiveInfoArrowStream(StandardSchemas.GetInfoSchema, dataArrays);

        }

        protected override IReadOnlyDictionary<string, int> GetColumnIndexMap(List<TColumnDesc> columns) => columns
            .Select(t => new { Index = t.Position - 1, t.ColumnName })
            .ToDictionary(t => t.ColumnName, t => t.Index);

        protected override bool AreResultsAvailableDirectly() => true;

        protected override TSparkGetDirectResults GetDirectResults() => sparkGetDirectResults;

        protected abstract void ValidateConnection();
        protected abstract void ValidateAuthentication();
        protected abstract void ValidateOptions();

        internal abstract SparkServerType ServerType { get; }   
    }
}
