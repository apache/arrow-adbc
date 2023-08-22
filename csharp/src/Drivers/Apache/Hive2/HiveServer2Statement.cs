using System;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    public abstract class HiveServer2Statement : AdbcStatement
    {
        protected HiveServer2Connection connection;
        protected TOperationHandle operationHandle;

        protected HiveServer2Statement(HiveServer2Connection connection)
        {
            this.connection = connection;
        }

        protected virtual void SetStatementProperties(TExecuteStatementReq statement)
        {
        }

        protected void ExecuteStatement()
        {
            TExecuteStatementReq executeRequest = new TExecuteStatementReq(this.connection.sessionHandle, this.SqlQuery);
            SetStatementProperties(executeRequest);
            var executeResponse = this.connection.client.ExecuteStatement(executeRequest).Result;
            if (executeResponse.Status.StatusCode == TStatusCode.ERROR_STATUS)
            {
                throw new Exception(executeResponse.Status.ErrorMessage);
            }

            this.operationHandle = executeResponse.OperationHandle;
        }

        protected void PollForResponse()
        {
            TGetOperationStatusResp statusResponse = null;
            do
            {
                if (statusResponse != null) { Thread.Sleep(500); }
                TGetOperationStatusReq request = new TGetOperationStatusReq(this.operationHandle);
                statusResponse = this.connection.client.GetOperationStatus(request).Result;
            } while (statusResponse.OperationState == TOperationState.PENDING_STATE || statusResponse.OperationState == TOperationState.RUNNING_STATE);
        }

        protected Schema GetSchema()
        {
            TGetResultSetMetadataReq request = new TGetResultSetMetadataReq(this.operationHandle);
            TGetResultSetMetadataResp response = this.connection.client.GetResultSetMetadata(request).Result;
            return GetArrowSchema(response.Schema);
        }

        public override void Dispose()
        {
            if (this.operationHandle != null)
            {
                TCloseOperationReq request = new TCloseOperationReq(this.operationHandle);
                this.connection.client.CloseOperation(request).Wait();
                this.operationHandle = null;
            }

            base.Dispose();
        }

        static Schema GetArrowSchema(TTableSchema thriftSchema)
        {
            Field[] fields = new Field[thriftSchema.Columns.Count];
            for (int i = 0; i < thriftSchema.Columns.Count; i++)
            {
                TColumnDesc column = thriftSchema.Columns[i];
                fields[i] = new Field(column.ColumnName, GetArrowType(column.TypeDesc.Types[0]), nullable: true /* ??? */);
            }
            return new Schema(fields, null);
        }

        static IArrowType GetArrowType(TTypeEntry thriftType)
        {
            if (thriftType.PrimitiveEntry != null)
            {
                return GetArrowType(thriftType.PrimitiveEntry.Type);
            }
            throw new InvalidOperationException();
        }

        static IArrowType GetArrowType(TTypeId thriftType)
        {
            switch (thriftType)
            {
                case TTypeId.BIGINT_TYPE: return Int64Type.Default;
                case TTypeId.BINARY_TYPE: return BinaryType.Default;
                case TTypeId.BOOLEAN_TYPE: return BooleanType.Default;
                case TTypeId.CHAR_TYPE: return StringType.Default;
                case TTypeId.DATE_TYPE: return Date32Type.Default;
                case TTypeId.DOUBLE_TYPE: return DoubleType.Default;
                case TTypeId.FLOAT_TYPE: return FloatType.Default;
                case TTypeId.INT_TYPE: return Int32Type.Default;
                case TTypeId.NULL_TYPE: return NullType.Default;
                case TTypeId.SMALLINT_TYPE: return Int16Type.Default;
                case TTypeId.STRING_TYPE: return StringType.Default;
                case TTypeId.TIMESTAMP_TYPE: return new TimestampType(TimeUnit.Microsecond, (string)null);
                case TTypeId.TINYINT_TYPE: return Int8Type.Default;
                case TTypeId.VARCHAR_TYPE: return StringType.Default;

                // ???
                case TTypeId.DECIMAL_TYPE:
                case TTypeId.INTERVAL_DAY_TIME_TYPE:
                case TTypeId.INTERVAL_YEAR_MONTH_TYPE:
                    return StringType.Default;

                case TTypeId.ARRAY_TYPE:
                case TTypeId.MAP_TYPE:
                case TTypeId.STRUCT_TYPE:
                case TTypeId.UNION_TYPE:
                case TTypeId.USER_DEFINED_TYPE:
                    return StringType.Default;
                default:
                    throw new NotImplementedException();
            }
        }
    }
}
