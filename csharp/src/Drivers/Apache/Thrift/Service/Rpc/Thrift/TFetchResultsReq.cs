/**
 * <auto-generated>
 * Autogenerated by Thrift Compiler (0.17.0)
 * BUT THIS FILE HAS BEEN HAND EDITED TO DISABLE NULLABLE SO REGENERATE AT YOUR OWN RISK
 * </auto-generated>
 */
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Thrift;
using Thrift.Collections;
using Thrift.Protocol;
using Thrift.Protocol.Entities;
using Thrift.Protocol.Utilities;
using Thrift.Transport;
using Thrift.Transport.Client;
using Thrift.Transport.Server;
using Thrift.Processor;

#nullable disable

#pragma warning disable IDE0079  // remove unnecessary pragmas
#pragma warning disable IDE0017  // object init can be simplified
#pragma warning disable IDE0028  // collection init can be simplified
#pragma warning disable IDE1006  // parts of the code use IDL spelling
#pragma warning disable CA1822   // empty DeepCopy() methods still non-static
#pragma warning disable IDE0083  // pattern matching "that is not SomeType" requires net5.0 but we still support earlier versions

namespace Apache.Hive.Service.Rpc.Thrift
{

  public partial class TFetchResultsReq : TBase
  {
    private short _fetchType;
    private long _maxBytes;
    private long _startRowOffset;
    private bool _includeResultSetMetadata;

    public global::Apache.Hive.Service.Rpc.Thrift.TOperationHandle OperationHandle { get; set; }

    /// <summary>
    ///
    /// <seealso cref="global::Apache.Hive.Service.Rpc.Thrift.TFetchOrientation"/>
    /// </summary>
    public global::Apache.Hive.Service.Rpc.Thrift.TFetchOrientation Orientation { get; set; }

    public long MaxRows { get; set; }

    public short FetchType
    {
      get
      {
        return _fetchType;
      }
      set
      {
        __isset.fetchType = true;
        this._fetchType = value;
      }
    }

    public long MaxBytes
    {
      get
      {
        return _maxBytes;
      }
      set
      {
        __isset.maxBytes = true;
        this._maxBytes = value;
      }
    }

    public long StartRowOffset
    {
      get
      {
        return _startRowOffset;
      }
      set
      {
        __isset.startRowOffset = true;
        this._startRowOffset = value;
      }
    }

    public bool IncludeResultSetMetadata
    {
      get
      {
        return _includeResultSetMetadata;
      }
      set
      {
        __isset.includeResultSetMetadata = true;
        this._includeResultSetMetadata = value;
      }
    }


    public Isset __isset;
    public struct Isset
    {
      public bool fetchType;
      public bool maxBytes;
      public bool startRowOffset;
      public bool includeResultSetMetadata;
    }

    public TFetchResultsReq()
    {
      this.Orientation = global::Apache.Hive.Service.Rpc.Thrift.TFetchOrientation.FETCH_NEXT;
      this._fetchType = 0;
      this.__isset.fetchType = true;
    }

    public TFetchResultsReq(global::Apache.Hive.Service.Rpc.Thrift.TOperationHandle operationHandle, global::Apache.Hive.Service.Rpc.Thrift.TFetchOrientation orientation, long maxRows) : this()
    {
      this.OperationHandle = operationHandle;
      this.Orientation = orientation;
      this.MaxRows = maxRows;
    }

    public TFetchResultsReq DeepCopy()
    {
      var tmp588 = new TFetchResultsReq();
      if ((OperationHandle != null))
      {
        tmp588.OperationHandle = (global::Apache.Hive.Service.Rpc.Thrift.TOperationHandle)this.OperationHandle.DeepCopy();
      }
      tmp588.Orientation = this.Orientation;
      tmp588.MaxRows = this.MaxRows;
      if (__isset.fetchType)
      {
        tmp588.FetchType = this.FetchType;
      }
      tmp588.__isset.fetchType = this.__isset.fetchType;
      if (__isset.maxBytes)
      {
        tmp588.MaxBytes = this.MaxBytes;
      }
      tmp588.__isset.maxBytes = this.__isset.maxBytes;
      if (__isset.startRowOffset)
      {
        tmp588.StartRowOffset = this.StartRowOffset;
      }
      tmp588.__isset.startRowOffset = this.__isset.startRowOffset;
      if (__isset.includeResultSetMetadata)
      {
        tmp588.IncludeResultSetMetadata = this.IncludeResultSetMetadata;
      }
      tmp588.__isset.includeResultSetMetadata = this.__isset.includeResultSetMetadata;
      return tmp588;
    }

    public async global::System.Threading.Tasks.Task ReadAsync(TProtocol iprot, CancellationToken cancellationToken)
    {
      iprot.IncrementRecursionDepth();
      try
      {
        bool isset_operationHandle = false;
        bool isset_orientation = false;
        bool isset_maxRows = false;
        TField field;
        await iprot.ReadStructBeginAsync(cancellationToken);
        while (true)
        {
          field = await iprot.ReadFieldBeginAsync(cancellationToken);
          if (field.Type == TType.Stop)
          {
            break;
          }

          switch (field.ID)
          {
            case 1:
              if (field.Type == TType.Struct)
              {
                OperationHandle = new global::Apache.Hive.Service.Rpc.Thrift.TOperationHandle();
                await OperationHandle.ReadAsync(iprot, cancellationToken);
                isset_operationHandle = true;
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 2:
              if (field.Type == TType.I32)
              {
                Orientation = (global::Apache.Hive.Service.Rpc.Thrift.TFetchOrientation)await iprot.ReadI32Async(cancellationToken);
                isset_orientation = true;
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 3:
              if (field.Type == TType.I64)
              {
                MaxRows = await iprot.ReadI64Async(cancellationToken);
                isset_maxRows = true;
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 4:
              if (field.Type == TType.I16)
              {
                FetchType = await iprot.ReadI16Async(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 1281:
              if (field.Type == TType.I64)
              {
                MaxBytes = await iprot.ReadI64Async(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 1282:
              if (field.Type == TType.I64)
              {
                StartRowOffset = await iprot.ReadI64Async(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 1283:
              if (field.Type == TType.Bool)
              {
                IncludeResultSetMetadata = await iprot.ReadBoolAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            default:
              await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              break;
          }

          await iprot.ReadFieldEndAsync(cancellationToken);
        }

        await iprot.ReadStructEndAsync(cancellationToken);
        if (!isset_operationHandle)
        {
          throw new TProtocolException(TProtocolException.INVALID_DATA);
        }
        if (!isset_orientation)
        {
          throw new TProtocolException(TProtocolException.INVALID_DATA);
        }
        if (!isset_maxRows)
        {
          throw new TProtocolException(TProtocolException.INVALID_DATA);
        }
      }
      finally
      {
        iprot.DecrementRecursionDepth();
      }
    }

    public async global::System.Threading.Tasks.Task WriteAsync(TProtocol oprot, CancellationToken cancellationToken)
    {
      oprot.IncrementRecursionDepth();
      try
      {
        var tmp589 = new TStruct("TFetchResultsReq");
        await oprot.WriteStructBeginAsync(tmp589, cancellationToken);
        var tmp590 = new TField();
        if ((OperationHandle != null))
        {
          tmp590.Name = "operationHandle";
          tmp590.Type = TType.Struct;
          tmp590.ID = 1;
          await oprot.WriteFieldBeginAsync(tmp590, cancellationToken);
          await OperationHandle.WriteAsync(oprot, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        tmp590.Name = "orientation";
        tmp590.Type = TType.I32;
        tmp590.ID = 2;
        await oprot.WriteFieldBeginAsync(tmp590, cancellationToken);
        await oprot.WriteI32Async((int)Orientation, cancellationToken);
        await oprot.WriteFieldEndAsync(cancellationToken);
        tmp590.Name = "maxRows";
        tmp590.Type = TType.I64;
        tmp590.ID = 3;
        await oprot.WriteFieldBeginAsync(tmp590, cancellationToken);
        await oprot.WriteI64Async(MaxRows, cancellationToken);
        await oprot.WriteFieldEndAsync(cancellationToken);
        if (__isset.fetchType)
        {
          tmp590.Name = "fetchType";
          tmp590.Type = TType.I16;
          tmp590.ID = 4;
          await oprot.WriteFieldBeginAsync(tmp590, cancellationToken);
          await oprot.WriteI16Async(FetchType, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if (__isset.maxBytes)
        {
          tmp590.Name = "maxBytes";
          tmp590.Type = TType.I64;
          tmp590.ID = 1281;
          await oprot.WriteFieldBeginAsync(tmp590, cancellationToken);
          await oprot.WriteI64Async(MaxBytes, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if (__isset.startRowOffset)
        {
          tmp590.Name = "startRowOffset";
          tmp590.Type = TType.I64;
          tmp590.ID = 1282;
          await oprot.WriteFieldBeginAsync(tmp590, cancellationToken);
          await oprot.WriteI64Async(StartRowOffset, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if (__isset.includeResultSetMetadata)
        {
          tmp590.Name = "includeResultSetMetadata";
          tmp590.Type = TType.Bool;
          tmp590.ID = 1283;
          await oprot.WriteFieldBeginAsync(tmp590, cancellationToken);
          await oprot.WriteBoolAsync(IncludeResultSetMetadata, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        await oprot.WriteFieldStopAsync(cancellationToken);
        await oprot.WriteStructEndAsync(cancellationToken);
      }
      finally
      {
        oprot.DecrementRecursionDepth();
      }
    }

    public override bool Equals(object that)
    {
      if (!(that is TFetchResultsReq other)) return false;
      if (ReferenceEquals(this, other)) return true;
      return global::System.Object.Equals(OperationHandle, other.OperationHandle)
        && global::System.Object.Equals(Orientation, other.Orientation)
        && global::System.Object.Equals(MaxRows, other.MaxRows)
        && ((__isset.fetchType == other.__isset.fetchType) && ((!__isset.fetchType) || (global::System.Object.Equals(FetchType, other.FetchType))))
        && ((__isset.maxBytes == other.__isset.maxBytes) && ((!__isset.maxBytes) || (global::System.Object.Equals(MaxBytes, other.MaxBytes))))
        && ((__isset.startRowOffset == other.__isset.startRowOffset) && ((!__isset.startRowOffset) || (global::System.Object.Equals(StartRowOffset, other.StartRowOffset))))
        && ((__isset.includeResultSetMetadata == other.__isset.includeResultSetMetadata) && ((!__isset.includeResultSetMetadata) || (global::System.Object.Equals(IncludeResultSetMetadata, other.IncludeResultSetMetadata))));
    }

    public override int GetHashCode() {
      int hashcode = 157;
      unchecked {
        if ((OperationHandle != null))
        {
          hashcode = (hashcode * 397) + OperationHandle.GetHashCode();
        }
        hashcode = (hashcode * 397) + Orientation.GetHashCode();
        hashcode = (hashcode * 397) + MaxRows.GetHashCode();
        if (__isset.fetchType)
        {
          hashcode = (hashcode * 397) + FetchType.GetHashCode();
        }
        if (__isset.maxBytes)
        {
          hashcode = (hashcode * 397) + MaxBytes.GetHashCode();
        }
        if (__isset.startRowOffset)
        {
          hashcode = (hashcode * 397) + StartRowOffset.GetHashCode();
        }
        if (__isset.includeResultSetMetadata)
        {
          hashcode = (hashcode * 397) + IncludeResultSetMetadata.GetHashCode();
        }
      }
      return hashcode;
    }

    public override string ToString()
    {
      var tmp591 = new StringBuilder("TFetchResultsReq(");
      if ((OperationHandle != null))
      {
        tmp591.Append(", OperationHandle: ");
        OperationHandle.ToString(tmp591);
      }
      tmp591.Append(", Orientation: ");
      Orientation.ToString(tmp591);
      tmp591.Append(", MaxRows: ");
      MaxRows.ToString(tmp591);
      if (__isset.fetchType)
      {
        tmp591.Append(", FetchType: ");
        FetchType.ToString(tmp591);
      }
      if (__isset.maxBytes)
      {
        tmp591.Append(", MaxBytes: ");
        MaxBytes.ToString(tmp591);
      }
      if (__isset.startRowOffset)
      {
        tmp591.Append(", StartRowOffset: ");
        StartRowOffset.ToString(tmp591);
      }
      if (__isset.includeResultSetMetadata)
      {
        tmp591.Append(", IncludeResultSetMetadata: ");
        IncludeResultSetMetadata.ToString(tmp591);
      }
      tmp591.Append(')');
      return tmp591.ToString();
    }
  }

}