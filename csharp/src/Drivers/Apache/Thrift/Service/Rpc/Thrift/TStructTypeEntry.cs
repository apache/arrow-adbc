/**
 * <auto-generated>
 * Autogenerated by Thrift Compiler (0.21.0)
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
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


// targeting netstandard 2.x
#if(! NETSTANDARD2_0_OR_GREATER && ! NET6_0_OR_GREATER && ! NET472_OR_GREATER)
#error Unexpected target platform. See 'thrift --help' for details.
#endif

#pragma warning disable IDE0079  // remove unnecessary pragmas
#pragma warning disable IDE0017  // object init can be simplified
#pragma warning disable IDE0028  // collection init can be simplified
#pragma warning disable IDE1006  // parts of the code use IDL spelling
#pragma warning disable CA1822   // empty DeepCopy() methods still non-static
#pragma warning disable CS0618   // silence our own deprecation warnings
#pragma warning disable IDE0083  // pattern matching "that is not SomeType" requires net5.0 but we still support earlier versions

namespace Apache.Hive.Service.Rpc.Thrift
{

  internal partial class TStructTypeEntry : TBase
  {

    public Dictionary<string, int> NameToTypePtr { get; set; }

    public TStructTypeEntry()
    {
    }

    public TStructTypeEntry(Dictionary<string, int> nameToTypePtr) : this()
    {
      this.NameToTypePtr = nameToTypePtr;
    }

    public async global::System.Threading.Tasks.Task ReadAsync(TProtocol iprot, CancellationToken cancellationToken)
    {
      iprot.IncrementRecursionDepth();
      try
      {
        bool isset_nameToTypePtr = false;
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
              if (field.Type == TType.Map)
              {
                {
                  var _map25 = await iprot.ReadMapBeginAsync(cancellationToken);
                  NameToTypePtr = new Dictionary<string, int>(_map25.Count);
                  for(int _i26 = 0; _i26 < _map25.Count; ++_i26)
                  {
                    string _key27;
                    int _val28;
                    _key27 = await iprot.ReadStringAsync(cancellationToken);
                    _val28 = await iprot.ReadI32Async(cancellationToken);
                    NameToTypePtr[_key27] = _val28;
                  }
                  await iprot.ReadMapEndAsync(cancellationToken);
                }
                isset_nameToTypePtr = true;
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
        if (!isset_nameToTypePtr)
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
        var tmp29 = new TStruct("TStructTypeEntry");
        await oprot.WriteStructBeginAsync(tmp29, cancellationToken);
        var tmp30 = new TField();
        if((NameToTypePtr != null))
        {
          tmp30.Name = "nameToTypePtr";
          tmp30.Type = TType.Map;
          tmp30.ID = 1;
          await oprot.WriteFieldBeginAsync(tmp30, cancellationToken);
          await oprot.WriteMapBeginAsync(new TMap(TType.String, TType.I32, NameToTypePtr.Count), cancellationToken);
          foreach (string _iter31 in NameToTypePtr.Keys)
          {
            await oprot.WriteStringAsync(_iter31, cancellationToken);
            await oprot.WriteI32Async(NameToTypePtr[_iter31], cancellationToken);
          }
          await oprot.WriteMapEndAsync(cancellationToken);
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
      if (!(that is TStructTypeEntry other)) return false;
      if (ReferenceEquals(this, other)) return true;
      return TCollections.Equals(NameToTypePtr, other.NameToTypePtr);
    }

    public override int GetHashCode() {
      int hashcode = 157;
      unchecked {
        if((NameToTypePtr != null))
        {
          hashcode = (hashcode * 397) + TCollections.GetHashCode(NameToTypePtr);
        }
      }
      return hashcode;
    }

    public override string ToString()
    {
      var tmp32 = new StringBuilder("TStructTypeEntry(");
      if((NameToTypePtr != null))
      {
        tmp32.Append(", NameToTypePtr: ");
        NameToTypePtr.ToString(tmp32);
      }
      tmp32.Append(')');
      return tmp32.ToString();
    }
  }

}
