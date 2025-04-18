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

  internal partial class TExpressionInfo : TBase
  {
    private string _className;
    private string _usage;
    private string _name;
    private string _extended;
    private string _db;
    private string _arguments;
    private string _examples;
    private string _note;
    private string _group;
    private string _since;
    private string _deprecated;
    private string _source;

    public string ClassName
    {
      get
      {
        return _className;
      }
      set
      {
        __isset.className = true;
        this._className = value;
      }
    }

    public string Usage
    {
      get
      {
        return _usage;
      }
      set
      {
        __isset.@usage = true;
        this._usage = value;
      }
    }

    public string Name
    {
      get
      {
        return _name;
      }
      set
      {
        __isset.@name = true;
        this._name = value;
      }
    }

    public string Extended
    {
      get
      {
        return _extended;
      }
      set
      {
        __isset.@extended = true;
        this._extended = value;
      }
    }

    public string Db
    {
      get
      {
        return _db;
      }
      set
      {
        __isset.@db = true;
        this._db = value;
      }
    }

    public string Arguments
    {
      get
      {
        return _arguments;
      }
      set
      {
        __isset.@arguments = true;
        this._arguments = value;
      }
    }

    public string Examples
    {
      get
      {
        return _examples;
      }
      set
      {
        __isset.@examples = true;
        this._examples = value;
      }
    }

    public string Note
    {
      get
      {
        return _note;
      }
      set
      {
        __isset.@note = true;
        this._note = value;
      }
    }

    public string Group
    {
      get
      {
        return _group;
      }
      set
      {
        __isset.@group = true;
        this._group = value;
      }
    }

    public string Since
    {
      get
      {
        return _since;
      }
      set
      {
        __isset.@since = true;
        this._since = value;
      }
    }

    public string Deprecated
    {
      get
      {
        return _deprecated;
      }
      set
      {
        __isset.@deprecated = true;
        this._deprecated = value;
      }
    }

    public string Source
    {
      get
      {
        return _source;
      }
      set
      {
        __isset.@source = true;
        this._source = value;
      }
    }


    public Isset __isset;
    public struct Isset
    {
      public bool className;
      public bool @usage;
      public bool @name;
      public bool @extended;
      public bool @db;
      public bool @arguments;
      public bool @examples;
      public bool @note;
      public bool @group;
      public bool @since;
      public bool @deprecated;
      public bool @source;
    }

    public TExpressionInfo()
    {
    }

    public async global::System.Threading.Tasks.Task ReadAsync(TProtocol iprot, CancellationToken cancellationToken)
    {
      iprot.IncrementRecursionDepth();
      try
      {
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
              if (field.Type == TType.String)
              {
                ClassName = await iprot.ReadStringAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 2:
              if (field.Type == TType.String)
              {
                Usage = await iprot.ReadStringAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 3:
              if (field.Type == TType.String)
              {
                Name = await iprot.ReadStringAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 4:
              if (field.Type == TType.String)
              {
                Extended = await iprot.ReadStringAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 5:
              if (field.Type == TType.String)
              {
                Db = await iprot.ReadStringAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 6:
              if (field.Type == TType.String)
              {
                Arguments = await iprot.ReadStringAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 7:
              if (field.Type == TType.String)
              {
                Examples = await iprot.ReadStringAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 8:
              if (field.Type == TType.String)
              {
                Note = await iprot.ReadStringAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 9:
              if (field.Type == TType.String)
              {
                Group = await iprot.ReadStringAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 10:
              if (field.Type == TType.String)
              {
                Since = await iprot.ReadStringAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 11:
              if (field.Type == TType.String)
              {
                Deprecated = await iprot.ReadStringAsync(cancellationToken);
              }
              else
              {
                await TProtocolUtil.SkipAsync(iprot, field.Type, cancellationToken);
              }
              break;
            case 12:
              if (field.Type == TType.String)
              {
                Source = await iprot.ReadStringAsync(cancellationToken);
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
        var tmp254 = new TStruct("TExpressionInfo");
        await oprot.WriteStructBeginAsync(tmp254, cancellationToken);
        var tmp255 = new TField();
        if((ClassName != null) && __isset.className)
        {
          tmp255.Name = "className";
          tmp255.Type = TType.String;
          tmp255.ID = 1;
          await oprot.WriteFieldBeginAsync(tmp255, cancellationToken);
          await oprot.WriteStringAsync(ClassName, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if((Usage != null) && __isset.@usage)
        {
          tmp255.Name = "usage";
          tmp255.Type = TType.String;
          tmp255.ID = 2;
          await oprot.WriteFieldBeginAsync(tmp255, cancellationToken);
          await oprot.WriteStringAsync(Usage, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if((Name != null) && __isset.@name)
        {
          tmp255.Name = "name";
          tmp255.Type = TType.String;
          tmp255.ID = 3;
          await oprot.WriteFieldBeginAsync(tmp255, cancellationToken);
          await oprot.WriteStringAsync(Name, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if((Extended != null) && __isset.@extended)
        {
          tmp255.Name = "extended";
          tmp255.Type = TType.String;
          tmp255.ID = 4;
          await oprot.WriteFieldBeginAsync(tmp255, cancellationToken);
          await oprot.WriteStringAsync(Extended, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if((Db != null) && __isset.@db)
        {
          tmp255.Name = "db";
          tmp255.Type = TType.String;
          tmp255.ID = 5;
          await oprot.WriteFieldBeginAsync(tmp255, cancellationToken);
          await oprot.WriteStringAsync(Db, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if((Arguments != null) && __isset.@arguments)
        {
          tmp255.Name = "arguments";
          tmp255.Type = TType.String;
          tmp255.ID = 6;
          await oprot.WriteFieldBeginAsync(tmp255, cancellationToken);
          await oprot.WriteStringAsync(Arguments, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if((Examples != null) && __isset.@examples)
        {
          tmp255.Name = "examples";
          tmp255.Type = TType.String;
          tmp255.ID = 7;
          await oprot.WriteFieldBeginAsync(tmp255, cancellationToken);
          await oprot.WriteStringAsync(Examples, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if((Note != null) && __isset.@note)
        {
          tmp255.Name = "note";
          tmp255.Type = TType.String;
          tmp255.ID = 8;
          await oprot.WriteFieldBeginAsync(tmp255, cancellationToken);
          await oprot.WriteStringAsync(Note, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if((Group != null) && __isset.@group)
        {
          tmp255.Name = "group";
          tmp255.Type = TType.String;
          tmp255.ID = 9;
          await oprot.WriteFieldBeginAsync(tmp255, cancellationToken);
          await oprot.WriteStringAsync(Group, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if((Since != null) && __isset.@since)
        {
          tmp255.Name = "since";
          tmp255.Type = TType.String;
          tmp255.ID = 10;
          await oprot.WriteFieldBeginAsync(tmp255, cancellationToken);
          await oprot.WriteStringAsync(Since, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if((Deprecated != null) && __isset.@deprecated)
        {
          tmp255.Name = "deprecated";
          tmp255.Type = TType.String;
          tmp255.ID = 11;
          await oprot.WriteFieldBeginAsync(tmp255, cancellationToken);
          await oprot.WriteStringAsync(Deprecated, cancellationToken);
          await oprot.WriteFieldEndAsync(cancellationToken);
        }
        if((Source != null) && __isset.@source)
        {
          tmp255.Name = "source";
          tmp255.Type = TType.String;
          tmp255.ID = 12;
          await oprot.WriteFieldBeginAsync(tmp255, cancellationToken);
          await oprot.WriteStringAsync(Source, cancellationToken);
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
      if (!(that is TExpressionInfo other)) return false;
      if (ReferenceEquals(this, other)) return true;
      return ((__isset.className == other.__isset.className) && ((!__isset.className) || (global::System.Object.Equals(ClassName, other.ClassName))))
        && ((__isset.@usage == other.__isset.@usage) && ((!__isset.@usage) || (global::System.Object.Equals(Usage, other.Usage))))
        && ((__isset.@name == other.__isset.@name) && ((!__isset.@name) || (global::System.Object.Equals(Name, other.Name))))
        && ((__isset.@extended == other.__isset.@extended) && ((!__isset.@extended) || (global::System.Object.Equals(Extended, other.Extended))))
        && ((__isset.@db == other.__isset.@db) && ((!__isset.@db) || (global::System.Object.Equals(Db, other.Db))))
        && ((__isset.@arguments == other.__isset.@arguments) && ((!__isset.@arguments) || (global::System.Object.Equals(Arguments, other.Arguments))))
        && ((__isset.@examples == other.__isset.@examples) && ((!__isset.@examples) || (global::System.Object.Equals(Examples, other.Examples))))
        && ((__isset.@note == other.__isset.@note) && ((!__isset.@note) || (global::System.Object.Equals(Note, other.Note))))
        && ((__isset.@group == other.__isset.@group) && ((!__isset.@group) || (global::System.Object.Equals(Group, other.Group))))
        && ((__isset.@since == other.__isset.@since) && ((!__isset.@since) || (global::System.Object.Equals(Since, other.Since))))
        && ((__isset.@deprecated == other.__isset.@deprecated) && ((!__isset.@deprecated) || (global::System.Object.Equals(Deprecated, other.Deprecated))))
        && ((__isset.@source == other.__isset.@source) && ((!__isset.@source) || (global::System.Object.Equals(Source, other.Source))));
    }

    public override int GetHashCode() {
      int hashcode = 157;
      unchecked {
        if((ClassName != null) && __isset.className)
        {
          hashcode = (hashcode * 397) + ClassName.GetHashCode();
        }
        if((Usage != null) && __isset.@usage)
        {
          hashcode = (hashcode * 397) + Usage.GetHashCode();
        }
        if((Name != null) && __isset.@name)
        {
          hashcode = (hashcode * 397) + Name.GetHashCode();
        }
        if((Extended != null) && __isset.@extended)
        {
          hashcode = (hashcode * 397) + Extended.GetHashCode();
        }
        if((Db != null) && __isset.@db)
        {
          hashcode = (hashcode * 397) + Db.GetHashCode();
        }
        if((Arguments != null) && __isset.@arguments)
        {
          hashcode = (hashcode * 397) + Arguments.GetHashCode();
        }
        if((Examples != null) && __isset.@examples)
        {
          hashcode = (hashcode * 397) + Examples.GetHashCode();
        }
        if((Note != null) && __isset.@note)
        {
          hashcode = (hashcode * 397) + Note.GetHashCode();
        }
        if((Group != null) && __isset.@group)
        {
          hashcode = (hashcode * 397) + Group.GetHashCode();
        }
        if((Since != null) && __isset.@since)
        {
          hashcode = (hashcode * 397) + Since.GetHashCode();
        }
        if((Deprecated != null) && __isset.@deprecated)
        {
          hashcode = (hashcode * 397) + Deprecated.GetHashCode();
        }
        if((Source != null) && __isset.@source)
        {
          hashcode = (hashcode * 397) + Source.GetHashCode();
        }
      }
      return hashcode;
    }

    public override string ToString()
    {
      var tmp256 = new StringBuilder("TExpressionInfo(");
      int tmp257 = 0;
      if((ClassName != null) && __isset.className)
      {
        if(0 < tmp257++) { tmp256.Append(", "); }
        tmp256.Append("ClassName: ");
        ClassName.ToString(tmp256);
      }
      if((Usage != null) && __isset.@usage)
      {
        if(0 < tmp257++) { tmp256.Append(", "); }
        tmp256.Append("Usage: ");
        Usage.ToString(tmp256);
      }
      if((Name != null) && __isset.@name)
      {
        if(0 < tmp257++) { tmp256.Append(", "); }
        tmp256.Append("Name: ");
        Name.ToString(tmp256);
      }
      if((Extended != null) && __isset.@extended)
      {
        if(0 < tmp257++) { tmp256.Append(", "); }
        tmp256.Append("Extended: ");
        Extended.ToString(tmp256);
      }
      if((Db != null) && __isset.@db)
      {
        if(0 < tmp257++) { tmp256.Append(", "); }
        tmp256.Append("Db: ");
        Db.ToString(tmp256);
      }
      if((Arguments != null) && __isset.@arguments)
      {
        if(0 < tmp257++) { tmp256.Append(", "); }
        tmp256.Append("Arguments: ");
        Arguments.ToString(tmp256);
      }
      if((Examples != null) && __isset.@examples)
      {
        if(0 < tmp257++) { tmp256.Append(", "); }
        tmp256.Append("Examples: ");
        Examples.ToString(tmp256);
      }
      if((Note != null) && __isset.@note)
      {
        if(0 < tmp257++) { tmp256.Append(", "); }
        tmp256.Append("Note: ");
        Note.ToString(tmp256);
      }
      if((Group != null) && __isset.@group)
      {
        if(0 < tmp257++) { tmp256.Append(", "); }
        tmp256.Append("Group: ");
        Group.ToString(tmp256);
      }
      if((Since != null) && __isset.@since)
      {
        if(0 < tmp257++) { tmp256.Append(", "); }
        tmp256.Append("Since: ");
        Since.ToString(tmp256);
      }
      if((Deprecated != null) && __isset.@deprecated)
      {
        if(0 < tmp257++) { tmp256.Append(", "); }
        tmp256.Append("Deprecated: ");
        Deprecated.ToString(tmp256);
      }
      if((Source != null) && __isset.@source)
      {
        if(0 < tmp257++) { tmp256.Append(", "); }
        tmp256.Append("Source: ");
        Source.ToString(tmp256);
      }
      tmp256.Append(')');
      return tmp256.ToString();
    }
  }

}
