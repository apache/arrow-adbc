using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    public static class HiveServer2Parameters
    {
        public const string DataTypeConv = "adbc.hive2.data_type_conv";
    }

    public static class HiveServer2DataTypeConversionConstants
    {
        public const string None = "none";
        public const string Scalar = "scalar";
        public const string SupportedList = None;

        public static HiveServer2DataTypeConversion Parse(string? dataTypeConversion)
        {
            return (dataTypeConversion?.Trim().ToLowerInvariant()) switch
            {
                null or "" => HiveServer2DataTypeConversion.Empty,
                Scalar => HiveServer2DataTypeConversion.Scalar,
                None => HiveServer2DataTypeConversion.None,
                _ => HiveServer2DataTypeConversion.Invalid,
            };
        }
    }

    public enum HiveServer2DataTypeConversion
    {
        Invalid = 0,
        None,
        Scalar,
        Empty = int.MaxValue,
    }
}
