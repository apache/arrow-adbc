namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry
{
    public class Util
    {
        private static string DRIVER_VERSION = "1.0.0";

        private static string DRIVER_NAME = "oss-adbc-driver";
        
        public static string GetDriverVersion()
        {
            return DRIVER_VERSION;
        }

        public static string GetDriverName()
        {
            return DRIVER_NAME;
        }
    }
}