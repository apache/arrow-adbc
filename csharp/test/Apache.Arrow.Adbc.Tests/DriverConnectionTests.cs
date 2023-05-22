using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Abstract class for the ADBC connection tests.
    /// </summary>
    public abstract class DriverConnectionTests
    {
        /// <summary>
        /// Ensures the driver can connect.
        /// </summary>
        public abstract void CanDriverConnect();

        /// <summary>
        /// Ensures the driver can perform an update.
        /// </summary>
        public abstract void CanDriverUpdate();

        /// <summary>
        /// Ensures the driver can read the Arrow Schema.
        /// </summary>
        public abstract void CanReadSchema();

        /// <summary>
        /// Ensures an error generates a derivative of AdbcException.
        /// </summary>
        public abstract void VerifyBadQueryGeneratesError();

        /// <summary>
        /// Ensures the types and values are returned from the driver.
        /// </summary>
        public abstract void VerifyTypesAndValues();
    }
}
