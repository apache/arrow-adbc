using System.IO;
using Thrift;
using Thrift.Transport.Client;

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    class ThriftSocketTransport : TSocketTransport, IPeekableTransport
    {
        public ThriftSocketTransport(string host, int port, TConfiguration config, int timeout = 0)
            : base(host, port, config, timeout)
        {
        }

        public Stream Input { get { return this.InputStream; } }
        public Stream Output { get { return this.InputStream; } }
    }
}
