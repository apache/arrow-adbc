using System.IO;

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    public interface IPeekableTransport
    {
        Stream Input { get; }
        Stream Output { get; }
    }
}
