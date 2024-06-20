using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc
{
    public abstract class MockServerBase<T>(T proxy) where T : class
    {
        internal Func<Task<T>>? NewConnectedServerAsync { get; set; }

        internal T Proxy { get; } = proxy;
    }

    public interface IProxyDatabase<T> where T : class
    {
        ProxyConnection<T> Connect(IReadOnlyDictionary<string, string>? properties, MockServerBase<T>? proxy);
    }

    public abstract class ProxyConnection<T> : AdbcConnection where T : class
    {
        internal ProxyConnection(MockServerBase<T>? proxy) : base()
        {
            Proxy = proxy?.Proxy;
            if (proxy != null)
            {
                proxy.NewConnectedServerAsync = NewConnectedServerAsync;
            }
        }

        protected T? Proxy { get; }

        internal abstract Task<T> NewConnectedServerAsync();
    }
}
