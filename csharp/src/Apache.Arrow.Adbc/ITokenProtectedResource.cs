using System;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc
{
    public interface ITokenProtectedResource
    {
        public Func<Task> UpdateToken { get; set; }
        public Func<Exception, bool> CheckIfTokenRequiresUpdate { get; set; }
    }
}
