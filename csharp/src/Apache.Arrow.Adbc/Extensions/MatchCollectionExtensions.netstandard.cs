#if NETSTANDARD
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace System.Text.RegularExpressions
{
    public static class MatchCollectionExtensions
    {
        public static IEnumerable<Match> Where(this MatchCollection matchCollection, Func<Match,bool> predicate)
        {
            List<Match> matches = new List<Match>();

            foreach (Match match in matchCollection) 
            { 
                matches.Add(match);
            }
            
            return matches.Where(predicate);
        }
    }
}
#endif