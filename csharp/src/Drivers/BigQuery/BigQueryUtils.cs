using System;
using System.Collections.Generic;
using System.Text;
using Google;

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    internal class BigQueryUtils
    {
        public static bool TokenRequiresUpdate(Exception ex)
        {
            bool result = false;

            switch (ex)
            {
                case GoogleApiException apiException:
                    if (apiException.HttpStatusCode == System.Net.HttpStatusCode.Unauthorized)
                    {
                        result = true;
                    }
                    break;
                // TODO: others?
                default:
                    return false;
            }

            return result;
        }
    }
}
