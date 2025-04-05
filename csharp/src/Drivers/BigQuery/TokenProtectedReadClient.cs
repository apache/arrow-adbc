using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.Storage.V1;
using Google.Cloud.BigQuery.V2;

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    internal class TokenProtectedReadClientManger : ITokenProtectedResource
    {
        BigQueryReadClient bigQueryReadClient;

        public TokenProtectedReadClientManger(GoogleCredential credential)
        {
            UpdateCredential(credential);

            if (bigQueryReadClient == null)
            {
                throw new InvalidOperationException("could not create a read client");
            }
        }

        public BigQueryReadClient ReadClient => bigQueryReadClient;

        public void UpdateCredential(GoogleCredential? credential)
        {
            if (credential == null)
            {
                throw new ArgumentNullException(nameof(credential));
            }

            BigQueryReadClientBuilder readClientBuilder = new BigQueryReadClientBuilder();
            readClientBuilder.Credential = credential;
            this.bigQueryReadClient = readClientBuilder.Build();
        }

        public string? AccessToken { get; set; }
        public Func<Task>? UpdateToken { get; set; }

        public bool TokenRequiresUpdate(Exception ex) => BigQueryUtils.TokenRequiresUpdate(ex);
    }
}
