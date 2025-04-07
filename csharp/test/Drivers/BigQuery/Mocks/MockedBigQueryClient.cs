using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Apis.Bigquery.v2.Data;
using Google.Cloud.BigQuery.V2;
using Moq;

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery.Mocks
{
    internal class MockedBigQueryClient
    {
        private Mock<BigQueryClient> mockedClient = new Mock<BigQueryClient>();

        public MockedBigQueryClient()
        {
            ConfigureClient();
        }

        public BigQueryClient Client => this.mockedClient.Object;

        void ConfigureClient()
        {
            mockedClient.Setup(c => c.CreateQueryJob(It.IsAny<string>(), It.IsAny<IEnumerable<BigQueryParameter>>(), It.IsAny<QueryOptions?>())).Returns(GetMockedBigQueryJob());

            mockedClient.Setup(c => c.GetQueryResults(It.IsAny<JobReference>(), It.IsAny<GetQueryResultsOptions>())).Returns(GetMockedBigQueryResults());
        }

        BigQueryJob GetMockedBigQueryJob()
        {
            Job job = new Job();
            job.Id = Guid.NewGuid().ToString();
            job.JobReference = new JobReference() { JobId = job.Id, ProjectId = "MockedProjectId" };
            job.Configuration = new JobConfiguration();
            job.Configuration.Query = new JobConfigurationQuery(){ Query = "This is a simulation", DestinationTable = GetMockedTableReference() };
            BigQueryJob mockedJob = new BigQueryJob(this.Client, job);
            return mockedJob;
        }

        BigQueryResults GetMockedBigQueryResults()
        {
            GetQueryResultsResponse response = new GetQueryResultsResponse();
            response.JobReference = GetMockedBigQueryJob().Reference;
            GetQueryResultsOptions options = new GetQueryResultsOptions();
            BigQueryResults expectedResults = new BigQueryResults(this.Client, response, GetMockedTableReference(), options);
            return expectedResults;
        }

        TableReference GetMockedTableReference()
        {
            TableReference tableReference = new TableReference()
            {
                DatasetId = "MockedDatasetId",
                ProjectId = "MockedProjectId",
                TableId = "MockedTableId"
            };

            return tableReference;
        }
    }
}
