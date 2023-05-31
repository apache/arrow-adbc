using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Provides a mechanism to easily run tests against an array stream
    /// </summary>
    public class MockArrayStream : IArrowArrayStream
    {
        private readonly List<RecordBatch> recordBatches;
        private readonly Schema schema;

        private int calls = -1;

        /// <summary>
        /// Initializes the TestArrayStream
        /// </summary>
        /// <param name="schema">The Arrow schema</param>
        /// <param name="recordBatches">A list of record batches</param>
        public MockArrayStream(Schema schema, List<RecordBatch> recordBatches)
        {
            this.schema = schema;
            this.recordBatches = recordBatches;
        }

        public Schema Schema => this.schema;

        public void Dispose() {}

        /// <summary>
        /// Moves through the list of record batches
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            calls++;

            if (calls >= this.recordBatches.Count)
                return new ValueTask<RecordBatch>();
            else
                return new ValueTask<RecordBatch>(this.recordBatches[calls]);
        }
    }
}
