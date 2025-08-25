/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Telemetry.Traces.Exporters.FileExporter;

namespace Apache.Arrow.Adbc.Tests.Telemetry.Traces.Exporters.FileExporter
{
    public class TracingFileTests
    {
        private static readonly string s_localApplicationDataFolderPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        private int _testInstance = 0;

        /// <summary>
        /// This test performs concurrent writes to the same trace file(s).
        /// This isn't the likely intended use case, as the FileExporter will
        /// instance by the combined key of folder and file base name. So there is
        /// little to no change of concurrent writes to the same file.
        /// </summary>
        /// <returns></returns>
        [Fact]
        internal async Task TestMultipleConcurrentTracingFiles()
        {
            CancellationTokenSource tokenSource = new CancellationTokenSource();
            int concurrentCount = 5;
            Task[] tasks = new Task[concurrentCount];
            int[] lineCounts = new int[concurrentCount];
            string sourceName = ExportersBuilderTests.NewName();
            string customFolderName = ExportersBuilderTests.NewName();
            string traceFolder = Path.Combine(s_localApplicationDataFolderPath, customFolderName);
            if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            try
            {
                for (int i = 0; i < concurrentCount; i++)
                {
                    tasks[i] = Task.Run(async () => await Run(sourceName, traceFolder, tokenSource.Token));
                }
                await Task.WhenAll(tasks);

                foreach (var file in Directory.GetFiles(traceFolder))
                {
                    foreach (string line in File.ReadLines(file))
                    {
                        Assert.StartsWith("line", line);
                        Assert.True(int.TryParse(line.Substring(4), out int writerNumber));
                        Assert.InRange(writerNumber, 0, concurrentCount - 1);
                        lineCounts[writerNumber]++;
                    }
                }
                for (int i = 0; i < concurrentCount; i++)
                {
                    Assert.True(100 == lineCounts[i], $"index {i} != 100. Actual {lineCounts[i]}");
                }
            }
            finally
            {
                if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            }
        }

        private async Task Run(string sourceName, string traceFolder, CancellationToken cancellationToken)
        {
            int instanceNumber = Interlocked.Increment(ref _testInstance) - 1;
            TracingFile tracingFile = new TracingFile(sourceName, traceFolder);
            await tracingFile.WriteLinesAsync(GetLinesAsync(instanceNumber, 100, cancellationToken), cancellationToken);
        }

        private static async IAsyncEnumerable<Stream> GetLinesAsync(int instanceNumber, int lineCount, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            for (int i = 0; i < lineCount; i++)
            {
                if (cancellationToken.IsCancellationRequested) yield break;
                yield return new MemoryStream(System.Text.Encoding.UTF8.GetBytes($"line{instanceNumber}" + Environment.NewLine));
                await Task.Delay(10); // Simulate some delay
            }
        }
    }
}
