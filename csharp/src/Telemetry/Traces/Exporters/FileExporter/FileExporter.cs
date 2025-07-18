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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using OpenTelemetry;

namespace Apache.Arrow.Adbc.Telemetry.Traces.Exporters.FileExporter
{
    internal class FileExporter : BaseExporter<Activity>
    {
        internal const long MaxFileSizeKbDefault = 1024;
        internal const int MaxTraceFilesDefault = 999;
        internal const string ApacheArrowAdbcNamespace = "Apache.Arrow.Adbc";
        private const string TracesFolderName = "Traces";

        private static readonly string s_tracingLocationDefault = TracingLocationDefault;
        private static readonly ConcurrentDictionary<string, Lazy<FileExporterInstance>> s_fileExporters = new();
        private static readonly byte[] s_newLine = Encoding.UTF8.GetBytes(Environment.NewLine);

        private readonly TracingFile _tracingFile;
        private readonly string _fileBaseName;
        private readonly string _tracesDirectoryFullName;
        private readonly ConcurrentQueue<Activity> _activityQueue = new();
        private readonly CancellationTokenSource _cancellationTokenSource;

        private bool _disposed = false;

        internal static bool TryCreate(FileExporterOptions options, out FileExporter? fileExporter)
        {
            return TryCreate(
                options.FileBaseName ?? ApacheArrowAdbcNamespace,
                options.TraceLocation ?? TracingLocationDefault,
                options.MaxTraceFileSizeKb,
                options.MaxTraceFiles,
                out fileExporter);
        }

        internal static bool TryCreate(
            string fileBaseName,
            string traceLocation,
            long maxTraceFileSizeKb,
            int maxTraceFiles,
            out FileExporter? fileExporter)
        {
            ValidateParameters(fileBaseName, traceLocation, maxTraceFileSizeKb, maxTraceFiles);

            DirectoryInfo tracesDirectory = new(traceLocation ?? s_tracingLocationDefault);
            string tracesDirectoryFullName = tracesDirectory.FullName;

            // In case we don't need to create this object, we'll lazy load the object only if added to the collection.
            var exporterInstance = new Lazy<FileExporterInstance>(() =>
            {
                CancellationTokenSource cancellationTokenSource = new();
                FileExporter fileExporter = new(fileBaseName, tracesDirectory, maxTraceFileSizeKb, maxTraceFiles);
                return new FileExporterInstance(
                    fileExporter,
                    // This listens/polls for activity in the queue and writes them to file
                    Task.Run(async () => await ProcessActivitiesAsync(fileExporter, cancellationTokenSource.Token)),
                    cancellationTokenSource);
            });

            // We only want one exporter listening on a source in a particular folder.
            // If two or more exporters are running, it'll create duplicate trace entries.
            // On Dispose, ensure to stop and remove the only instance, in case we need a new one later.
            string listenerId = GetListenerId(fileBaseName, tracesDirectoryFullName);
            bool isAdded = s_fileExporters.TryAdd(listenerId, exporterInstance);
            if (isAdded)
            {
                // This instance was added so load the object now.
                fileExporter = exporterInstance.Value.FileExporter;
                return true;
            }

            // There is already an exporter listening on the source/location
            fileExporter = null;
            return false;
        }

        internal static void ValidateParameters(string fileBaseName, string traceLocation, long maxTraceFileSizeKb, int maxTraceFiles)
        {
            if (string.IsNullOrWhiteSpace(fileBaseName))
                throw new ArgumentNullException(nameof(fileBaseName));
            if (fileBaseName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                throw new ArgumentException("Invalid or unsupported file name", nameof(fileBaseName));
            if (string.IsNullOrWhiteSpace(traceLocation) || traceLocation.IndexOfAny(Path.GetInvalidPathChars()) >= 0)
                throw new ArgumentException("Invalid or unsupported folder name", nameof(traceLocation));
            if (maxTraceFileSizeKb < 1)
                throw new ArgumentException("maxTraceFileSizeKb must be greater than zero", nameof(maxTraceFileSizeKb));
            if (maxTraceFiles < 1)
                throw new ArgumentException("maxTraceFiles must be greater than zero.", nameof(maxTraceFiles));

            IsDirectoryWritable(traceLocation, throwIfFails: true);
        }

        private static string GetListenerId(string sourceName, string traceFolderLocation) => $"{sourceName}{traceFolderLocation}";

        public override ExportResult Export(in Batch<Activity> batch)
        {
            foreach (Activity activity in batch)
            {
                if (activity == null) continue;
                _activityQueue.Enqueue(activity);
            }
            return ExportResult.Success;
        }

        private static async Task ProcessActivitiesAsync(FileExporter fileExporter, CancellationToken cancellationToken)
        {
            TimeSpan delay = TimeSpan.FromMilliseconds(100);
            // Polls for and then writes any activities in the queue
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(delay, cancellationToken);
                await fileExporter._tracingFile.WriteLinesAsync(GetActivitiesAsync(fileExporter._activityQueue), cancellationToken);
            }
        }

        private static bool IsDirectoryWritable(string traceLocation, bool throwIfFails = false)
        {
            try
            {
                if (!Directory.Exists(traceLocation))
                {
                    Directory.CreateDirectory(traceLocation);
                }
                string tempFilePath = Path.Combine(traceLocation, Path.GetRandomFileName());
                using FileStream fs = File.Create(tempFilePath, 1, FileOptions.DeleteOnClose);
                return true;
            }
            catch when (!throwIfFails)
            {
                return false;
            }
        }

        private static async IAsyncEnumerable<Stream> GetActivitiesAsync(ConcurrentQueue<Activity> activityQueue)
        {
            MemoryStream stream = new MemoryStream();
            while (activityQueue.TryDequeue(out Activity? activity))
            {
                stream.SetLength(0);
                SerializableActivity serializableActivity = new(activity);
                await JsonSerializer.SerializeAsync(stream, serializableActivity);
                stream.Write(s_newLine, 0, s_newLine.Length);
                stream.Position = 0;

                yield return stream;
            }
        }

        private FileExporter(string fileBaseName, DirectoryInfo tracesDirectory, long maxTraceFileSizeKb, int maxTraceFiles)
        {
            string fullName = tracesDirectory.FullName;
            _fileBaseName = fileBaseName;
            _tracesDirectoryFullName = fullName;
            _tracingFile = new(fileBaseName, fullName, maxTraceFileSizeKb, maxTraceFiles);
            _cancellationTokenSource = new CancellationTokenSource();
        }

        internal static string TracingLocationDefault =>
            new DirectoryInfo(
                Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    ApacheArrowAdbcNamespace,
                    TracesFolderName)
                ).FullName;

        private async Task FlushAsync(CancellationToken cancellationToken = default)
        {
            // Ensure existing writes are completed.
            while (!cancellationToken.IsCancellationRequested && !_activityQueue.IsEmpty)
            {
                await Task.Delay(100);
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (!_disposed && disposing)
            {
                // Allow flush of any existing events.
                using CancellationTokenSource flushTimeout = new();
                flushTimeout.CancelAfter(TimeSpan.FromSeconds(5));
                FlushAsync(flushTimeout.Token).Wait();

                // Remove and dispose of single instance of exporter
                string listenerId = GetListenerId(_fileBaseName, _tracesDirectoryFullName);
                bool isRemoved = s_fileExporters.TryRemove(listenerId, out Lazy<FileExporterInstance>? listener);
                if (isRemoved && listener != null && listener.IsValueCreated)
                {
                    listener?.Value.Dispose();
                }
                _cancellationTokenSource.Cancel();
                _cancellationTokenSource.Dispose();
                _disposed = true;
            }
            base.Dispose(disposing);
        }

        private class FileExporterInstance(
            FileExporter fileExporter,
            Task writeActivitiesTask,
            CancellationTokenSource cancellationTokenSource)
            : IDisposable
        {
            private bool _disposedValue;

            internal FileExporter FileExporter { get; } = fileExporter;

            internal CancellationTokenSource CancellationTokenSource { get; } = cancellationTokenSource;

            internal Task WriteActivitiesTask { get; } = writeActivitiesTask;

            protected virtual void Dispose(bool disposing)
            {
                if (!_disposedValue && disposing)
                {
                    CancellationTokenSource.Cancel();
                    try
                    {
                        WriteActivitiesTask.Wait();
                    }
                    catch
                    {
                        // Ignore
                    }
                    WriteActivitiesTask.Dispose();
                    CancellationTokenSource.Dispose();
                    _disposedValue = true;
                }
            }

            public void Dispose()
            {
                // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
                Dispose(disposing: true);
                GC.SuppressFinalize(this);
            }
        }
    }
}
