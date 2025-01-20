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
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading;
using OpenTelemetry;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace Apache.Arrow.Adbc.Tracing
{
    internal class FileExporter : BaseExporter<Activity>
    {
        internal const long MaxFileSizeKbDefault = 1024;
        internal const int MaxTraceFilesDefault = 999;
        internal const string ApacheArrowAdbcNamespace = "Apache.Arrow.Adbc";
        private const string TracesFolderName = "Traces";

        private static readonly string s_tracingLocationDefault = TracingLocationDefault;
        private static readonly ConcurrentDictionary<string, Lazy<FileExporterInstance>> s_fileExporters = new();

        private readonly TracingFile _tracingFile;
        private readonly string _fileBaseName;
        private readonly string _tracesDirectoryFullName;
        private readonly ConcurrentQueue<Activity> _activityQueue = new();
        private readonly CancellationTokenSource _cancellationTokenSource;

        private bool _disposed = false;

        private static string GetListenerId(string sourceName, string traceFolderLocation) => $"{sourceName}{traceFolderLocation}";

        internal static bool TryCreate(out FileExporter? fileExporter, FileExporterOptions options)
        {
            return TryCreate(
                out fileExporter,
                options.FileBaseName ?? ApacheArrowAdbcNamespace,
                options.TraceLocation ?? TracingLocationDefault,
                options.MaxTraceFileSizeKb,
                options.MaxTraceFiles);
        }

        internal static bool TryCreate(
            out FileExporter? fileExporter,
            string fileBaseName,
            string traceLocation,
            long maxTraceFileSizeKb,
            int maxTraceFiles)
        {
            ValidParameters(fileBaseName, traceLocation, maxTraceFileSizeKb, maxTraceFiles);

            DirectoryInfo tracesDirectory = new(traceLocation ?? s_tracingLocationDefault);
            string tracesDirectoryFullName = tracesDirectory.FullName;

            // In case we don't need to create this object, we'll lazy load the object only if added to the collection.
            var exporterInstance = new Lazy<FileExporterInstance>(() =>
            {
                CancellationTokenSource cancellationTokenSource = new();
                FileExporter fileExporter = new(fileBaseName, tracesDirectory, maxTraceFileSizeKb, maxTraceFiles);
                return new FileExporterInstance(
                    fileExporter,
                    Task.Run(async () => await WriteActivities(fileExporter, cancellationTokenSource.Token)),
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

        internal static void ValidParameters(string fileBaseName, string traceLocation, long maxTraceFileSizeKb, int maxTraceFiles)
        {
            if (string.IsNullOrWhiteSpace(fileBaseName))
                throw new ArgumentNullException(nameof(fileBaseName));
            if (fileBaseName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                throw new ArgumentException("Invalid or unsupported file name", nameof(fileBaseName));
            if ((string.IsNullOrWhiteSpace(traceLocation) || (traceLocation.IndexOfAny(Path.GetInvalidPathChars()) >= 0)))
                throw new ArgumentException("Invalid or unsupported folder name", nameof(traceLocation));
            if (maxTraceFileSizeKb < 1)
                throw new ArgumentException("maxTraceFileSizeKb must be greater than zero", nameof(maxTraceFileSizeKb));
            if (maxTraceFiles < 1)
                throw new ArgumentException("maxTraceFiles must be greater than zero.", nameof(maxTraceFiles));

            IsDirectoryWritable(traceLocation, throwIfFails: true);
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
            catch
            {
                if (throwIfFails)
                    throw;
                else
                    return false;
            }
        }

        private static async Task WriteActivities(FileExporter fileExporter, CancellationToken cancellationToken)
        {
            TimeSpan delay = TimeSpan.FromMilliseconds(10);
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(delay, cancellationToken);
                await fileExporter._tracingFile.WriteLines(GetActivitiesAsync(fileExporter._activityQueue), cancellationToken);
            }
        }

        private static readonly byte[] s_newLine = Encoding.UTF8.GetBytes(Environment.NewLine);

        private static async IAsyncEnumerable<Stream> GetActivitiesAsync(ConcurrentQueue<Activity> activityQueue)
        {
            MemoryStream stream = new MemoryStream();
            while (activityQueue.TryDequeue(out Activity? activity))
            {
                stream.SetLength(0);
                SerializableActivity serilalizableActivity = new(activity);
                await JsonSerializer.SerializeAsync(stream, serilalizableActivity);
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

        public override ExportResult Export(in Batch<Activity> batch)
        {
            foreach (Activity activity in batch)
            {
                if (activity == null) continue;
                // Intentionally don't await the result of the call
                //_ = WriteActivity(activity, _cancellationTokenSource.Token);
                //Task.Run(async () => await WriteActivity(activity, _tracingFile, _cancellationTokenSource.Token));
                //s_taskQueue.Enqueue(task);
                _activityQueue.Enqueue(activity);
            }
            return ExportResult.Success;
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
            if (!_disposed)
            {
                if (disposing)
                {
                    // Allow flush of any existing events.
                    using CancellationTokenSource flushTimeoutTS = new();
                    flushTimeoutTS.CancelAfter(TimeSpan.FromSeconds(5));
                    FlushAsync(flushTimeoutTS.Token).Wait();

                    // Remove and dispose of single instance of exporter
                    string listenerId = GetListenerId(_fileBaseName, _tracesDirectoryFullName);
                    bool isRemoved = s_fileExporters.TryRemove(listenerId, out Lazy<FileExporterInstance>? listener);
                    if (isRemoved && listener != null && listener.IsValueCreated)
                    {
                        listener?.Value.Dispose();
                    }
                    _cancellationTokenSource.Cancel();
                    _cancellationTokenSource.Dispose();
                }
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
                if (!_disposedValue)
                {
                    if (disposing)
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
                    }
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
