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

        private bool _disposed = false;
        private readonly ConcurrentQueue<Activity> _activityQueue = new();
        private readonly TracingFile _tracingFile;
        private readonly string _fileBaseName;
        private readonly string _tracesDirectoryFullName;
        private readonly CancellationTokenSource _cancellationTokenSource;

        private static string GetListenerId(string sourceName, string traceFolderLocation) => $"{sourceName}{traceFolderLocation}";

        internal static bool TryCreate(out FileExporter? fileExporter, FileExporterOptions options)
        {
            return TryCreate(
                out fileExporter,
                options.FileBaseName ?? ApacheArrowAdbcNamespace,
                options.TraceLocation,
                options.MaxTraceFileSizeKb,
                options.MaxTraceFiles);
        }

        internal static bool TryCreate(
            out FileExporter? fileExporter,
            string fileBaseName = ApacheArrowAdbcNamespace,
            string? traceLocation = default,
            long maxTraceFileSizeKb = FileExporter.MaxFileSizeKbDefault,
            int maxTraceFiles = FileExporter.MaxTraceFilesDefault)
        {
            ValidParameters(fileBaseName, traceLocation, maxTraceFileSizeKb, maxTraceFiles);

            DirectoryInfo tracesDirectory = new(traceLocation ?? s_tracingLocationDefault);
            string tracesDirectoryFullName = tracesDirectory.FullName;
            if (!Directory.Exists(tracesDirectoryFullName))
            {
                // TODO: Handle exceptions
                Directory.CreateDirectory(tracesDirectoryFullName);
            }

            // In case we don't need to create this object, we'll lazy load the object only if added to the collection.
            var exporterInstance = new Lazy<FileExporterInstance>(() =>
            {
                CancellationTokenSource cancellationTokenSource = new();
                return new FileExporterInstance(
                    new FileExporter(fileBaseName, tracesDirectory, maxTraceFileSizeKb),
                    CleanupTraceDirectory(tracesDirectory, fileBaseName, maxTraceFiles, cancellationTokenSource.Token),
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

        internal static void ValidParameters(string fileBaseName, string? traceLocation, long maxTraceFileSizeKb, int maxTraceFiles)
        {
            if (string.IsNullOrWhiteSpace(fileBaseName))
                throw new ArgumentNullException(nameof(fileBaseName));
            if (fileBaseName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
                throw new ArgumentException("Invalid or unsupported file name", nameof(fileBaseName));
            if ((string.IsNullOrWhiteSpace(traceLocation) || (traceLocation?.IndexOfAny(Path.GetInvalidPathChars()) >= 0)))
                throw new ArgumentException("Invalid or unsupported folder name", nameof(traceLocation));
            if (maxTraceFileSizeKb < 1)
                throw new ArgumentException("maxTraceFileSizeKb must be greater than zero", nameof(maxTraceFileSizeKb));
            if (maxTraceFiles < 1)
                throw new ArgumentException("maxTraceFiles must be greater than zero.", nameof(maxTraceFiles));
        }

        /// <summary>
        /// Runs continuously to monitor the tracing folder to remove older trace files.
        /// </summary>
        /// <param name="traceDirectory">The tracing folder to monitor.</param>
        /// <param name="fileBaseName">The name of the files in the tracing folder to monitor.</param>
        /// <param name="maxTraceFiles">The maximun number of trace files allowed exist in the tracing folder.</param>
        /// <param name="cancellationToken">The cancellation token used to stop/cancel this task.</param>
        /// <returns></returns>
        private static async Task CleanupTraceDirectory(
            DirectoryInfo traceDirectory,
            string fileBaseName,
            int maxTraceFiles,
            CancellationToken cancellationToken)
        {
            const int delayTimeSeconds = 5;
            string searchPattern = fileBaseName + "-trace-*.log";
            while (!cancellationToken.IsCancellationRequested)
            {
                if (traceDirectory.Exists)
                {
                    FileInfo[] tracingFiles = [.. TracingFile.GetTracingFiles(traceDirectory, searchPattern)];
                    if (tracingFiles != null && tracingFiles.Length > maxTraceFiles)
                    {
                        for (int i = tracingFiles.Length - 1; i >= maxTraceFiles; i--)
                        {
                            FileInfo? file = tracingFiles.ElementAtOrDefault(i);
                            await TracingFile.ActionWithRetry<IOException>(() => file?.Delete(), cancellationToken: cancellationToken);
                        }
                    }
                }
                else
                {
                    // The folder may have temporarily been removed. Let's keep monitoring in case it returns.
                }
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(delayTimeSeconds), cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    // Need to catch this exception or it will be propogated.
                    break;
                }
            }
            return;
        }

        private FileExporter(string fileBaseName, DirectoryInfo tracesDirectory, long maxTraceFileSizeKb)
        {
            string fullName = tracesDirectory.FullName;
            _fileBaseName = fileBaseName;
            _tracesDirectoryFullName = fullName;
            _tracingFile = new(fileBaseName, fullName, maxTraceFileSizeKb);
            _cancellationTokenSource = new CancellationTokenSource();
        }

        public override ExportResult Export(in Batch<Activity> batch)
        {
            foreach (Activity activity in batch)
            {
                _activityQueue.Enqueue(activity);
                // Intentionally avoid await.
                DequeueAndWrite(_cancellationTokenSource.Token)
                    .ContinueWith(t => Console.WriteLine(t.Exception), TaskContinuationOptions.OnlyOnFaulted);
            }
            return ExportResult.Success;
        }

        private async Task DequeueAndWrite(CancellationToken cancellationToken = default)
        {
            if (_activityQueue.TryDequeue(out Activity? activity) && activity != null)
            {
                try
                {
                    string json = JsonSerializer.Serialize(activity);
                    await _tracingFile.WriteLine(json, cancellationToken);
                }
                catch (NotSupportedException ex)
                {
                    // TODO: Handle excption thrown by JsonSerializer
                    Console.WriteLine(ex.Message);
                }
            }

            return;
        }

        internal static string TracingLocationDefault =>
            new DirectoryInfo(
                Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    ApacheArrowAdbcNamespace,
                    TracesFolderName)
                ).FullName;

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
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

        private class FileExporterInstance(FileExporter fileExporter, Task folderCleanupTask, CancellationTokenSource cancellationTokenSource)
            : IDisposable
        {
            private bool _disposedValue;

            internal FileExporter FileExporter { get; } = fileExporter;

            internal Task FolderCleanupTask { get; } = folderCleanupTask;

            internal CancellationTokenSource CancellationTokenSource { get; } = cancellationTokenSource;

            protected virtual void Dispose(bool disposing)
            {
                if (!_disposedValue)
                {
                    if (disposing)
                    {
                        CancellationTokenSource.Cancel();
                        CancellationTokenSource.Dispose();
                        try
                        {
                            FolderCleanupTask.Wait();
                        }
                        catch
                        {
                            // Ignore
                        }
                        FolderCleanupTask.Dispose();
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
