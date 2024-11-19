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
using OpenTelemetry.Trace;
using System.IO;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System.ComponentModel.Design;

namespace Apache.Arrow.Adbc.Tracing
{
    internal class FileExporter : BaseExporter<Activity>
    {
        private bool _disposed = false;
        internal const long MaxFileSizeKbDefault = 1024;
        internal const int MaxTraceFilesDefault = 999;
        internal const string ApacheArrowAdbcNamespace = "Apache.Arrow.Adbc";
        private const string TracesFolderName = "Traces";
        private static readonly string s_tracingLocationDefault = TracingLocationDefault;

        private readonly ConcurrentQueue<Activity> _activityQueue = new();
        private readonly TracingFile _tracingFile;
        private readonly string _fileBaseName;
        private readonly string _tracesDirectoryFullName;

        private static readonly ConcurrentDictionary<string, FileExporterInstance> s_listeners = new();
        private static readonly object s_lock = new();

        private static string GetListenerId(string sourceName, string traceFolderLocation) => $"{sourceName}{traceFolderLocation}";

        internal static bool TryCreate(out FileExporter? fileExporter, FileExporterOptions options)
        {
            return TryCreate(out fileExporter, options.FileBaseName ?? ApacheArrowAdbcNamespace, options.TraceLocation, options.MaxTraceFileSizeKb, options.MaxTraceFiles);
        }

        internal static bool TryCreate(out FileExporter? fileExporter, string fileBaseName = ApacheArrowAdbcNamespace, string? traceLocation = default, long maxTraceFileSizeKb = FileExporter.MaxFileSizeKbDefault, int maxTraceFiles = FileExporter.MaxTraceFilesDefault)
        {
            DirectoryInfo tracesDirectory = new(traceLocation ?? s_tracingLocationDefault);
            string tracesDirectoryFullName = tracesDirectory.FullName;
            if (!Directory.Exists(tracesDirectoryFullName))
            {
                // TODO: Handle exceptions
                Directory.CreateDirectory(tracesDirectoryFullName);
            }

            lock (s_lock)
            {
                string listenerId = GetListenerId(fileBaseName, tracesDirectoryFullName);
                if (!s_listeners.ContainsKey(listenerId))
                {
                    CancellationTokenSource cancellationTokenSource = new();
                    FileExporterInstance instance = s_listeners.GetOrAdd(listenerId, (_) =>
                        new FileExporterInstance(
                            new FileExporter(fileBaseName, tracesDirectory, maxTraceFileSizeKb),
                            CleanupTraceDirectory(tracesDirectory, fileBaseName, maxTraceFiles, cancellationTokenSource.Token),
                            cancellationTokenSource
                        ));
                    fileExporter = instance.FileExporter;
                    return true;
                }
                fileExporter = null;
                return false;
            }
        }

        private static async Task CleanupTraceDirectory(DirectoryInfo traceDirectory, string fileBaseName, int maxTraceFiles, CancellationToken cancellationToken)
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
                        for (int i = tracingFiles.Length - 1; i > maxTraceFiles; i--)
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
                catch (TaskCanceledException)
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
        }

        public override ExportResult Export(in Batch<Activity> batch)
        {
            foreach (Activity activity in batch)
            {
                _activityQueue.Enqueue(activity);
                // Intentionally avoid await.
                DequeueAndWrite()
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
                    Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                    ApacheArrowAdbcNamespace,
                    TracesFolderName)
                ).FullName;

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                lock (s_lock)
                {
                    string listenerId = GetListenerId(_fileBaseName, _tracesDirectoryFullName);
                    _ = s_listeners.TryRemove(listenerId, out FileExporterInstance? listener);
                    listener?.Dispose();
                }
                _disposed = true;
            }
            base.Dispose(disposing);
        }

        internal class FileExporterInstance(FileExporter fileExporter, Task folderCleanupTask, CancellationTokenSource cancellationTokenSource)
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
