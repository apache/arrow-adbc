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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Telemetry.Traces.Exporters.FileExporter
{
    /// <summary>
    /// Provides access to writing trace files, limiting the
    /// individual files size and ensuring unique file names.
    /// </summary>
    internal class TracingFile : IDisposable
    {
        private static readonly string s_defaultTracePath = FileExporter.TracingLocationDefault;
        private readonly string _fileBaseName;
        private readonly DirectoryInfo _tracingDirectory;
        private FileInfo? _currentTraceFileInfo;
        private bool _disposedValue;
        private readonly long _maxFileSizeKb = FileExporter.MaxFileSizeKbDefault;
        private readonly int _maxTraceFiles = FileExporter.MaxTraceFilesDefault;

        internal TracingFile(string fileBaseName, string? traceDirectoryPath = default, long maxFileSizeKb = FileExporter.MaxFileSizeKbDefault, int maxTraceFiles = FileExporter.MaxTraceFilesDefault) :
            this(fileBaseName, traceDirectoryPath == null ? new DirectoryInfo(s_defaultTracePath) : new DirectoryInfo(traceDirectoryPath), maxFileSizeKb, maxTraceFiles)
        { }

        internal TracingFile(string fileBaseName, DirectoryInfo traceDirectory, long maxFileSizeKb, int maxTraceFiles)
        {
            if (string.IsNullOrWhiteSpace(fileBaseName)) throw new ArgumentNullException(nameof(fileBaseName));
            _fileBaseName = fileBaseName;
            _tracingDirectory = traceDirectory;
            _maxFileSizeKb = maxFileSizeKb;
            _maxTraceFiles = maxTraceFiles;
            EnsureTraceDirectory();
        }

        /// <summary>
        /// Writes lines of trace where each stream is a line in the trace file.
        /// </summary>
        /// <param name="streams">The enumerable of trace lines.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        internal async Task WriteLinesAsync(IAsyncEnumerable<Stream> streams, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested) return;

            string searchPattern = _fileBaseName + "-trace-*.log";
            if (_currentTraceFileInfo == null)
            {
                IOrderedEnumerable<FileInfo>? traceFileInfos = GetTracingFiles(_tracingDirectory, searchPattern);
                FileInfo? mostRecentFile = traceFileInfos?.FirstOrDefault();
                mostRecentFile?.Refresh();

                // Use the latest file, if it is not maxxed-out, or start a new tracing file.
                _currentTraceFileInfo = mostRecentFile != null && mostRecentFile.Length < _maxFileSizeKb * 1024
                    ? mostRecentFile
                    : new FileInfo(NewFileName());
            }

            // Write out to the file and retry if IO errors occur.
            await ActionWithRetryAsync<IOException>(async () => await WriteLinesAsync(streams), cancellationToken: cancellationToken);

            // Check if we need to remove old files
            if (_tracingDirectory.Exists)
            {
                FileInfo[] tracingFiles = [.. GetTracingFiles(_tracingDirectory, searchPattern)];
                if (tracingFiles != null && tracingFiles.Length > _maxTraceFiles)
                {
                    for (int i = tracingFiles.Length - 1; i >= _maxTraceFiles; i--)
                    {
                        FileInfo? file = tracingFiles.ElementAtOrDefault(i);
                        // Note: don't pass the cancellation tokenm, as we want this to ALWAYS run at the end.
                        await ActionWithRetryAsync<IOException>(() => file?.Delete());
                    }
                }
            }
        }

        private async Task WriteLinesAsync(IAsyncEnumerable<Stream> streams)
        {
            bool hasMoreData;
            do
            {
                bool newFileRequired = false;
                _currentTraceFileInfo!.Refresh();
                using (FileStream fileStream = _currentTraceFileInfo!.OpenWrite())
                {
                    fileStream.Position = fileStream.Length;
                    hasMoreData = false;
                    await foreach (Stream stream in streams)
                    {
                        if (fileStream.Length >= _maxFileSizeKb * 1024)
                        {
                            hasMoreData = true;
                            newFileRequired = true;
                            break;
                        }

                        await stream.CopyToAsync(fileStream);
                    }
                }
                if (newFileRequired)
                {
                    // If tracing file is maxxed-out, start a new tracing file.
                    _currentTraceFileInfo = new FileInfo(NewFileName());
                }
            } while (hasMoreData);
        }

        private static IOrderedEnumerable<FileInfo> GetTracingFiles(DirectoryInfo tracingDirectory, string searchPattern)
        {
            return tracingDirectory
                .EnumerateFiles(searchPattern, SearchOption.TopDirectoryOnly)
                .OrderByDescending(f => f.LastWriteTimeUtc);
        }

        private static async Task ActionWithRetryAsync<T>(Action action, int maxRetries = 5, CancellationToken cancellationToken = default) where T : Exception
        {
            int retryCount = 0;
            TimeSpan pauseTime = TimeSpan.FromMilliseconds(10);
            bool completed = false;

            while (!cancellationToken.IsCancellationRequested && !completed && retryCount < maxRetries)
            {
                try
                {
                    action.Invoke();
                    completed = true;
                }
                catch (T)
                {
                    retryCount++;
                    if (retryCount >= maxRetries)
                    {
                        throw;
                    }
                    try
                    {
                        await Task.Delay(pauseTime, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        // Need to catch this exception or it will be propagated.
                        break;
                    }
                }
            }
        }

        private static async Task ActionWithRetryAsync<T>(
            Func<Task> action,
            int maxRetries = 5,
            CancellationToken cancellationToken = default) where T : Exception
        {
            int retryCount = 0;
            TimeSpan pauseTime = TimeSpan.FromMilliseconds(10);
            bool completed = false;

            while (!cancellationToken.IsCancellationRequested && !completed && retryCount < maxRetries)
            {
                try
                {
                    await action.Invoke();
                    completed = true;
                }
                catch (T)
                {
                    retryCount++;
                    if (retryCount >= maxRetries)
                    {
                        throw;
                    }
                    try
                    {
                        await Task.Delay(pauseTime, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        // Need to catch this exception or it will be propagated.
                        break;
                    }
                }
            }
        }

        private string NewFileName()
        {
            string dateTimeSortable = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss-fff");
            return Path.Combine(_tracingDirectory.FullName, $"{_fileBaseName}-trace-{dateTimeSortable}.log");
        }

        private void EnsureTraceDirectory()
        {
            if (!Directory.Exists(_tracingDirectory.FullName))
            {
                Directory.CreateDirectory(_tracingDirectory.FullName);
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue && disposing)
            {
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
