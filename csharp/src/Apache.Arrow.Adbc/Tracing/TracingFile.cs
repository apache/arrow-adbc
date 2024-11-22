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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Tracing
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

        internal TracingFile(string fileBaseName, string? traceDirectoryPath = default, long maxFileSizeKb = FileExporter.MaxFileSizeKbDefault) :
            this(fileBaseName, traceDirectoryPath == null ? new DirectoryInfo(s_defaultTracePath) : new DirectoryInfo(traceDirectoryPath), maxFileSizeKb)
        { }

        internal TracingFile(string fileBaseName, DirectoryInfo traceDirectory, long maxFileSizeKb)
        {
            if (string.IsNullOrWhiteSpace(fileBaseName)) throw new ArgumentNullException(nameof(fileBaseName));
            _fileBaseName = fileBaseName;
            _tracingDirectory = traceDirectory;
            _maxFileSizeKb = maxFileSizeKb;
            EnsureTraceDirectory();
        }

        /// <summary>
        /// Append text to a last modified trace file. If the last modified trace file exceeds the
        /// individual file size limit, a new trace file is created.
        /// </summary>
        /// <param name="text">The text to write to the trace file.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        internal async Task WriteLine(string text, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested) return;

            if (_currentTraceFileInfo == null)
            {
                string searchPattern = _fileBaseName + "-trace-*.log";
                IOrderedEnumerable<FileInfo>? traceFileInfos = GetTracingFiles(_tracingDirectory, searchPattern);
                FileInfo? mostRecentFile = traceFileInfos?.FirstOrDefault();

                // Use the latest file, if it is not maxxed-out, or start a new tracing file.
                _currentTraceFileInfo = mostRecentFile != null && mostRecentFile.Length < _maxFileSizeKb * 1024
                    ? mostRecentFile
                    : new FileInfo(NewFileName());
            }
            else
            {
                _currentTraceFileInfo.Refresh();
                if (_currentTraceFileInfo.Length >= _maxFileSizeKb * 1024)
                {
                    // If tracing file is maxxed-out, start a new tracing file.
                    _currentTraceFileInfo = new FileInfo(NewFileName());
                }
            }

            // Write out to the file and retry if IO errors occur.
            await ActionWithRetry<IOException>(() =>
            {
                using (StreamWriter sw = _currentTraceFileInfo.AppendText())
                {
                    sw.WriteLine(text);
                }
            }, cancellationToken: cancellationToken);
        }

        internal static IOrderedEnumerable<FileInfo> GetTracingFiles(DirectoryInfo tracingDirectory, string searchPattern)
        {
            return tracingDirectory
                .EnumerateFiles(searchPattern, SearchOption.TopDirectoryOnly)
                .OrderByDescending(f => f.LastWriteTimeUtc);
        }

        internal static async Task ActionWithRetry<T>(Action action, int maxRetries = 5, CancellationToken cancellationToken = default) where T : Exception
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
                        // TODO: Handle if not complete after retries
                        throw;
                    }
                    try
                    {
                        await Task.Delay(pauseTime, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        // Need to catch this exception or it will be propogated.
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
            if (!_disposedValue)
            {
                if (disposing)
                {
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
