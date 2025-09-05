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
using System.Diagnostics;
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
        private const int KbInByes = 1024;
        private static readonly string s_defaultTracePath = FileExporter.TracingLocationDefault;
        private static readonly Random s_globalRandom = new();
        private static readonly ThreadLocal<Random> s_threadLocalRandom = new(NewRandom);
        private static readonly Lazy<string> s_processId = new(() => Process.GetCurrentProcess().Id.ToString(), isThreadSafe: true);
        private readonly string _fileBaseName;
        private readonly DirectoryInfo _tracingDirectory;
        private FileInfo? _currentTraceFileInfo;
        private FileStream? _currentFileStream;
        private readonly long _maxFileSizeKb;
        private readonly int _maxTraceFiles;

        internal TracingFile(string fileBaseName, string? traceDirectoryPath = default, long maxFileSizeKb = FileExporter.MaxFileSizeKbDefault, int maxTraceFiles = FileExporter.MaxTraceFilesDefault) :
            this(fileBaseName, ResolveTraceDirectory(traceDirectoryPath), maxFileSizeKb, maxTraceFiles)
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
        internal async Task WriteLineAsync(Stream stream, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested) return;

            string findSearchPattern = _fileBaseName + $"-trace-*-{ProcessId}.log";
            if (_currentTraceFileInfo == null)
            {
                await OpenNewTracingFileAsync();
            }

            // Write out to the file and retry if IO errors occur.
            await ActionWithRetryAsync<IOException>(async () => await WriteSingleLineAsync(stream), cancellationToken: cancellationToken);

        }

        private async Task TryRemoveOlderFiles()
        {
            // Check if we need to remove old files
            if (_tracingDirectory.Exists)
            {
                // This will clean-up files for all processes in the same directory.
                string deleteSearchPattern = _fileBaseName + $"-trace-*.log";
                IOrderedEnumerable<FileInfo> orderedFiles = await GetTracingFilesAsync(_tracingDirectory, deleteSearchPattern);
                // Avoid accidentally trying to delete the current file.
                FileInfo[] tracingFiles = orderedFiles.Where(f => !f.FullName.Equals(_currentTraceFileInfo?.FullName))?.ToArray() ?? new FileInfo[0];
                if (tracingFiles.Length >= _maxTraceFiles)
                {
                    int lastIndex = Math.Max(0, _maxTraceFiles - 1);
                    for (int i = tracingFiles.Length - 1; i >= lastIndex; i--)
                    {
                        FileInfo? file = tracingFiles.ElementAtOrDefault(i);
                        // Note: don't pass the cancellation token, as we want this to ALWAYS run at the end.
                        await ActionWithRetryAsync<IOException>(() =>
                        {
                            file?.Delete();
                            return Task.CompletedTask;
                        });
                    }
                }
            }
        }

        private async Task WriteSingleLineAsync(Stream stream)
        {
            if ((_currentFileStream!.Length + stream.Length) >= (_maxFileSizeKb * KbInByes))
            {
                // If tracing file is maxxed-out, start a new tracing file.
                await OpenNewTracingFileAsync();
            }
            _currentFileStream!.Position = _currentFileStream.Length;
            await stream.CopyToAsync(_currentFileStream);
        }

        private async Task OpenNewTracingFileAsync()
        {
            _currentFileStream?.Dispose();
            _currentFileStream = null;

            do
            {
                _currentTraceFileInfo = new FileInfo(NewFileName());
                try
                {
                    if (!_tracingDirectory.Exists)
                    {
                        _tracingDirectory.Create();
                    }
                    _currentFileStream = _currentTraceFileInfo.OpenWrite();
                }
                catch (IOException ioEx) when ((uint)ioEx.HResult == 0x80070020) // ERROR_SHARING_VIOLATION
                {
                    // If we can't open the file, just set to null.
                    _currentFileStream = null;
                    int delayMs = ThreadLocalRandom.Next(5, 50);
                    await Task.Delay(delayMs).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    Debug.WriteLine(ex.Message);
                }
            } while (_currentFileStream == null);

            await TryRemoveOlderFiles();
        }

        private static async Task<IOrderedEnumerable<FileInfo>> GetTracingFilesAsync(DirectoryInfo tracingDirectory, string searchPattern)
        {
            return await Task.Run(() =>
            {
                if (!tracingDirectory.Exists)
                {
                    tracingDirectory.Create();
                    return Enumerable.Empty<FileInfo>().OrderByDescending(f => f.LastWriteTimeUtc);
                }
                return tracingDirectory
                    .EnumerateFiles(searchPattern, SearchOption.TopDirectoryOnly)
                    .OrderByDescending(f => f.LastWriteTimeUtc.Ticks);
            });
        }

        private static async Task ActionWithRetryAsync<T>(
            Func<Task> action,
            int maxRetries = 100,
            CancellationToken cancellationToken = default) where T : Exception
        {
            int retryCount = 0;
            int delayTime = ThreadLocalRandom.Next(50, 500); // Introduce a small random delay to avoid contention.
            TimeSpan pauseTime = TimeSpan.FromMilliseconds(delayTime);
            bool completed = false;

            while (!cancellationToken.IsCancellationRequested && !completed && retryCount < maxRetries)
            {
                try
                {
                    await action.Invoke();
                    completed = true;
                }
                catch (T) when (retryCount < (maxRetries - 1))
                {
                    retryCount++;
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
            return Path.Combine(_tracingDirectory.FullName, $"{_fileBaseName}-trace-{dateTimeSortable}-{ProcessId}.log");
        }

        private void EnsureTraceDirectory()
        {
            if (!Directory.Exists(_tracingDirectory.FullName))
            {
                Directory.CreateDirectory(_tracingDirectory.FullName);
            }
        }

        private static DirectoryInfo ResolveTraceDirectory(string? traceDirectoryPath) =>
            traceDirectoryPath == null ? new DirectoryInfo(s_defaultTracePath) : new DirectoryInfo(traceDirectoryPath);

        private static string ProcessId => s_processId.Value;

        private static Random ThreadLocalRandom => s_threadLocalRandom.Value!;

        private static Random NewRandom()
        {
            int seed;
            lock (s_globalRandom)
            {
                seed = s_globalRandom.Next();
            }
            // Create a new random instance based on the global random seed.
            // This ensures that each thread gets a different seed.
            return new Random(seed);
        }

        public void Dispose()
        {
            _currentFileStream?.Dispose();
            _currentFileStream = null;
            _currentTraceFileInfo = null;
        }
    }
}
