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
using System.Security.Authentication.ExtendedProtection;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc.Tracing
{
    internal class TracingFile : IDisposable
    {
        private const int MaxFileSize = 1024 * 1024;
        private static readonly string s_defaultTracePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "Apache.Arrow.Adbc", "Tracing");
        private readonly string _fileBaseName;
        private readonly DirectoryInfo _traceDirectory;
        private FileInfo? _currentTraceFileInfo;
        private CancellationTokenSource _cancellationTokenSource = new();
        private bool _disposedValue;

        public TracingFile(string fileBaseName) : this(fileBaseName, new DirectoryInfo(s_defaultTracePath)) { }

        public TracingFile(string fileBaseName, string traceDirectoryPath) : this(fileBaseName, new DirectoryInfo(traceDirectoryPath)) { }

        public TracingFile(string fileBaseName, DirectoryInfo traceDirectory)
        {
            _fileBaseName = fileBaseName;
            _traceDirectory = traceDirectory;
            EnsureTraceDirectory();
        }

        public async Task WriteLine(string text)
        {
            IOrderedEnumerable<FileInfo>? traceFileInfos;
            if (_currentTraceFileInfo == null)
            {
                string searchPattern = _fileBaseName + "-trace-*.log";
                traceFileInfos = _traceDirectory
                    .EnumerateFiles(searchPattern, SearchOption.TopDirectoryOnly)
                    .OrderByDescending(f => f.LastWriteTimeUtc);
                FileInfo? mostRecentFile = traceFileInfos.FirstOrDefault();
                _currentTraceFileInfo = mostRecentFile != null && mostRecentFile.Length < MaxFileSize
                    ? mostRecentFile
                    : new FileInfo(NewFileName());
            }
            else if (_currentTraceFileInfo.Length >= MaxFileSize)
            {
                _currentTraceFileInfo = new FileInfo(NewFileName());
            }
            // Write out to the file and retry if IO errors occur.
            await ActionWithRetry<IOException>(() =>
            {
                using (StreamWriter sw = _currentTraceFileInfo.AppendText())
                {
                    sw.WriteLine(text);
                }
            });
        }

        private static async Task ActionWithRetry<T>(Action action, int maxRetries = 3) where T : Exception
        {
            int retryCount = 0;
            TimeSpan pauseTime = TimeSpan.FromMilliseconds(10);
            bool completed = false;

            while (!completed && retryCount < maxRetries)
            {
                try
                {
                    action.Invoke();
                    completed = true;
                }
                catch (T)
                {
                    retryCount++;
                    await Task.Delay(pauseTime);
                }
            }
        }

        private string NewFileName()
        {
            string dateTimeSortable = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss-fff");
            return Path.Combine(_traceDirectory.FullName, $"{_fileBaseName}-trace-{dateTimeSortable}.log");
        }

        private void EnsureTraceDirectory()
        {
            if (!Directory.Exists(_traceDirectory.FullName))
            {
                Directory.CreateDirectory(_traceDirectory.FullName);
            }
        }

        //private static async Task CleanupTraceDirectory(FileInfo traceDirectory, TimeSpan CancellationToken cancellationToken)
        //{
        //    while (!cancellationToken.IsCancellationRequested)
        //    {

        //    }
        //    return;
        //}

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _cancellationTokenSource.Cancel();
                    _cancellationTokenSource.Dispose();
                    // TODO: dispose managed state (managed objects)
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                _disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~TracingFile()
        // {
        //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
