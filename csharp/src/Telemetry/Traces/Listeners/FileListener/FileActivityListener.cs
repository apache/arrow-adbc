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
using System.IO;

namespace Apache.Arrow.Adbc.Telemetry.Traces.Listeners.FileListener
{
    public sealed class FileActivityListener : IDisposable
    {
        public const long MaxFileSizeKbDefault = 1024;
        public const int MaxTraceFilesDefault = 999;
        internal const string ApacheArrowAdbcNamespace = "Apache.Arrow.Adbc";
        private const string TracesFolderName = "Traces";

        private readonly ActivityListener _listener;
        private readonly TracingFile _tracingFile;
        private readonly ActivityProcessor _activityProcessor;

        public static bool TryActivateFileListener(
            string activitySourceName,
            string? exporterOption,
            out FileActivityListener? listener,
            Func<ActivitySource, bool>? shouldListenTo = default,
            string environmentName = ListenersOptions.Environment.Exporter,
            string? tracesLocation = null,
            long maxTraceFileSizeKb = MaxFileSizeKbDefault,
            int maxTraceFiles = MaxTraceFilesDefault)
        {
            listener = null;
            if (string.IsNullOrWhiteSpace(exporterOption))
            {
                // Fall back to check the environment variable
                exporterOption = Environment.GetEnvironmentVariable(environmentName);
            }
            if (string.IsNullOrWhiteSpace(exporterOption))
            {
                // Neither option or environment variable is set - no tracer provider will be activated.
                return false;
            }

            if (string.Equals(exporterOption, ListenersOptions.Exporters.AdbcFile, StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    listener = new FileActivityListener(
                        activitySourceName,
                        tracesLocation,
                        maxTraceFileSizeKb,
                        maxTraceFiles,
                        shouldListenTo);
                    return true;
                }
                catch (Exception ex)
                {
                    // Swallow any exceptions to avoid impacting application behavior
                    Trace.WriteLine(ex.Message);
                    listener = null;
                    return false;
                }
            }
            return false;
        }

        public FileActivityListener(string fileBaseName, string? tracesLocation = null, long maxTraceFileSizeKb = MaxFileSizeKbDefault, int maxTraceFiles = MaxTraceFilesDefault, Func<ActivitySource, bool>? shouldListenTo = default)
        {
            tracesLocation = string.IsNullOrWhiteSpace(tracesLocation) ? TracingLocationDefault : tracesLocation;
            ValidateParameters(fileBaseName, tracesLocation!, maxTraceFileSizeKb, maxTraceFiles);
            DirectoryInfo tracesDirectory = new(tracesLocation); // Ensured to be valid by ValidateParameters
            Func<ActivitySource, bool> shouldListenToAll = (source) => source.Name == fileBaseName;
            _listener = new ActivityListener()
            {
                ShouldListenTo = shouldListenTo ?? shouldListenToAll,
                Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
            };

            _tracingFile = new TracingFile(fileBaseName, tracesDirectory.FullName, maxTraceFileSizeKb, maxTraceFiles);
            _activityProcessor = new ActivityProcessor(_tracingFile.WriteLineAsync);
            _listener.ActivityStopped = OnActivityStopped;
            ActivitySource.AddActivityListener(_listener);

            _activityProcessor.TryStartAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public void Dispose()
        {
            _listener.Dispose();
            _activityProcessor.Dispose();
            _tracingFile.Dispose();
        }

        private void OnActivityStopped(Activity activity)
        {
            // Write activity to file or other storage
            _activityProcessor.TryWrite(activity);
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

        internal static string TracingLocationDefault { get; } =
            new DirectoryInfo(
                Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                    ApacheArrowAdbcNamespace,
                    TracesFolderName)).FullName;
    }
}
