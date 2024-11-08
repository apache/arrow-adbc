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
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Apache.Thrift;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal abstract class HiveServer2Connection : AdbcConnection
    {
        internal const long BatchSizeDefault = 50000;
        internal const int PollTimeMillisecondsDefault = 500;
        internal const string ProductVersionDefault = "1.0.0";

        private TTransport? _transport;
        private TCLIService.Client? _client;
        private readonly Lazy<string> _vendorVersion;
        private readonly Lazy<string> _vendorName;

        private static readonly string s_activitySourceName = Assembly.GetExecutingAssembly().GetName().Name!;
        private static readonly string s_assemblyVersion = GetProductVersion();
        private static readonly ConcurrentDictionary<string, ActivityListener> s_listeners = new();
        private readonly ActivityListener? _listener;
        private readonly ConcurrentQueue<Activity> _activityQueue = new();

        internal HiveServer2Connection(IReadOnlyDictionary<string, string> properties)
        {
            Properties = properties;
            // TODO: Only create a source/listener when tracing is requested
            // Key of listeners collection should be ouput file location
            if (true)
            {
                // TODO: Determine the best handling of listener lifetimes.
                _listener = s_listeners.GetOrAdd(s_activitySourceName, (_) => new()
                {
                    ShouldListenTo = (source) => source.Name == s_activitySourceName,
                    Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded,
                    ActivityStarted = OnActivityStarted,
                    ActivityStopped = OnActivityStopped,
                    SampleUsingParentId = (ref ActivityCreationOptions<string> options) => ActivitySamplingResult.AllDataAndRecorded,
                });
                ActivitySource.AddActivityListener(_listener);
                ActivitySource = new(s_activitySourceName, s_assemblyVersion);
            }

            // Note: "LazyThreadSafetyMode.PublicationOnly" is thread-safe initialization where
            // the first successful thread sets the value. If an exception is thrown, initialization
            // will retry until it successfully returns a value without an exception.
            // https://learn.microsoft.com/en-us/dotnet/framework/performance/lazy-initialization#exceptions-in-lazy-objects
            _vendorVersion = new Lazy<string>(() => GetInfoTypeStringValue(TGetInfoType.CLI_DBMS_VER), LazyThreadSafetyMode.PublicationOnly);
            _vendorName = new Lazy<string>(() => GetInfoTypeStringValue(TGetInfoType.CLI_DBMS_NAME), LazyThreadSafetyMode.PublicationOnly);
        }

        internal TCLIService.Client Client
        {
            get { return _client ?? throw new InvalidOperationException("connection not open"); }
        }

        internal string VendorVersion => _vendorVersion.Value;

        internal string VendorName => _vendorName.Value;

        internal IReadOnlyDictionary<string, string> Properties { get; }

        protected ActivitySource? ActivitySource { get; }

        internal async Task OpenAsync()
        {
            using var activity = StartActivity(nameof(OpenAsync));
            TTransport transport = await CreateTransportAsync();
            TProtocol protocol = await CreateProtocolAsync(transport);
            _transport = protocol.Transport;
            _client = new TCLIService.Client(protocol);
            TOpenSessionReq request = CreateSessionRequest();

            activity?.AddEvent(new ActivityEvent("start open session"));
            TOpenSessionResp? session = await Client.OpenSession(request);
            activity?.AddEvent(new ActivityEvent("end open session"));

            // Some responses don't raise an exception. Explicitly check the status.
            if (session == null)
            {
                throw new HiveServer2Exception("unable to open session. unknown error.");
            }
            else if (session.Status.StatusCode != TStatusCode.SUCCESS_STATUS)
            {
                throw new HiveServer2Exception(session.Status.ErrorMessage)
                    .SetNativeError(session.Status.ErrorCode)
                    .SetSqlState(session.Status.SqlState);
            }

            SessionHandle = session.SessionHandle;
        }

        private static void TraceException<T>(Task<T> task, Activity? activity) where T : class
        {
            activity?.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection(
                [
                    new("message", task.Exception?.InnerException?.Message),
                    new("source", task.Exception?.InnerException?.Source),
                    new("stackTrace", task.Exception?.InnerException?.StackTrace),
                ])));
        }

        internal TSessionHandle? SessionHandle { get; private set; }

        protected internal DataTypeConversion DataTypeConversion { get; set; } = DataTypeConversion.None;

        protected internal HiveServer2TlsOption TlsOptions { get; set; } = HiveServer2TlsOption.Empty;

        protected internal int HttpRequestTimeout { get; set; } = 30000;

        protected abstract Task<TTransport> CreateTransportAsync();

        protected abstract Task<TProtocol> CreateProtocolAsync(TTransport transport);

        protected abstract TOpenSessionReq CreateSessionRequest();

        internal abstract SchemaParser SchemaParser { get; }

        internal abstract IArrowArrayStream NewReader<T>(T statement, Schema schema) where T : HiveServer2Statement;

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            throw new NotImplementedException();
        }

        public override IArrowArrayStream GetTableTypes()
        {
            throw new NotImplementedException();
        }

        internal static async Task PollForResponseAsync(TOperationHandle operationHandle, TCLIService.IAsync client, int pollTimeMilliseconds, ActivitySource? activitySource)
        {
            using Activity? activity = StartActivity(activitySource, nameof(PollForResponseAsync));
            TGetOperationStatusResp? statusResponse = null;
            do
            {
                if (statusResponse != null) { await Task.Delay(pollTimeMilliseconds); }
                TGetOperationStatusReq request = new(operationHandle);
                activity?.AddEvent(new ActivityEvent("calling GetOperationStatus"));
                statusResponse = await client.GetOperationStatus(request);
                activity?
                    .AddEvent(
                        new ActivityEvent(
                            "completed call GetOperationStatus",
                            tags: new ActivityTagsCollection([new("statusResponse.OperationState", statusResponse.OperationState)])));
            } while (statusResponse.OperationState == TOperationState.PENDING_STATE || statusResponse.OperationState == TOperationState.RUNNING_STATE);
            activity?.AddEvent(new ActivityEvent("completed"));
        }

        private string GetInfoTypeStringValue(TGetInfoType infoType)
        {
            using var activity = StartActivity(nameof(GetInfoTypeStringValue));
            TGetInfoReq req = new()
            {
                SessionHandle = SessionHandle ?? throw new InvalidOperationException("session not created"),
                InfoType = infoType,
            };

            TGetInfoResp getInfoResp = Client.GetInfo(req).Result;
            if (getInfoResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
            {
                throw new HiveServer2Exception(getInfoResp.Status.ErrorMessage)
                    .SetNativeError(getInfoResp.Status.ErrorCode)
                    .SetSqlState(getInfoResp.Status.SqlState);
            }

            return getInfoResp.InfoValue.StringValue;
        }

        public override void Dispose()
        {
            if (_client != null)
            {
                TCloseSessionReq r6 = new TCloseSessionReq(SessionHandle);
                _client.CloseSession(r6).Wait();

                _transport?.Close();
                _client.Dispose();
                _transport = null;
                _client = null;
            }
            // TODO: Determine best approach to lifetime of ActivityListener
            //_listener?.Dispose();
            ActivitySource?.Dispose();
        }

        internal static async Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TOperationHandle operationHandle, TCLIService.IAsync client, CancellationToken cancellationToken = default, ActivitySource? activitySource = null)
        {
            TGetResultSetMetadataReq request = new(operationHandle);
            TGetResultSetMetadataResp response = await client.GetResultSetMetadata(request, cancellationToken);
            return response;
        }

        private void OnActivityStarted(Activity activity)
        {
            _activityQueue.Enqueue(activity);
            // Intentionally avoid await.
            DequeueAndWrite("started")
                .ContinueWith(t => Console.WriteLine(t.Exception), TaskContinuationOptions.OnlyOnFaulted);
        }

        private void OnActivityStopped(Activity activity)
        {
            _activityQueue.Enqueue(activity);
            // Intentionally avoid await.
            DequeueAndWrite("stopped")
                .ContinueWith(t => Console.WriteLine(t.Exception), TaskContinuationOptions.OnlyOnFaulted);
        }

        private Task DequeueAndWrite(string state)
        {
            if (_activityQueue.TryDequeue(out Activity? activity))
            {
                if (activity != null)
                {
                    try
                    {
                        string json = JsonSerializer.Serialize(new { State = state, Activity = activity });
                        Console.WriteLine(json);
                    }
                    catch (NotSupportedException ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }

            return Task.CompletedTask;
        }

        private Activity? StartActivity(string methodName) => StartActivity(ActivitySource, methodName);

        private static Activity? StartActivity(ActivitySource? activitySource, string methodName) => activitySource?.StartActivity(typeof(HiveServer2Connection).FullName + "." + methodName);


        protected internal static string GetProductVersion()
        {
            FileVersionInfo fileVersionInfo = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);
            return fileVersionInfo.ProductVersion ?? ProductVersionDefault;
        }
    }
}
