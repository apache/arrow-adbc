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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Tests the default implementations of the ADBC 1.1.0 and asynchronous
    /// members of the ADBC base classes.
    /// </summary>
    public class BaseClassDefaultsTests
    {
        [Fact]
        public async Task ExecuteQueryAsyncDefaultsToExecuteQuery()
        {
            var stmt = new SyncOnlyStatement();

            await stmt.ExecuteQueryAsync();
            await stmt.ExecuteQueryAsync(CancellationToken.None);
            await stmt.ExecuteUpdateAsync();
            await stmt.ExecuteUpdateAsync(CancellationToken.None);

            Assert.Equal(2, stmt.QueryCount);
            Assert.Equal(2, stmt.UpdateCount);
        }

        [Fact]
        public async Task TokenOverloadUsesLegacyAsyncOverride()
        {
            var stmt = new LegacyAsyncStatement();
            using var cts = new CancellationTokenSource();

            QueryResult result = await stmt.ExecuteQueryAsync(cts.Token);
            UpdateResult update = await stmt.ExecuteUpdateAsync(cts.Token);

            Assert.Equal(LegacyAsyncStatement.QueryRows, result.RowCount);
            Assert.Equal(LegacyAsyncStatement.UpdateRows, update.AffectedRows);
            Assert.Equal(0, stmt.SyncCount);
        }

        [Fact]
        public async Task LegacyOverrideCallingBaseAfterAwaitDoesNotRecurse()
        {
            var stmt = new LegacyCallsBaseStatement();

            Task<QueryResult> task = stmt.ExecuteQueryAsync(CancellationToken.None).AsTask();
            Assert.Same(task, await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(10))));
            QueryResult result = await task;

            Assert.Equal(1, stmt.OverrideCount);
            Assert.Equal(1, stmt.SyncCount);
            Assert.Equal(7, result.RowCount);
        }

        [Fact]
        public async Task TokenOverloadUsesNewAsyncOverride()
        {
            var stmt = new NewAsyncStatement();

            QueryResult result = await stmt.ExecuteQueryAsync(CancellationToken.None);
            UpdateResult update = await stmt.ExecuteUpdateAsync(CancellationToken.None);

            Assert.Equal(NewAsyncStatement.QueryRows, result.RowCount);
            Assert.Equal(NewAsyncStatement.UpdateRows, update.AffectedRows);
            Assert.Equal(0, stmt.SyncCount);
        }

        [Fact]
        public async Task ParameterlessOverloadDoesNotForwardToTheTokenOverload()
        {
            // Forwarding goes only from the token overload to the parameterless one, so a
            // driver that overrides only the token overload isn't reached this way.
            var stmt = new NewAsyncStatement();

            await stmt.ExecuteQueryAsync();
            await stmt.ExecuteUpdateAsync();

            Assert.Equal(2, stmt.SyncCount);
        }

        [Fact]
        public async Task RecommendedNewDriverIsReachedThroughBothOverloads()
        {
            var stmt = new RecommendedAsyncStatement();

            Assert.Equal(NewAsyncStatement.QueryRows, (await stmt.ExecuteQueryAsync()).RowCount);
            Assert.Equal(NewAsyncStatement.QueryRows, (await stmt.ExecuteQueryAsync(CancellationToken.None)).RowCount);
            Assert.Equal(NewAsyncStatement.UpdateRows, (await stmt.ExecuteUpdateAsync()).AffectedRows);
            Assert.Equal(NewAsyncStatement.UpdateRows, (await stmt.ExecuteUpdateAsync(CancellationToken.None)).AffectedRows);
            Assert.Equal(0, stmt.SyncCount);
        }

        [Fact]
        public async Task LegacyOverrideCallingBaseRunsOnceWhenCalledDirectly()
        {
            var stmt = new LegacyCallsBaseStatement();

            QueryResult result = await stmt.ExecuteQueryAsync();

            Assert.Equal(1, stmt.OverrideCount);
            Assert.Equal(1, stmt.SyncCount);
            Assert.Equal(7, result.RowCount);
        }

        [Fact]
        public async Task NewOverrideCallingParameterlessBaseDoesNotRecurse()
        {
            var stmt = new NewCallsParameterlessBaseStatement();

            Task<QueryResult> task = stmt.ExecuteQueryAsync(CancellationToken.None).AsTask();
            Assert.Same(task, await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(10))));

            Assert.Equal(1, stmt.OverrideCount);
            Assert.Equal(1, stmt.SyncCount);
        }

        [Fact]
        public async Task CancellationCallsCancelAndThrowsOperationCanceled()
        {
            var stmt = new BlockingStatement();
            using var cts = new CancellationTokenSource();

            ValueTask<QueryResult> pending = stmt.ExecuteQueryAsync(cts.Token);
            await WaitForStartAsync(stmt.Started.Task);
            cts.Cancel();

            OperationCanceledException ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);
            Assert.Equal(cts.Token, ex.CancellationToken);
            Assert.IsType<AdbcException>(ex.InnerException);
            Assert.True(stmt.CancelCalled);
        }

        [Fact]
        public async Task UpdateCancellationCallsCancelAndThrowsOperationCanceled()
        {
            var stmt = new BlockingStatement();
            using var cts = new CancellationTokenSource();

            Task<UpdateResult> pending = stmt.ExecuteUpdateAsync(cts.Token);
            await WaitForStartAsync(stmt.Started.Task);
            cts.Cancel();

            OperationCanceledException ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
            Assert.Equal(cts.Token, ex.CancellationToken);
            Assert.True(stmt.CancelCalled);
        }

        [Fact]
        public async Task CancellationReachesALegacyAsyncOverride()
        {
            var stmt = new LegacyBlockingStatement();
            using var cts = new CancellationTokenSource();

            // The token overload forwards to the legacy override and calls Cancel on cancellation
            ValueTask<QueryResult> pending = stmt.ExecuteQueryAsync(cts.Token);
            await WaitForStartAsync(stmt.Started.Task);
            cts.Cancel();

            OperationCanceledException ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);
            Assert.Equal(cts.Token, ex.CancellationToken);
            Assert.IsType<AdbcException>(ex.InnerException);
            Assert.True(stmt.CancelCalled);
        }

        [Fact]
        public async Task PreCancelledTokenDoesNotExecute()
        {
            var stmt = new SyncOnlyStatement();
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await stmt.ExecuteQueryAsync(cts.Token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stmt.ExecuteSchemaAsync(cts.Token));

            Assert.Equal(0, stmt.QueryCount);
        }

        [Fact]
        public async Task ConnectionAsyncCancellationCallsConnectionCancel()
        {
            var conn = new BlockingConnection();
            using var cts = new CancellationTokenSource();

            Task<IArrowArrayStream> pending = conn.GetTableTypesAsync(cts.Token);
            await WaitForStartAsync(conn.Started.Task);
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
            Assert.True(conn.CancelCalled);
        }

        [Fact]
        public async Task CancellationIsBestEffortWhenCancelIsNotImplemented()
        {
            // SyncOnlyStatement doesn't override Cancel, so the operation runs to completion
            var stmt = new SyncOnlyStatement { Delay = TimeSpan.FromMilliseconds(200) };
            using var cts = new CancellationTokenSource();

            ValueTask<QueryResult> pending = stmt.ExecuteQueryAsync(cts.Token);
            cts.Cancel();
            QueryResult result = await pending;

            Assert.Equal(1, stmt.QueryCount);
            Assert.Equal(0, result.RowCount);
        }

        [Fact]
        public async Task CancellationDoesNotHideErrorsFromDriversThatCannotCancel()
        {
            // FailingStatement doesn't override Cancel, so the default throws NotImplemented.
            // The failure is the driver's own and must not be reported as cancellation.
            var stmt = new FailingStatement();
            using var cts = new CancellationTokenSource();

            ValueTask<QueryResult> pending = stmt.ExecuteQueryAsync(cts.Token);
            await WaitForStartAsync(stmt.Started.Task);
            cts.Cancel();
            stmt.Fail.Set();

            AdbcException ex = await Assert.ThrowsAsync<AdbcException>(async () => await pending);
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
        }

        [Fact]
        public void DoubleOptionsRoundTripThroughStrings()
        {
            // "R" formatting doesn't round-trip this value on .NET Framework
            const double value = 0.84551240822557006;
            var stmt = new SyncOnlyStatement();

            stmt.SetOptionDouble("d", value);
            Assert.Equal(value, stmt.GetOptionDouble("d"));
        }

        [Fact]
        public void TypedOptionsDefaultToStringOptions()
        {
            var stmt = new SyncOnlyStatement();

            stmt.SetOptionInt64("i", -12);
            stmt.SetOptionDouble("d", 0.1);

            Assert.Equal("-12", stmt.Options["i"]);
            Assert.Equal("0.1", stmt.Options["d"]);
            Assert.Equal(-12L, stmt.GetOptionInt64("i"));
            Assert.Equal(0.1, stmt.GetOptionDouble("d"));

            stmt.SetOption("s", "not a number");
            Assert.Equal(AdbcStatusCode.InvalidArgument, Assert.Throws<AdbcException>(() => stmt.GetOptionInt64("s")).Status);
            Assert.Equal(AdbcStatusCode.NotImplemented, Assert.Throws<AdbcException>(() => stmt.SetOptionBytes("b", new byte[1])).Status);
            Assert.Equal(AdbcStatusCode.NotFound, Assert.Throws<AdbcException>(() => stmt.GetOptionBytes("b")).Status);
        }

        [Fact]
        public void ConnectionGetOptionAnswersStandardOptions()
        {
            var conn = new BlockingConnection();

            Assert.Equal(AdbcOptions.Enabled, conn.GetOption(AdbcOptions.Connection.Autocommit));
            Assert.Equal(AdbcOptions.Disabled, conn.GetOption(AdbcOptions.Connection.ReadOnly));
            Assert.Equal(AdbcOptions.IsolationLevels.Default, conn.GetOption(AdbcOptions.Connection.IsolationLevel));
            Assert.Equal(AdbcStatusCode.NotFound, Assert.Throws<AdbcException>(() => conn.GetOption(AdbcOptions.Connection.CurrentCatalog)).Status);
            Assert.Equal(AdbcStatusCode.NotFound, Assert.Throws<AdbcException>(() => conn.GetOption("unknown")).Status);
        }

        [Fact]
        public async Task ConnectionOptionAsyncDefaultsToSyncOptions()
        {
            var conn = new BlockingConnection();

            await conn.SetOptionAsync("key", "value");
            Assert.Equal("value", conn.Options["key"]);

            Assert.Equal(AdbcOptions.Enabled, await conn.GetOptionAsync(AdbcOptions.Connection.Autocommit));
            AdbcException e = await Assert.ThrowsAsync<AdbcException>(() => conn.GetOptionAsync("unknown"));
            Assert.Equal(AdbcStatusCode.NotFound, e.Status);
        }

        [Fact]
        public async Task ConnectionOptionAsyncHonorsAPreCancelledToken()
        {
            var conn = new BlockingConnection();
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => conn.SetOptionAsync("key", "value", cts.Token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => conn.GetOptionAsync(AdbcOptions.Connection.Autocommit, cts.Token));
            Assert.Empty(conn.Options);
        }

        [Fact]
        public async Task ConnectionSetOptionAsyncCancellationCallsConnectionCancel()
        {
            var conn = new BlockingConnection();
            using var cts = new CancellationTokenSource();

            Task pending = conn.SetOptionAsync(BlockingConnection.BlockingKey, "value", cts.Token);
            await WaitForStartAsync(conn.Started.Task);
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
            Assert.True(conn.CancelCalled);
        }

        [Fact]
        public void CurrentCatalogSetterDefaultsToSetOption()
        {
            var conn = new BlockingConnection();

            conn.CurrentCatalog = "cat";
            conn.CurrentDbSchema = "schema";

            Assert.Equal("cat", conn.Options[AdbcOptions.Connection.CurrentCatalog]);
            Assert.Equal("schema", conn.Options[AdbcOptions.Connection.CurrentDbSchema]);
        }

        [Fact]
        public void ConnectWithTypedOptionsDefaultsToConnect()
        {
            var db = new ConnectingDatabase();

            using AdbcConnection conn = db.ConnectWithTypedOptions(new Dictionary<string, object>
            {
                { "s", "text" },
                { "b", false },
                { "l", 4L },
                { "d", 2.5 },
                { "bytes", new byte[] { 1, 2 } },
                { "memory", new ReadOnlyMemory<byte>(new byte[] { 3 }) },
            });

            Assert.Equal(new Dictionary<string, string> { { "s", "text" }, { "b", "false" }, { "l", "4" }, { "d", "2.5" } }, db.LastStringOptions);
            var binary = ((BlockingConnection)conn).BinaryOptions;
            Assert.Equal(new byte[] { 1, 2 }, binary["bytes"]);
            Assert.Equal(new byte[] { 3 }, binary["memory"]);

            using AdbcConnection noOptions = db.ConnectWithTypedOptions(null);
            Assert.Null(db.LastStringOptions);
        }

        [Fact]
        public async Task ConnectWithTypedOptionsAsyncDefaultsToConnectWithTypedOptions()
        {
            var db = new ConnectingDatabase();

            using AdbcConnection conn = await db.ConnectWithTypedOptionsAsync(new Dictionary<string, object>
            {
                { "l", 4L },
                { "bytes", new byte[] { 1 } },
            });

            Assert.Equal(new Dictionary<string, string> { { "l", "4" } }, db.LastStringOptions);
            Assert.Equal(new byte[] { 1 }, ((BlockingConnection)conn).BinaryOptions["bytes"]);

            using var cts = new CancellationTokenSource();
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => db.ConnectWithTypedOptionsAsync(null, cts.Token));
        }

        [Fact]
        public void ConnectWithTypedOptionsDisposesTheConnectionIfABinaryOptionFails()
        {
            var db = new ConnectingDatabase();

            Assert.Throws<AdbcException>(() => db.ConnectWithTypedOptions(new Dictionary<string, object>
            {
                { BlockingConnection.RejectedKey, new byte[] { 1 } },
            }));
            Assert.True(db.LastConnection!.Disposed);
        }

        [Fact]
        public void DatabaseObjectSetOptionDispatchesByType()
        {
            var db = new RecordingDatabase();

            db.SetOption("s", (object)"text");
            db.SetOption("b", (object)true);
            db.SetOption("i", (object)3);
            db.SetOption("l", (object)4L);
            db.SetOption("f", (object)1.5f);
            db.SetOption("d", (object)2.5);
            db.SetOption("bytes", (object)new byte[] { 9 });

            Assert.Equal("text", db.Options["s"]);
            Assert.Equal("true", db.Options["b"]);
            Assert.Equal(3L, db.Options["i"]);
            Assert.Equal(4L, db.Options["l"]);
            Assert.Equal(1.5, db.Options["f"]);
            Assert.Equal(2.5, db.Options["d"]);
            Assert.Equal(new byte[] { 9 }, db.Options["bytes"]);

            Assert.Equal(AdbcStatusCode.InvalidArgument, Assert.Throws<AdbcException>(() => db.SetOption("x", (object)DateTime.Now)).Status);
        }

        [Fact]
        public void DriverOpenWithObjectsConvertsToStrings()
        {
            var driver = new StringOnlyDriver();

            driver.Open(new Dictionary<string, object> { { "n", 5 }, { "d", 0.5 }, { "s", "x" }, { "b", false } });

            Assert.Equal("5", driver.Parameters!["n"]);
            Assert.Equal("0.5", driver.Parameters["d"]);
            Assert.Equal("x", driver.Parameters["s"]);
            Assert.Equal("false", driver.Parameters["b"]);
        }

        [Fact]
        public async Task DisposeAsyncCallsDispose()
        {
            var stmt = new SyncOnlyStatement();
            await stmt.DisposeAsync();
            Assert.True(stmt.Disposed);
        }


        [Fact]
        public void DriverOpenWithObjectsAppliesBinaryValuesAfterOpening()
        {
            var driver = new StringOnlyDriver();

            AdbcDatabase db = driver.Open(new Dictionary<string, object> { { "s", "x" }, { "bytes", new byte[] { 9 } } });

            Assert.Equal("x", driver.Parameters!["s"]);
            Assert.False(driver.Parameters.ContainsKey("bytes"));
            Assert.Equal(new byte[] { 9 }, ((RecordingDatabase)db).Options["bytes"]);
        }

        [Fact]
        public void DriverOpenWithObjectsDisposesTheDatabaseIfABinaryValueFails()
        {
            var driver = new StringOnlyDriver();

            AdbcException ex = Assert.Throws<AdbcException>(() => driver.Open(new Dictionary<string, object>
            {
                { RecordingDatabase.RejectedKey, new byte[] { 1 } },
            }));
            Assert.Equal(AdbcStatusCode.InvalidArgument, ex.Status);
            Assert.True(driver.LastDatabase!.Disposed);
        }

        // Waits without blocking a thread-pool thread, which the operation under test needs
        private static async Task WaitForStartAsync(Task started)
        {
            Assert.Same(started, await Task.WhenAny(started, Task.Delay(TimeSpan.FromSeconds(10))));
        }

        private sealed class SyncOnlyStatement : AdbcStatement
        {
            public int QueryCount;
            public int UpdateCount;
            public bool Disposed;
            public TimeSpan Delay;
            public Dictionary<string, string> Options { get; } = new Dictionary<string, string>();

            public override void SetOption(string key, string value) => Options[key] = value;
            public override string GetOption(string key) => Options.TryGetValue(key, out string? v) ? v : base.GetOption(key);

            public override QueryResult ExecuteQuery()
            {
                Thread.Sleep(Delay);
                Interlocked.Increment(ref QueryCount);
                return new QueryResult(0, null!);
            }

            public override UpdateResult ExecuteUpdate()
            {
                Interlocked.Increment(ref UpdateCount);
                return new UpdateResult(0);
            }

            public override Schema ExecuteSchema() => throw new InvalidOperationException("should not be called");

            public override void Dispose() => Disposed = true;
        }

        private sealed class FailingStatement : AdbcStatement
        {
            public TaskCompletionSource<bool> Started { get; } = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            public ManualResetEventSlim Fail { get; } = new ManualResetEventSlim();

            public override QueryResult ExecuteQuery()
            {
                Started.TrySetResult(true);
                Fail.Wait(TimeSpan.FromSeconds(30));
                throw new AdbcException("bad query", AdbcStatusCode.InvalidArgument);
            }

            public override UpdateResult ExecuteUpdate() => throw new NotSupportedException();
        }

        /// <summary>
        /// A driver written before the CancellationToken overloads existed.
        /// </summary>
        private sealed class LegacyAsyncStatement : AdbcStatement
        {
            public const long QueryRows = 11;
            public const long UpdateRows = 12;
            public int SyncCount;

            public override async ValueTask<QueryResult> ExecuteQueryAsync()
            {
                await Task.Yield();
                return new QueryResult(QueryRows, null!);
            }

            public override async Task<UpdateResult> ExecuteUpdateAsync()
            {
                await Task.Yield();
                return new UpdateResult(UpdateRows);
            }

            public override QueryResult ExecuteQuery() { SyncCount++; return new QueryResult(0, null!); }
            public override UpdateResult ExecuteUpdate() { SyncCount++; return new UpdateResult(0); }
        }

        /// <summary>
        /// A legacy override that does asynchronous work before delegating to the base class.
        /// </summary>
        private sealed class LegacyCallsBaseStatement : AdbcStatement
        {
            public int OverrideCount;
            public int SyncCount;

            public override async ValueTask<QueryResult> ExecuteQueryAsync()
            {
                Interlocked.Increment(ref OverrideCount);
                await Task.Delay(1);
                return await base.ExecuteQueryAsync();
            }

            public override QueryResult ExecuteQuery()
            {
                Interlocked.Increment(ref SyncCount);
                return new QueryResult(7, null!);
            }

            public override UpdateResult ExecuteUpdate() => new UpdateResult(0);
        }

        /// <summary>
        /// A driver written against the CancellationToken overloads.
        /// </summary>
        private sealed class NewAsyncStatement : AdbcStatement
        {
            public const long QueryRows = 21;
            public const long UpdateRows = 22;
            public int SyncCount;

            public override async ValueTask<QueryResult> ExecuteQueryAsync(CancellationToken cancellationToken)
            {
                await Task.Yield();
                return new QueryResult(QueryRows, null!);
            }

            public override async Task<UpdateResult> ExecuteUpdateAsync(CancellationToken cancellationToken)
            {
                await Task.Yield();
                return new UpdateResult(UpdateRows);
            }

            public override QueryResult ExecuteQuery() { SyncCount++; return new QueryResult(0, null!); }
            public override UpdateResult ExecuteUpdate() { SyncCount++; return new UpdateResult(0); }
        }

        /// <summary>
        /// A driver following the recommended pattern: it implements the token overloads
        /// and forwards the parameterless ones to them.
        /// </summary>
        private sealed class RecommendedAsyncStatement : AdbcStatement
        {
            public int SyncCount;

            public override ValueTask<QueryResult> ExecuteQueryAsync() => ExecuteQueryAsync(CancellationToken.None);

            public override async ValueTask<QueryResult> ExecuteQueryAsync(CancellationToken cancellationToken)
            {
                await Task.Yield();
                return new QueryResult(NewAsyncStatement.QueryRows, null!);
            }

            public override Task<UpdateResult> ExecuteUpdateAsync() => ExecuteUpdateAsync(CancellationToken.None);

            public override async Task<UpdateResult> ExecuteUpdateAsync(CancellationToken cancellationToken)
            {
                await Task.Yield();
                return new UpdateResult(NewAsyncStatement.UpdateRows);
            }

            public override QueryResult ExecuteQuery() { SyncCount++; return new QueryResult(0, null!); }
            public override UpdateResult ExecuteUpdate() { SyncCount++; return new UpdateResult(0); }
        }

        /// <summary>
        /// A token-overload override that falls back to the parameterless base implementation.
        /// </summary>
        private sealed class NewCallsParameterlessBaseStatement : AdbcStatement
        {
            public int OverrideCount;
            public int SyncCount;

            public override async ValueTask<QueryResult> ExecuteQueryAsync(CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref OverrideCount);
                return await base.ExecuteQueryAsync();
            }

            public override QueryResult ExecuteQuery()
            {
                Interlocked.Increment(ref SyncCount);
                return new QueryResult(0, null!);
            }

            public override UpdateResult ExecuteUpdate() => new UpdateResult(0);
        }

        private sealed class BlockingStatement : AdbcStatement
        {
            private readonly ManualResetEventSlim _cancel = new ManualResetEventSlim();
            public TaskCompletionSource<bool> Started { get; } = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            public bool CancelCalled => _cancel.IsSet;

            public override void Cancel() => _cancel.Set();

            public override QueryResult ExecuteQuery()
            {
                Started.TrySetResult(true);
                _cancel.Wait(TimeSpan.FromSeconds(30));
                throw new AdbcException("cancelled", AdbcStatusCode.Cancelled);
            }

            public override UpdateResult ExecuteUpdate()
            {
                Started.TrySetResult(true);
                _cancel.Wait(TimeSpan.FromSeconds(30));
                throw new AdbcException("cancelled", AdbcStatusCode.Cancelled);
            }
        }

        /// <summary>
        /// A driver written before the token overloads existed, with a genuinely
        /// asynchronous query that completes when Cancel is called.
        /// </summary>
        private sealed class LegacyBlockingStatement : AdbcStatement
        {
            private readonly TaskCompletionSource<bool> _cancel = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource<bool> Started { get; } = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            public bool CancelCalled => _cancel.Task.IsCompleted;

            public override void Cancel() => _cancel.TrySetResult(true);

            public override async ValueTask<QueryResult> ExecuteQueryAsync()
            {
                Started.TrySetResult(true);
                await _cancel.Task.ConfigureAwait(false);
                throw new AdbcException("cancelled", AdbcStatusCode.Cancelled);
            }

            public override QueryResult ExecuteQuery() => throw new NotSupportedException();
            public override UpdateResult ExecuteUpdate() => throw new NotSupportedException();
        }

        private sealed class BlockingConnection : AdbcConnection
        {
            private readonly ManualResetEventSlim _cancel = new ManualResetEventSlim();
            public TaskCompletionSource<bool> Started { get; } = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            public bool CancelCalled => _cancel.IsSet;
            public Dictionary<string, string> Options { get; } = new Dictionary<string, string>();

            public const string BlockingKey = "test.blocking";
            public const string RejectedKey = "test.rejected";

            public Dictionary<string, byte[]> BinaryOptions { get; } = new Dictionary<string, byte[]>();
            public bool Disposed { get; private set; }

            public override void SetOptionBytes(string key, ReadOnlySpan<byte> value)
            {
                if (key == RejectedKey)
                {
                    throw new AdbcException("rejected", AdbcStatusCode.InvalidArgument);
                }
                BinaryOptions[key] = value.ToArray();
            }

            public override void Dispose() => Disposed = true;

            public override void Cancel() => _cancel.Set();

            public override void SetOption(string key, string value)
            {
                if (key == BlockingKey)
                {
                    Block();
                }
                Options[key] = value;
            }

            public override IArrowArrayStream GetTableTypes()
            {
                Block();
                return null!;
            }

            private void Block()
            {
                Started.TrySetResult(true);
                _cancel.Wait(TimeSpan.FromSeconds(30));
                throw new AdbcException("cancelled", AdbcStatusCode.Cancelled);
            }

            public override AdbcStatement CreateStatement() => throw new NotImplementedException();

            public override IArrowArrayStream GetObjects(
                GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern,
                string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
                => throw new NotImplementedException();

            public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
                => throw new NotImplementedException();
        }

        private sealed class ConnectingDatabase : AdbcDatabase
        {
            public IReadOnlyDictionary<string, string>? LastStringOptions { get; private set; }
            public BlockingConnection? LastConnection { get; private set; }

            public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
            {
                LastStringOptions = options;
                return LastConnection = new BlockingConnection();
            }
        }

        private sealed class RecordingDatabase : AdbcDatabase
        {
            public const string RejectedKey = "test.rejected";

            public Dictionary<string, object> Options { get; } = new Dictionary<string, object>();
            public bool Disposed { get; private set; }

            public override void SetOption(string key, string value) => Options[key] = value;
            public override void SetOptionInt64(string key, long value) => Options[key] = value;
            public override void SetOptionDouble(string key, double value) => Options[key] = value;

            public override void SetOptionBytes(string key, ReadOnlySpan<byte> value)
            {
                if (key == RejectedKey)
                {
                    throw new AdbcException("rejected", AdbcStatusCode.InvalidArgument);
                }
                Options[key] = value.ToArray();
            }

            public override void Dispose() => Disposed = true;

            public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options) => throw new NotImplementedException();
        }

        private sealed class StringOnlyDriver : AdbcDriver
        {
            public IReadOnlyDictionary<string, string>? Parameters { get; private set; }
            public RecordingDatabase? LastDatabase { get; private set; }

            public override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
            {
                Parameters = parameters;
                return LastDatabase = new RecordingDatabase();
            }
        }
    }
}
