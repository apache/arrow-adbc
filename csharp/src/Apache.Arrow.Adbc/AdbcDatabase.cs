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

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Clients first initialize a database, then create a connection.
    /// This gives the implementation a place to initialize and own any
    /// common connection state.
    /// For example, in-memory databases can place ownership of the actual
    /// database in this object.
    /// </summary>
    public abstract class AdbcDatabase : IDisposable, IAsyncDisposable
    {
        /// <summary>
        /// Options are generally set before opening a database.  Some drivers may
        /// support setting options after opening as well.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        public virtual void SetOption(string key, string value)
        {
            throw AdbcException.NotImplemented("Database does not support setting options");
        }

        /// <summary>
        /// Options are generally set before opening a database.  Some drivers may
        /// support setting options after opening as well.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">
        /// Option value. Strings and booleans are passed to <see cref="SetOption(string, string)"/>,
        /// integers to <see cref="SetOptionInt64"/>, floating-point numbers to
        /// <see cref="SetOptionDouble"/> and byte arrays to <see cref="SetOptionBytes"/>.
        /// </param>
        public virtual void SetOption(string key, object value)
        {
            if (AdbcOptionValue.TryGetInt64(value, out long l))
            {
                SetOptionInt64(key, l);
            }
            else if (AdbcOptionValue.TryGetDouble(value, out double d))
            {
                SetOptionDouble(key, d);
            }
            else if (AdbcOptionValue.TryGetBytes(value, out ReadOnlyMemory<byte> bytes))
            {
                SetOptionBytes(key, bytes.Span);
            }
            else
            {
                // Strings and booleans; throws for anything else
                SetOption(key, AdbcOptionValue.ToOptionString(key, value));
            }
        }

        /// <summary>
        /// Sets an integer-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        /// <remarks>
        /// Added in ADBC 1.1.0. The default implementation converts the value to
        /// a string and calls <see cref="SetOption(string, string)"/>.
        /// </remarks>
        public virtual void SetOptionInt64(string key, long value)
        {
            SetOption(key, AdbcOptionValue.FormatInt64(value));
        }

        /// <summary>
        /// Sets a floating-point-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value</param>
        /// <remarks>
        /// Added in ADBC 1.1.0. The default implementation converts the value to
        /// a string and calls <see cref="SetOption(string, string)"/>.
        /// </remarks>
        public virtual void SetOptionDouble(string key, double value)
        {
            SetOption(key, AdbcOptionValue.FormatDouble(value));
        }

        /// <summary>
        /// Sets a binary-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <param name="value">Option value. Implementations must copy the data if they retain it.</param>
        /// <remarks>Added in ADBC 1.1.0.</remarks>
        public virtual void SetOptionBytes(string key, ReadOnlySpan<byte> value)
        {
            throw AdbcException.NotImplemented("Database does not support binary options");
        }

        /// <summary>
        /// Gets the value of a string-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>The option value.</returns>
        /// <exception cref="AdbcException">
        /// With <see cref="AdbcStatusCode.NotFound"/> if the option is not recognized or has no value.
        /// </exception>
        /// <remarks>Added in ADBC 1.1.0.</remarks>
        public virtual string GetOption(string key)
        {
            throw AdbcOptionValue.NotFound(key);
        }

        /// <summary>
        /// Gets the value of an integer-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>The option value.</returns>
        /// <remarks>
        /// Added in ADBC 1.1.0. The default implementation parses the result of
        /// <see cref="GetOption(string)"/>.
        /// </remarks>
        public virtual long GetOptionInt64(string key)
        {
            return AdbcOptionValue.ParseInt64(key, GetOption(key));
        }

        /// <summary>
        /// Gets the value of a floating-point-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>The option value.</returns>
        /// <remarks>
        /// Added in ADBC 1.1.0. The default implementation parses the result of
        /// <see cref="GetOption(string)"/>.
        /// </remarks>
        public virtual double GetOptionDouble(string key)
        {
            return AdbcOptionValue.ParseDouble(key, GetOption(key));
        }

        /// <summary>
        /// Gets the value of a binary-valued option.
        /// </summary>
        /// <param name="key">Option name</param>
        /// <returns>The option value.</returns>
        /// <remarks>Added in ADBC 1.1.0.</remarks>
        public virtual byte[] GetOptionBytes(string key)
        {
            throw AdbcOptionValue.NotFound(key);
        }

        /// <summary>
        /// Create a new connection to the database.
        /// </summary>
        /// <param name="options">
        /// Additional options to use when connecting.
        /// </param>
        public abstract AdbcConnection Connect(IReadOnlyDictionary<string, string>? options);

        /// <summary>
        /// Create a new connection to the database using options that aren't all strings.
        /// </summary>
        /// <param name="options">
        /// Additional options to use when connecting. Values may be strings, booleans,
        /// integers, floating-point numbers, byte arrays or <see cref="ReadOnlyMemory{T}"/>
        /// of bytes.
        /// </param>
        /// <remarks>
        /// Added in ADBC 1.1.0. This isn't an overload of
        /// <see cref="Connect(IReadOnlyDictionary{string, string})"/> because that would
        /// make existing calls of the form <c>Connect(null)</c> ambiguous.
        ///
        /// The default implementation converts the non-binary values to strings and calls
        /// <see cref="Connect(IReadOnlyDictionary{string, string})"/>, then applies binary
        /// values to the new connection through <see cref="AdbcConnection.SetOptionBytes"/>.
        /// Drivers that need binary options while connecting should override it.
        /// </remarks>
        public virtual AdbcConnection ConnectWithTypedOptions(IReadOnlyDictionary<string, object>? options)
        {
            if (options == null)
            {
                return Connect(null);
            }

            Dictionary<string, string> stringOptions = new Dictionary<string, string>(options.Count);
            List<KeyValuePair<string, ReadOnlyMemory<byte>>> binaryOptions = new List<KeyValuePair<string, ReadOnlyMemory<byte>>>();
            foreach (KeyValuePair<string, object> pair in options)
            {
                if (AdbcOptionValue.TryGetBytes(pair.Value, out ReadOnlyMemory<byte> bytes))
                {
                    binaryOptions.Add(new KeyValuePair<string, ReadOnlyMemory<byte>>(pair.Key, bytes));
                }
                else
                {
                    stringOptions[pair.Key] = AdbcOptionValue.ToOptionString(pair.Key, pair.Value);
                }
            }

            AdbcConnection connection = Connect(stringOptions);
            try
            {
                foreach (KeyValuePair<string, ReadOnlyMemory<byte>> pair in binaryOptions)
                {
                    connection.SetOptionBytes(pair.Key, pair.Value.Span);
                }
            }
            catch
            {
                connection.Dispose();
                throw;
            }

            return connection;
        }

        /// <summary>
        /// Asynchronously create a new connection to the database using options that
        /// aren't all strings.
        /// </summary>
        /// <param name="options">
        /// Additional options to use when connecting. See
        /// <see cref="ConnectWithTypedOptions(IReadOnlyDictionary{string, object})"/>.
        /// </param>
        /// <param name="cancellationToken">
        /// A token used to cancel the operation before it starts. Once started, the operation
        /// runs to completion, because databases and drivers have no way to interrupt it.
        /// </param>
        /// <remarks>
        /// Added in ADBC 1.1.0. The default implementation runs
        /// <see cref="ConnectWithTypedOptions(IReadOnlyDictionary{string, object})"/>
        /// on the thread pool.
        /// </remarks>
        public virtual Task<AdbcConnection> ConnectWithTypedOptionsAsync(IReadOnlyDictionary<string, object>? options, CancellationToken cancellationToken = default)
        {
            return AdbcAsync.Run(() => ConnectWithTypedOptions(options), null, cancellationToken);
        }

        /// <summary>
        /// Asynchronously create a new connection to the database.
        /// </summary>
        /// <param name="options">
        /// Additional options to use when connecting.
        /// </param>
        /// <param name="cancellationToken">
        /// A token used to cancel the operation before it starts. Once started, the operation
        /// runs to completion, because databases and drivers have no way to interrupt it.
        /// </param>
        public virtual Task<AdbcConnection> ConnectAsync(IReadOnlyDictionary<string, string>? options, CancellationToken cancellationToken = default)
        {
            return AdbcAsync.Run(() => Connect(options), null, cancellationToken);
        }

        public virtual void Dispose() { }

        /// <summary>
        /// Asynchronously releases the resources used by the database.
        /// </summary>
        /// <remarks>
        /// The default implementation calls <see cref="Dispose"/>.
        /// </remarks>
        public virtual ValueTask DisposeAsync()
        {
            Dispose();
            return default;
        }
    }
}
