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
    /// This provides a common interface for vendor-specific driver
    /// initialization routines.
    /// </summary>
    public abstract class AdbcDriver : IDisposable
    {
        /// <summary>
        /// Gets the version of the ADBC specification implemented by this driver,
        /// as one of the constants defined by <see cref="AdbcVersion"/>.
        /// </summary>
        public virtual int DriverVersion => AdbcVersion.Version_1_0_0;

        /// <summary>
        /// Open a database via this driver.
        /// </summary>
        /// <param name="parameters">
        /// Driver-specific parameters.
        /// </param>
        public abstract AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters);

        /// <summary>
        /// Open a database via this driver.
        /// </summary>
        /// <param name="parameters">
        /// Driver-specific parameters. Values may be strings, booleans, integers,
        /// floating-point numbers, byte arrays or <see cref="ReadOnlyMemory{T}"/> of bytes.
        /// </param>
        /// <remarks>
        /// The default implementation converts the non-binary values to strings and calls
        /// <see cref="Open(IReadOnlyDictionary{string, string})"/>, then applies binary values
        /// to the new database through <see cref="AdbcDatabase.SetOptionBytes"/>. Drivers that
        /// need binary options while opening should override this method.
        /// </remarks>
        public virtual AdbcDatabase Open(IReadOnlyDictionary<string, object> parameters)
        {
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            Dictionary<string, string> stringParameters = new Dictionary<string, string>(parameters.Count);
            List<KeyValuePair<string, ReadOnlyMemory<byte>>> binaryParameters = new List<KeyValuePair<string, ReadOnlyMemory<byte>>>();
            foreach (KeyValuePair<string, object> pair in parameters)
            {
                if (AdbcOptionValue.TryGetBytes(pair.Value, out ReadOnlyMemory<byte> bytes))
                {
                    binaryParameters.Add(new KeyValuePair<string, ReadOnlyMemory<byte>>(pair.Key, bytes));
                }
                else
                {
                    stringParameters[pair.Key] = AdbcOptionValue.ToOptionString(pair.Key, pair.Value);
                }
            }

            AdbcDatabase database = Open(stringParameters);
            try
            {
                foreach (KeyValuePair<string, ReadOnlyMemory<byte>> pair in binaryParameters)
                {
                    database.SetOptionBytes(pair.Key, pair.Value.Span);
                }
            }
            catch
            {
                database.Dispose();
                throw;
            }

            return database;
        }

        /// <summary>
        /// Asynchronously open a database via this driver.
        /// </summary>
        /// <param name="parameters">
        /// Driver-specific parameters.
        /// </param>
        /// <param name="cancellationToken">
        /// A token used to cancel the operation before it starts. Once started, the operation
        /// runs to completion, because databases and drivers have no way to interrupt it.
        /// </param>
        public virtual Task<AdbcDatabase> OpenAsync(IReadOnlyDictionary<string, string> parameters, CancellationToken cancellationToken = default)
        {
            return AdbcAsync.Run(() => Open(parameters), null, cancellationToken);
        }

        public virtual void Dispose()
        {
        }
    }
}
