
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
using System.Threading.Tasks;

namespace Apache.Arrow.Adbc
{
    /// <summary>
    /// Class that will retry calling a method with an exponential backoff.
    /// </summary>
    public class AdbcRetryManager
    {
        public static async Task<T> ExecuteWithRetriesAsync<T>(
           Func<Task<T>> action,
           int maxRetries = 5,
           int initialDelayMilliseconds = 200)
        {
            return await ExecuteWithRetriesAsync<T>(null, action, maxRetries, initialDelayMilliseconds);
        }

        public static async Task<T> ExecuteWithRetriesAsync<T>(
            ITokenProtectedResource? tokenProtectedResource,
            Func<Task<T>> action,
            int maxRetries = 5,
            int initialDelayMilliseconds = 200)
        {
            if (action == null)
            {
                throw new AdbcException("There is no method to retry", AdbcStatusCode.InvalidArgument);
            }

            int retryCount = 0;
            int delay = initialDelayMilliseconds;

            while (retryCount < maxRetries)
            {
                try
                {
                    T result = await action();
                    return result;
                }
                catch (Exception ex)
                {
                    retryCount++;
                    if (retryCount >= maxRetries)
                    {
                        if (tokenProtectedResource?.TokenRequiresUpdate(ex) == true)
                        {
                            throw new AdbcException($"Cannot update access token after {maxRetries} tries", AdbcStatusCode.Unauthenticated, ex);
                        }

                        throw new AdbcException($"Cannot execute {action.Method.Name} after {maxRetries} tries", AdbcStatusCode.UnknownError, ex);
                    }

                    if ((tokenProtectedResource?.UpdateToken == null))
                    {
                        throw new AdbcException($"UpdateToken cannot be null on the token-protected resource", AdbcStatusCode.InvalidArgument);
                    }
                    else
                    {
                        if (tokenProtectedResource.TokenRequiresUpdate(ex))
                        {
                            await tokenProtectedResource.UpdateToken();
                        }
                    }

                    await Task.Delay(delay);
                    delay *= 2;
                }
            }

            throw new AdbcException($"Could not successfully call {action.Method.Name}", AdbcStatusCode.UnknownError);
        }
    }
}
