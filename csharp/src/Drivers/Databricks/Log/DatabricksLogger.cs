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

using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Log;

public class DatabricksLogger
{
    public static readonly string DATABRICKS_LOG_FILE = "databricks.log";

    public static readonly DatabricksLogger LOGGER = new DatabricksLogger();

    public void trace(string message)
    {
        log(message, LogLevel.Trace);
    }

    public void debug(string message)
    {
        log(message, LogLevel.Debug);
    }

    public void info(string message)
    {
        log(message, LogLevel.Information);
    }

    public void warn(string message)
    {
        log(message, LogLevel.Warning);
    }

    public void error(string message)
    {
        log(message, LogLevel.Error);
    }

    public void fatal(string message)
    {
        log(message, LogLevel.Critical);
    }

    private void log(string message, LogLevel level)
    {
        var logMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{level}] {message}";
        string logFilePath = Path.Combine(Path.GetTempPath(), DATABRICKS_LOG_FILE);
        File.AppendAllText(logFilePath, logMessage + Environment.NewLine);
    }
}
