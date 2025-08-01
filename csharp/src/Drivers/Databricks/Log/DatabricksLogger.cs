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

namespace Apache.Arrow.Adbc.Drivers.Databricks.Log;

public class DatabricksLogger
{
    public static readonly string DATABRICKS_LOG_FILE = "databricks_adbc.log";
    public static readonly DatabricksLogger LOGGER = new DatabricksLogger();

    private static readonly object _logLock = new object();
    private static readonly string _logFilePath = Path.Combine(Path.GetTempPath(), DATABRICKS_LOG_FILE);

    public void trace(string message)
    {
        WriteLog("TRACE", message);
    }

    public void debug(string message)
    {
        WriteLog("DEBUG", message);
    }

    public void info(string message)
    {
        WriteLog("INFO", message);
    }

    public void warn(string message)
    {
        WriteLog("WARN", message);
    }

    public void error(string message)
    {
        WriteLog("ERROR", message);
    }

    public void fatal(string message)
    {
        WriteLog("FATAL", message);
    }

    private static void WriteLog(string level, string message)
    {
        var logMessage = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{level}] {message}";
        
        lock (_logLock)
        {
            try
            {
                File.AppendAllText(_logFilePath, logMessage + Environment.NewLine);
            }
            catch (Exception)
            {
                // Silently fail if we can't write to log file
                // Don't throw exceptions from logging code
            }
        }
    }
}
