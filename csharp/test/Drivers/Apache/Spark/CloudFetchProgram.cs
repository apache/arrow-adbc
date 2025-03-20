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
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Client;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    /// <summary>
    /// Simple program to test connection to Databricks using the config file.
    /// </summary>
    public static class CloudFetchProgram
    {
        public static async Task Main(string[] args)
        {
            // Get the config file path from environment variable
            string configFile = Environment.GetEnvironmentVariable("SPARK_TEST_CONFIG_FILE")
                ?? Path.Combine(Directory.GetCurrentDirectory(), "test", "spark_test_config.json");
            
            Console.WriteLine($"Using config file: {configFile}");
            
            if (!File.Exists(configFile))
            {
                Console.WriteLine($"Config file does not exist: {configFile}");
                return;
            }
            
            try
            {
                // Read the config file
                string jsonContent = File.ReadAllText(configFile);
                var config = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(jsonContent);
                
                if (config == null)
                {
                    Console.WriteLine("Failed to deserialize config file");
                    return;
                }
                
                // Extract connection details
                string hostName = config.TryGetValue("hostName", out var host) ? host.GetString() ?? "" : "";
                string path = config.TryGetValue("path", out var p) ? p.GetString() ?? "" : "";
                string token = config.TryGetValue("token", out var t) ? t.GetString() ?? "" : "";
                string authType = config.TryGetValue("auth_type", out var a) ? a.GetString() ?? "" : "";
                
                Console.WriteLine($"Host: {hostName}");
                Console.WriteLine($"Path: {path}");
                Console.WriteLine($"Auth Type: {authType}");
                Console.WriteLine($"Token: {(string.IsNullOrEmpty(token) ? "Not found" : "Found (not shown)")}");
                
                // Create connection properties
                var properties = new Dictionary<string, string>
                {
                    { "host", hostName },
                    { "token", token },
                    { "http_path", path },
                    { "adbc.spark.cloudfetch.enabled", "true" },
                    { "adbc.spark.cloudfetch.lz4.enabled", "true" }
                };
                
                // Create the driver and connection
                var driver = new SparkDriver();
                using var db = driver.Open(properties);
                using var connection = db.Connect(new Dictionary<string, string>());
                
                Console.WriteLine("Successfully created connection to Databricks");
                
                // Create a statement
                using var statement = connection.CreateStatement();
                
                // Execute a simple query
                statement.SqlQuery = "SELECT 1 as test";
                var result = await statement.ExecuteQueryAsync();
                
                Console.WriteLine("Successfully executed query");
                
                if (result.Stream == null)
                {
                    Console.WriteLine("Received null result stream");
                    return;
                }
                
                // Read the data
                var batch = await result.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                {
                    Console.WriteLine("Received null batch");
                    return;
                }
                
                Console.WriteLine($"Received batch with {batch.Length} rows and {batch.ColumnCount} columns");
                
                // Get the value
                var column = batch.Column(0) as Int32Array;
                if (column == null)
                {
                    Console.WriteLine("Column is not an Int32Array");
                    return;
                }
                
                Console.WriteLine($"Value: {column.GetValue(0)}");
                
                // Test large query with CloudFetch enabled
                statement.SetOption(SparkStatement.Options.UseCloudFetch, "true");
                statement.SetOption(SparkStatement.Options.CanDecompressLz4, "true");
                statement.SetOption(SparkStatement.Options.MaxBytesPerFile, "10485760"); // 10MB
                
                // Execute a range query
                statement.SqlQuery = "SELECT * FROM range(100000)";
                result = await statement.ExecuteQueryAsync();
                
                Console.WriteLine("Successfully executed large query");
                
                if (result.Stream == null)
                {
                    Console.WriteLine("Received null result stream for large query");
                    return;
                }
                
                // Read and count rows
                long totalRows = 0;
                RecordBatch? rangeBatch;
                while ((rangeBatch = await result.Stream.ReadNextRecordBatchAsync()) != null)
                {
                    totalRows += rangeBatch.Length;
                    rangeBatch.Dispose();
                }
                
                Console.WriteLine($"Read {totalRows} rows from range function");
                
                Console.WriteLine("Tests completed successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                    Console.WriteLine(ex.InnerException.StackTrace);
                }
            }
        }
    }
} 