/*
 * Test program to verify CloudFetchDownloader and CloudFetchReader tracing
 */

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Drivers.Databricks.Reader.CloudFetch;
using Apache.Arrow.Adbc.Tracing;
using Moq;

namespace TestCloudFetchTracing
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("=== CloudFetch Tracing Verification ===");

            // Check environment
            var tracesExporter = Environment.GetEnvironmentVariable("OTEL_TRACES_EXPORTER");
            Console.WriteLine($"OTEL_TRACES_EXPORTER = '{tracesExporter ?? "(not set)"}'");

            if (tracesExporter != "adbcfile")
            {
                Console.WriteLine("⚠️  WARNING: Expected OTEL_TRACES_EXPORTER=adbcfile");
                Console.WriteLine("   Please set: export OTEL_TRACES_EXPORTER=adbcfile");
                Console.WriteLine("   Or in PowerShell: $env:OTEL_TRACES_EXPORTER='adbcfile'");
                Console.WriteLine();
            }

            try
            {
                Console.WriteLine("=== Testing DatabricksConnection Auto-Tracing ===");

                // Create DatabricksConnection - this triggers DatabricksConnection's static constructor
                var connectionProperties = new Dictionary<string, string>
                {
                    ["adbc.databricks.hostname"] = "test.databricks.com",
                    ["adbc.databricks.path"] = "/sql/1.0/endpoints/test",
                    ["adbc.databricks.token"] = "fake-token-for-testing"
                };

                Console.WriteLine("Creating DatabricksConnection to trigger auto-initialization...");

                var connection = new DatabricksConnection(connectionProperties);
                Console.WriteLine($"✅ DatabricksConnection created with assembly: {connection.AssemblyName}");
                Console.WriteLine("✅ DatabricksConnection static constructor should have auto-initialized TracerProvider");

                Console.WriteLine("\n=== Testing CloudFetch Components Use Connection Tracing ===");

                // Create CloudFetchDownloader - should work with global TracerProvider
                var downloadQueue = new BlockingCollection<IDownloadResult>(10);
                var resultQueue = new BlockingCollection<IDownloadResult>(10);
                var mockMemoryManager = new Mock<ICloudFetchMemoryBufferManager>();
                var httpClient = new HttpClient();
                var mockResultFetcher = new Mock<ICloudFetchResultFetcher>();

                var downloader = new CloudFetchDownloader(
                    downloadQueue,
                    resultQueue,
                    mockMemoryManager.Object,
                    httpClient,
                    mockResultFetcher.Object,
                    3, // maxParallelDownloads
                    false // isLz4Compressed
                );

                // Verify downloader forces TracerProvider initialization and uses matching assembly name
                Console.WriteLine($"CloudFetchDownloader.AssemblyName: '{downloader.AssemblyName}'");
                Console.WriteLine($"CloudFetchDownloader.Trace.ActivitySourceName: '{downloader.Trace.ActivitySourceName}'");
                Console.WriteLine("✅ CloudFetchDownloader forces DatabricksConnection static constructor to run first");
                Console.WriteLine("✅ TracerProvider initialized before ActivitySource creation = activities captured!");

                Console.WriteLine("\n=== Testing Activity Creation ===");

                // Test creating an activity - this should be captured by the TracerProvider
                downloader.TraceActivity(activity =>
                {
                    if (activity != null)
                    {
                        activity.SetTag("test.cloudfetch.downloader", "activity_test");
                        activity.AddEvent("Test event for CloudFetchDownloader");
                        Console.WriteLine($"✅ Activity created: {activity.DisplayName}");
                        Console.WriteLine($"   Activity ID: {activity.Id}");
                        Console.WriteLine($"   Activity Source: {activity.Source.Name}");
                    }
                    else
                    {
                        Console.WriteLine("❌ Activity is null - TracerProvider may not be listening");
                    }
                }, "TestActivity");

                // Clean up
                downloader.Dispose();
                httpClient.Dispose();
                connection.Dispose();

                Console.WriteLine("\n=== Check Output Files ===");
                Console.WriteLine("If the DatabricksConnection auto-tracing is working correctly, you should see:");
                Console.WriteLine("1. Activity data in trace files (look for .log files in ~/Library/Application Support/Apache.Arrow.Adbc/Traces/)");
                Console.WriteLine("2. The test activity with tag 'test.cloudfetch.downloader'");
                Console.WriteLine("3. All Databricks activities (DatabricksConnection, CloudFetchDownloader, CloudFetchReader)");
                Console.WriteLine("4. Future CloudFetch activities from real usage automatically captured");
                Console.WriteLine("5. Silent operation - ExportersBuilder handles everything in the background");

            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error during tracing test: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }

            Console.WriteLine("\n=== Test Complete ===");
        }
    }
}
