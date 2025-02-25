using System.Diagnostics;
using Apache.Arrow.Adbc.Tracing;
using OpenTelemetry;
using OpenTelemetry.Trace;

namespace TracingPerformance
{
    internal class Program
    {
        const string SourceName = "mySource";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello, World!");
            using (TracerProvider traceProvider = Sdk.CreateTracerProviderBuilder()
                .AddSource(SourceName)
                .AddAdbcFileExporter(SourceName, null)
                .Build())
            {
                using (ActivitySource activitySource = new ActivitySource(SourceName))
                {
                    for (int i = 0; i < 10000; i++)
                    {
                        using (Activity? activity = activitySource.StartActivity("activity"))
                        {
                            Debug.Assert(activity != null);
                            activity?.AddEvent("event");
                            await Task.Delay(TimeSpan.FromMilliseconds(0.1));
                        }
                    }
                }
            }
        }
    }
}
