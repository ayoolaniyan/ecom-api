using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace EcomAPI.Observability
{
    public static class ObservabilityExtensions
    {
        // Exports the signals enabled under Telemetry over OTLP. All three share one resource
        // (service name, version, environment), so a backend can correlate them: logs carry the
        // trace/span id they were written in, and latency histograms carry trace exemplars.
        public static WebApplicationBuilder AddObservability(this WebApplicationBuilder builder, bool redisCache)
        {
            var options = builder.Configuration.GetSection(TelemetryOptions.SectionName).Get<TelemetryOptions>() ?? new TelemetryOptions();
            if (!options.AnyEnabled)
                return builder;

            var endpoint = new Uri(options.OtlpEndpoint);
            var otel = builder.Services.AddOpenTelemetry()
                .ConfigureResource(resource => resource
                    .AddService(options.ServiceName, serviceVersion: typeof(Program).Assembly.GetName().Version?.ToString())
                    .AddAttributes([new("deployment.environment.name", builder.Environment.EnvironmentName)]));

            // The trace of a POST continues through the outbox dispatcher, Kafka and the projection (see Telemetry.cs).
            if (options.Tracing.Enabled)
            {
                otel.WithTracing(tracing =>
                {
                    tracing
                        .SetSampler(new ParentBasedSampler(new TraceIdRatioBasedSampler(options.Tracing.SamplingRatio)))
                        .AddSource(Telemetry.SourceName)
                        // Probes hit the health endpoints every few seconds; don't trace them or their dependency checks.
                        .AddAspNetCoreInstrumentation(opt => opt.Filter = ctx => !ctx.Request.Path.StartsWithSegments("/healthz"))
                        .AddEntityFrameworkCoreInstrumentation()
                        .AddOtlpExporter(opt => opt.Endpoint = endpoint);

                    if (redisCache)
                        tracing.AddRedisInstrumentation();
                });
            }

            if (options.Metrics.Enabled)
            {
                otel.WithMetrics(metrics => metrics
                    .AddMeter(AppMetrics.MeterName)
                    // HTTP server and Kestrel metrics built into ASP.NET Core.
                    .AddAspNetCoreInstrumentation()
                    // GC, thread pool, memory, CPU and exception metrics built into the .NET 9 runtime.
                    .AddMeter("System.Runtime")
                    // Attach the current trace id to histogram samples, so a latency spike links to a trace.
                    .SetExemplarFilter(ExemplarFilterType.TraceBased)
                    .AddOtlpExporter((exporter, reader) =>
                    {
                        exporter.Endpoint = endpoint;
                        reader.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = options.Metrics.ExportIntervalMs;
                    }));
            }

            // Console logging stays; this adds a second provider that ships the same entries over OTLP.
            if (options.Logs.Enabled)
            {
                otel.WithLogging(
                    logging => logging.AddOtlpExporter(opt => opt.Endpoint = endpoint),
                    opt =>
                    {
                        opt.IncludeFormattedMessage = true;
                        opt.IncludeScopes = true;
                    });
            }

            return builder;
        }
    }
}
