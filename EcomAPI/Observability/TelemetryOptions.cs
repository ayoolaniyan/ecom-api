namespace EcomAPI.Observability
{
    // OpenTelemetry export settings. Traces, metrics and logs are switched on separately and all
    // go to the same OTLP/gRPC endpoint, normally an OpenTelemetry Collector.
    // Everything is off by default so `dotnet run` needs no collector.
    public class TelemetryOptions
    {
        public const string SectionName = "Telemetry";

        public string ServiceName { get; set; } = "ecomapi";

        // OTLP/gRPC endpoint of a collector or backend.
        public string OtlpEndpoint { get; set; } = "http://localhost:4317";

        public TracingSettings Tracing { get; set; } = new();

        public MetricsSettings Metrics { get; set; } = new();

        public LogsSettings Logs { get; set; } = new();

        public bool AnyEnabled => Tracing.Enabled || Metrics.Enabled || Logs.Enabled;

        public class TracingSettings
        {
            public bool Enabled { get; set; }

            // Fraction of new traces to record (0.0–1.0). Traces continued from a caller or from
            // an outbox/Kafka message follow the sampling decision already made upstream.
            public double SamplingRatio { get; set; } = 1.0;
        }

        public class MetricsSettings
        {
            public bool Enabled { get; set; }

            // How often metrics are pushed to the collector.
            public int ExportIntervalMs { get; set; } = 15000;
        }

        public class LogsSettings
        {
            public bool Enabled { get; set; }
        }
    }
}
