namespace EcomAPI.Tracing
{
    public class TracingOptions
    {
        public const string SectionName = "Tracing";

        // Off by default so `dotnet run` needs no collector. Nothing is exported while disabled.
        public bool Enabled { get; set; }

        public string ServiceName { get; set; } = "ecomapi";

        // OTLP/gRPC endpoint of a collector or tracing backend (e.g. Jaeger).
        public string OtlpEndpoint { get; set; } = "http://localhost:4317";

        // Fraction of new traces to record (0.0–1.0). Traces continued from a caller or from
        // an outbox/Kafka message follow the sampling decision already made upstream.
        public double SamplingRatio { get; set; } = 1.0;
    }
}
