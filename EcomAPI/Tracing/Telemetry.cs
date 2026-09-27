using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;

namespace EcomAPI.Tracing
{
    // The application's ActivitySource and helpers for carrying W3C trace context across
    // the asynchronous hops (outbox row, Kafka headers) that auto-instrumentation can't see.
    public static class Telemetry
    {
        public const string SourceName = "EcomAPI";

        public static readonly ActivitySource Source = new(SourceName);

        // Starts a span continuing the trace stored on an outbox row, or a new trace if there is none.
        public static Activity? StartActivityFromStoredContext(string name, ActivityKind kind, string? traceParent, string? traceState)
        {
            ActivityContext.TryParse(traceParent, traceState, isRemote: true, out var parent);
            return Source.StartActivity(name, kind, parent);
        }

        // Writes traceparent/tracestate (and baggage) into Kafka headers.
        public static void Inject(ActivityContext context, Headers headers)
        {
            Propagators.DefaultTextMapPropagator.Inject(
                new PropagationContext(context, Baggage.Current),
                headers,
                static (h, key, value) =>
                {
                    h.Remove(key);
                    h.Add(key, Encoding.UTF8.GetBytes(value));
                });
        }

        public static PropagationContext Extract(Headers headers)
        {
            return Propagators.DefaultTextMapPropagator.Extract(
                default,
                headers,
                static (h, key) => h.TryGetLastBytes(key, out var bytes) ? [Encoding.UTF8.GetString(bytes)] : []);
        }

        public static void RecordException(this Activity? activity, Exception ex)
        {
            if (activity is null)
                return;

            activity.AddException(ex);
            activity.SetStatus(ActivityStatusCode.Error, ex.Message);
        }
    }
}
