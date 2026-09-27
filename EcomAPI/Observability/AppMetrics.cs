using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace EcomAPI.Observability
{
    // The application's own metrics: orders, the outbox → Kafka → projection pipeline and the
    // query cache. Instruments are cheap no-ops until a MeterListener (the OpenTelemetry SDK,
    // with Telemetry:Metrics:Enabled) subscribes to the meter.
    public static class AppMetrics
    {
        public const string MeterName = "EcomAPI";

        private static readonly Meter Meter = new(MeterName);

        // Buckets for the pipeline delays: the outbox polls every second, so most values land
        // between 0.1s and a few seconds; the long tail catches outages.
        private static readonly InstrumentAdvice<double> DelayBuckets = new()
        {
            HistogramBucketBoundaries = [0.05, 0.1, 0.25, 0.5, 1, 1.5, 2, 3, 5, 10, 30, 60, 120, 300]
        };

        private static readonly Counter<long> OrdersCreated = Meter.CreateCounter<long>(
            "ecom.orders.created", "{order}", "Orders written to the write database.");

        private static readonly Counter<long> OutboxPublished = Meter.CreateCounter<long>(
            "ecom.outbox.published", "{message}", "Outbox publish attempts, by outcome (success, failure).");

        private static readonly Histogram<double> OutboxDispatchDelay = Meter.CreateHistogram(
            "ecom.outbox.dispatch.delay", "s", "Time from writing an outbox message to publishing it.", advice: DelayBuckets);

        private static readonly Counter<long> EventsProcessed = Meter.CreateCounter<long>(
            "ecom.events.processed", "{event}", "Events handled by the projections, by outcome (success, retry, skipped).");

        private static readonly Histogram<double> ProjectionLag = Meter.CreateHistogram(
            "ecom.projection.lag", "s", "Time from an order being written to it appearing in the read model.", advice: DelayBuckets);

        private static readonly Counter<long> CacheRequests = Meter.CreateCounter<long>(
            "ecom.cache.requests", "{request}", "Query cache lookups, by query and result (hit, miss).");

        // Outbox backlog, measured by OutboxDispatcher after each poll and read at export time.
        private static long _outboxPending;
        private static double _outboxOldestPendingAge;

        static AppMetrics()
        {
            Meter.CreateObservableGauge(
                "ecom.outbox.pending", () => Volatile.Read(ref _outboxPending), "{message}",
                "Outbox messages not yet published.");
            Meter.CreateObservableGauge(
                "ecom.outbox.oldest_pending.age", () => Volatile.Read(ref _outboxOldestPendingAge), "s",
                "Age of the oldest unpublished outbox message; 0 when the outbox is empty.");
        }

        public static void OrderCreated() => OrdersCreated.Add(1);

        public static void OutboxPublishSucceeded(string eventType, DateTime occurredAt)
        {
            OutboxPublished.Add(1, new TagList { { "event.type", eventType }, { "outcome", "success" } });
            OutboxDispatchDelay.Record(SecondsSince(occurredAt), new KeyValuePair<string, object?>("event.type", eventType));
        }

        public static void OutboxPublishFailed(string eventType) =>
            OutboxPublished.Add(1, new TagList { { "event.type", eventType }, { "outcome", "failure" } });

        public static void OutboxBacklog(long pending, DateTime? oldestOccurredAt)
        {
            Volatile.Write(ref _outboxPending, pending);
            Volatile.Write(ref _outboxOldestPendingAge, oldestOccurredAt is { } oldest ? SecondsSince(oldest) : 0);
        }

        // outcome: "success", "retry" (handler failed, message will be redelivered) or "skipped" (unknown/malformed).
        public static void EventProcessed(string messagingSystem, string? eventType, string outcome) =>
            EventsProcessed.Add(1, new TagList
            {
                { "messaging.system", messagingSystem },
                { "event.type", eventType ?? "unknown" },
                { "outcome", outcome }
            });

        public static void OrderProjected(DateTime orderCreatedAt) =>
            ProjectionLag.Record(SecondsSince(orderCreatedAt), new KeyValuePair<string, object?>("event.type", nameof(OrderCreatedEvent)));

        public static void CacheLookup(string query, bool hit) =>
            CacheRequests.Add(1, new TagList { { "query", query }, { "result", hit ? "hit" : "miss" } });

        // Order.CreatedAt is local time (Kind = Local, also after the JSON round trip). Outbox
        // timestamps are written as UTC but come back from SQLite as Kind = Unspecified.
        private static double SecondsSince(DateTime timestamp)
        {
            var utc = timestamp.Kind == DateTimeKind.Local ? timestamp.ToUniversalTime() : timestamp;
            return Math.Max(0, (DateTime.UtcNow - utc).TotalSeconds);
        }
    }
}
