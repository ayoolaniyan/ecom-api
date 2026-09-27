using System.Diagnostics;
using EcomAPI.Events;

namespace EcomAPI.Outbox
{
    // An event stored in the write database in the same transaction as the state change
    // that raised it. OutboxDispatcher later hands it to the configured IEventPublisher.
    public class OutboxMessage
    {
        public long Id { get; set; }

        public required string Type { get; set; }

        public required string Key { get; set; }

        public required string Payload { get; set; }

        public DateTime OccurredAt { get; set; }

        public DateTime? ProcessedAt { get; set; }

        public int Attempts { get; set; }

        public string? Error { get; set; }

        // W3C trace context of the request that raised the event, so dispatch and consumption
        // continue the same trace after the request has returned.
        public string? TraceParent { get; set; }

        public string? TraceState { get; set; }

        public static OutboxMessage Create<TEvent>(TEvent evt, string key) where TEvent : notnull
        {
            var (type, payload) = EventSerializer.Serialize(evt);
            var activity = Activity.Current;

            return new OutboxMessage
            {
                Type = type,
                Key = key,
                Payload = payload,
                OccurredAt = DateTime.UtcNow,
                TraceParent = activity?.Id,
                TraceState = activity?.TraceStateString
            };
        }
    }
}
