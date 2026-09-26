using System.Text.Json;
using MediatR;

namespace EcomAPI.Events
{
    // Maps event type names to CLR types so events can cross a process boundary as JSON.
    // Register every event that is written to the outbox here.
    public static class EventSerializer
    {
        private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

        private static readonly Dictionary<string, Type> EventTypes = new()
        {
            [nameof(OrderCreatedEvent)] = typeof(OrderCreatedEvent),
        };

        public static (string Type, string Payload) Serialize<TEvent>(TEvent evt) where TEvent : notnull
        {
            var type = evt.GetType();
            if (!EventTypes.ContainsKey(type.Name))
                throw new InvalidOperationException($"Event type '{type.Name}' is not registered in {nameof(EventSerializer)}.");

            return (type.Name, JsonSerializer.Serialize(evt, type, JsonOptions));
        }

        // Returns null when the type is unknown, e.g. an event produced by a newer version of the service.
        public static INotification? Deserialize(string type, string payload)
        {
            if (!EventTypes.TryGetValue(type, out var clrType))
                return null;

            return JsonSerializer.Deserialize(payload, clrType, JsonOptions) as INotification;
        }
    }
}
