using System.Diagnostics;
using EcomAPI.Outbox;
using EcomAPI.Tracing;
using MediatR;

namespace EcomAPI.Events
{
    // Dispatches outbox events straight to the MediatR notification handlers in this process.
    // Used when no broker is configured (EventBus:Provider = InProcess).
    public class InProcessEventPublisher : IEventPublisher
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<InProcessEventPublisher> _logger;

        public InProcessEventPublisher(IServiceScopeFactory scopeFactory, ILogger<InProcessEventPublisher> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
        }

        public async Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            using var activity = Telemetry.Source.StartActivity($"process {message.Type}", ActivityKind.Consumer);
            activity?.SetTag("messaging.system", "in-process");
            activity?.SetTag("messaging.operation.type", "process");
            activity?.SetTag("messaging.message.id", message.Id.ToString());

            var evt = EventSerializer.Deserialize(message.Type, message.Payload);
            if (evt is null)
            {
                activity?.SetStatus(ActivityStatusCode.Error, "Unknown event type; skipped");
                _logger.LogWarning("Skipping outbox message {MessageId}: unknown event type {EventType}", message.Id, message.Type);
                return;
            }

            using var scope = _scopeFactory.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
            await mediator.Publish(evt, cancellationToken);
        }
    }
}
