using EcomAPI.Outbox;

namespace EcomAPI.Events
{
    public interface IEventPublisher
    {
        Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default);
    }
}
