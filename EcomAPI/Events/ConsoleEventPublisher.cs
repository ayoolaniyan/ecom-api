using EcomAPI.Outbox;

namespace EcomAPI.Events
{
    public class ConsoleEventPublisher : IEventPublisher
    {
        public Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            Console.WriteLine($"--> Event published: {message.Type} {message.Payload}");
            return Task.CompletedTask;
        }
    }
}
