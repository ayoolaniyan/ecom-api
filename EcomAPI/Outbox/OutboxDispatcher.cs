using EcomAPI.Data;
using EcomAPI.Events;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace EcomAPI.Outbox
{
    // Polls the write database for unpublished outbox messages and hands them to the
    // configured IEventPublisher in insertion order. A message may be published more than
    // once (e.g. crash between publish and marking it processed), so consumers must be idempotent.
    // Assumes a single running instance of the API.
    public class OutboxDispatcher : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IEventPublisher _publisher;
        private readonly OutboxOptions _options;
        private readonly ILogger<OutboxDispatcher> _logger;

        public OutboxDispatcher(
            IServiceScopeFactory scopeFactory,
            IEventPublisher publisher,
            IOptions<OutboxOptions> options,
            ILogger<OutboxDispatcher> logger)
        {
            _scopeFactory = scopeFactory;
            _publisher = publisher;
            _options = options.Value;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(_options.PollingIntervalMs));

            do
            {
                try
                {
                    await DispatchPendingAsync(stoppingToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(ex, "Outbox dispatch failed");
                }
            }
            while (await timer.WaitForNextTickAsync(stoppingToken));
        }

        private async Task DispatchPendingAsync(CancellationToken stoppingToken)
        {
            using var scope = _scopeFactory.CreateScope();
            var context = scope.ServiceProvider.GetRequiredService<WriteDbContext>();

            var messages = await context.OutboxMessages
                .Where(m => m.ProcessedAt == null)
                .OrderBy(m => m.Id)
                .Take(_options.BatchSize)
                .ToListAsync(stoppingToken);

            if (messages.Count == 0)
                return;

            foreach (var message in messages)
            {
                try
                {
                    await _publisher.PublishAsync(message, stoppingToken);
                    message.ProcessedAt = DateTime.UtcNow;
                    message.Error = null;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    message.Attempts++;
                    message.Error = ex.Message;
                    _logger.LogWarning(ex, "Failed to publish outbox message {MessageId} (attempt {Attempts})", message.Id, message.Attempts);

                    // Stop here so later events are not published ahead of this one.
                    break;
                }
            }

            await context.SaveChangesAsync(CancellationToken.None);
        }
    }
}
