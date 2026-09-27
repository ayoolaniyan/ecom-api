using System.Diagnostics;
using EcomAPI.Data;
using EcomAPI.Events;
using EcomAPI.Observability;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using OpenTelemetry;

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

            // Polling runs every second; keep its queries out of the traces.
            List<OutboxMessage> messages;
            using (SuppressInstrumentationScope.Begin())
            {
                messages = await context.OutboxMessages
                    .Where(m => m.ProcessedAt == null)
                    .OrderBy(m => m.Id)
                    .Take(_options.BatchSize)
                    .ToListAsync(stoppingToken);
            }

            if (messages.Count == 0)
            {
                AppMetrics.OutboxBacklog(0, null);
                return;
            }

            foreach (var message in messages)
            {
                // Continues the trace of the request that wrote the message.
                using var activity = Telemetry.StartActivityFromStoredContext("outbox dispatch", ActivityKind.Internal, message.TraceParent, message.TraceState);
                activity?.SetTag("outbox.message_id", message.Id);
                activity?.SetTag("outbox.event_type", message.Type);
                activity?.SetTag("outbox.attempt", message.Attempts + 1);

                try
                {
                    await _publisher.PublishAsync(message, stoppingToken);
                    message.ProcessedAt = DateTime.UtcNow;
                    message.Error = null;
                    AppMetrics.OutboxPublishSucceeded(message.Type, message.OccurredAt);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    activity.RecordException(ex);
                    AppMetrics.OutboxPublishFailed(message.Type);
                    message.Attempts++;
                    message.Error = ex.Message;
                    _logger.LogWarning(ex, "Failed to publish outbox message {MessageId} (attempt {Attempts})", message.Id, message.Attempts);

                    // Stop here so later events are not published ahead of this one.
                    break;
                }
            }

            using (SuppressInstrumentationScope.Begin())
            {
                await context.SaveChangesAsync(CancellationToken.None);
                await MeasureBacklogAsync(context, stoppingToken);
            }
        }

        // Records what is still waiting after this poll, so a growing or stuck outbox shows up in metrics.
        private static async Task MeasureBacklogAsync(WriteDbContext context, CancellationToken stoppingToken)
        {
            var pending = context.OutboxMessages.Where(m => m.ProcessedAt == null);
            var count = await pending.LongCountAsync(stoppingToken);
            var oldest = count == 0 ? null : await pending.MinAsync(m => (DateTime?)m.OccurredAt, stoppingToken);

            AppMetrics.OutboxBacklog(count, oldest);
        }
    }
}
