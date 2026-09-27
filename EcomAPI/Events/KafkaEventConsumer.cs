using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using EcomAPI.Tracing;
using MediatR;
using Microsoft.Extensions.Options;
using OpenTelemetry;

namespace EcomAPI.Events
{
    // Consumes events from Kafka and dispatches them to the MediatR notification handlers
    // (the read-model projections). Offsets are committed only after a handler succeeds,
    // giving at-least-once delivery, so handlers must be idempotent.
    public class KafkaEventConsumer : BackgroundService
    {
        private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

        private readonly IServiceScopeFactory _scopeFactory;
        private readonly KafkaOptions _options;
        private readonly ILogger<KafkaEventConsumer> _logger;

        public KafkaEventConsumer(IServiceScopeFactory scopeFactory, IOptions<KafkaOptions> options, ILogger<KafkaEventConsumer> logger)
        {
            _scopeFactory = scopeFactory;
            _options = options.Value;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Consume() blocks, so get off the host startup path first.
            await Task.Yield();

            await EnsureTopicExistsAsync(stoppingToken);

            var config = new ConsumerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                GroupId = _options.ConsumerGroupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };

            using var consumer = new ConsumerBuilder<string, string>(config).Build();
            consumer.Subscribe(_options.Topic);
            _logger.LogInformation("Consuming {Topic} as group {GroupId}", _options.Topic, _options.ConsumerGroupId);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string> result;
                    try
                    {
                        result = consumer.Consume(stoppingToken);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Error consuming from {Topic}", _options.Topic);
                        continue;
                    }

                    await HandleAsync(consumer, result, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
            }
            finally
            {
                consumer.Close();
            }
        }

        private async Task HandleAsync(IConsumer<string, string> consumer, ConsumeResult<string, string> result, CancellationToken stoppingToken)
        {
            // Continue the trace started by the request that raised the event (via the outbox and producer).
            var parent = Telemetry.Extract(result.Message.Headers);
            Baggage.Current = parent.Baggage;
            using var activity = Telemetry.Source.StartActivity($"process {result.Topic}", ActivityKind.Consumer, parent.ActivityContext);
            activity?.SetTag("messaging.system", "kafka");
            activity?.SetTag("messaging.operation.type", "process");
            activity?.SetTag("messaging.operation.name", "process");
            activity?.SetTag("messaging.destination.name", result.Topic);
            activity?.SetTag("messaging.destination.partition.id", result.Partition.Value.ToString());
            activity?.SetTag("messaging.kafka.offset", result.Offset.Value);
            activity?.SetTag("messaging.kafka.message.key", result.Message.Key);
            activity?.SetTag("messaging.consumer.group.name", _options.ConsumerGroupId);
            if (result.Message.Headers.TryGetLastBytes(KafkaEventPublisher.MessageIdHeader, out var messageId))
                activity?.SetTag("messaging.message.id", Encoding.UTF8.GetString(messageId));

            var eventType = result.Message.Headers.TryGetLastBytes(KafkaEventPublisher.EventTypeHeader, out var bytes)
                ? Encoding.UTF8.GetString(bytes)
                : null;

            INotification? evt = null;
            try
            {
                if (eventType is not null)
                    evt = EventSerializer.Deserialize(eventType, result.Message.Value);
            }
            catch (System.Text.Json.JsonException ex)
            {
                activity.RecordException(ex);
                _logger.LogError(ex, "Malformed {EventType} payload at {TopicPartitionOffset}", eventType, result.TopicPartitionOffset);
            }

            if (evt is null)
            {
                // Retrying will never succeed, so skip rather than block the partition.
                activity?.SetStatus(ActivityStatusCode.Error, "Unknown or malformed event; skipped");
                activity?.SetTag("messaging.message.skipped", true);
                _logger.LogWarning("Skipping message at {TopicPartitionOffset} with event type {EventType}", result.TopicPartitionOffset, eventType);
                consumer.Commit(result);
                return;
            }

            try
            {
                using var scope = _scopeFactory.CreateScope();
                var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();
                await mediator.Publish(evt, stoppingToken);

                consumer.Commit(result);
                _logger.LogInformation("Handled {EventType} from {TopicPartitionOffset}", eventType, result.TopicPartitionOffset);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Rewind so the same message is delivered again on the next Consume().
                activity.RecordException(ex);
                activity?.SetTag("messaging.message.retried", true);
                _logger.LogError(ex, "Failed to handle {EventType} from {TopicPartitionOffset}; retrying in {Delay}",
                    eventType, result.TopicPartitionOffset, RetryDelay);
                consumer.Seek(result.TopicPartitionOffset);
                activity?.Stop(); // Keep the back-off out of the span's duration.
                await Task.Delay(RetryDelay, stoppingToken);
            }
        }

        private async Task EnsureTopicExistsAsync(CancellationToken stoppingToken)
        {
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _options.BootstrapServers }).Build();

            while (true)
            {
                try
                {
                    await admin.CreateTopicsAsync(
                    [
                        new TopicSpecification
                        {
                            Name = _options.Topic,
                            NumPartitions = _options.TopicPartitions,
                            ReplicationFactor = _options.TopicReplicationFactor
                        }
                    ]);
                    _logger.LogInformation("Created topic {Topic}", _options.Topic);
                    return;
                }
                catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
                {
                    return;
                }
                catch (KafkaException ex)
                {
                    _logger.LogWarning("Could not create topic {Topic} ({Reason}); retrying in {Delay}", _options.Topic, ex.Error.Reason, RetryDelay);
                    await Task.Delay(RetryDelay, stoppingToken);
                }
            }
        }
    }
}
