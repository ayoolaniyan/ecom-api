using System.Text;
using Confluent.Kafka;
using EcomAPI.Outbox;
using Microsoft.Extensions.Options;

namespace EcomAPI.Events
{
    public class KafkaEventPublisher : IEventPublisher, IDisposable
    {
        public const string EventTypeHeader = "event-type";
        public const string MessageIdHeader = "message-id";

        private readonly IProducer<string, string> _producer;
        private readonly KafkaOptions _options;
        private readonly ILogger<KafkaEventPublisher> _logger;

        public KafkaEventPublisher(IOptions<KafkaOptions> options, ILogger<KafkaEventPublisher> logger)
        {
            _options = options.Value;
            _logger = logger;

            var config = new ProducerConfig
            {
                BootstrapServers = _options.BootstrapServers,
                Acks = Acks.All,
                EnableIdempotence = true,
                // Fail fast so the outbox dispatcher can record the error and retry on its next poll.
                MessageTimeoutMs = 30_000
            };

            _producer = new ProducerBuilder<string, string>(config).Build();
        }

        public async Task PublishAsync(OutboxMessage message, CancellationToken cancellationToken = default)
        {
            // Keying by aggregate id keeps all events for one order on the same partition, in order.
            var kafkaMessage = new Message<string, string>
            {
                Key = message.Key,
                Value = message.Payload,
                Headers = new Headers
                {
                    { EventTypeHeader, Encoding.UTF8.GetBytes(message.Type) },
                    { MessageIdHeader, Encoding.UTF8.GetBytes(message.Id.ToString()) }
                }
            };

            var result = await _producer.ProduceAsync(_options.Topic, kafkaMessage, cancellationToken);

            _logger.LogInformation("Published {EventType} (outbox {MessageId}) to {TopicPartitionOffset}",
                message.Type, message.Id, result.TopicPartitionOffset);
        }

        public void Dispose()
        {
            _producer.Flush(TimeSpan.FromSeconds(10));
            _producer.Dispose();
        }
    }
}
