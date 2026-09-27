using Confluent.Kafka;
using EcomAPI.Events;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace EcomAPI.Health
{
    // Checks that the Kafka cluster answers a metadata request and that the events topic exists.
    // Registered as a singleton so the admin client (and its broker connections) is reused between probes.
    public class KafkaHealthCheck : IHealthCheck, IDisposable
    {
        private static readonly TimeSpan MetadataTimeout = TimeSpan.FromSeconds(2);

        private readonly KafkaOptions _options;
        private readonly Lazy<IAdminClient> _admin;

        public KafkaHealthCheck(IOptions<KafkaOptions> options)
        {
            _options = options.Value;
            _admin = new Lazy<IAdminClient>(() =>
                new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _options.BootstrapServers }).Build());
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            try
            {
                // GetMetadata blocks for up to the timeout, so keep it off the request thread.
                var metadata = await Task.Run(() => _admin.Value.GetMetadata(_options.Topic, MetadataTimeout), cancellationToken);
                var data = new Dictionary<string, object> { ["brokers"] = metadata.Brokers.Count };

                var topic = metadata.Topics.FirstOrDefault(t => t.Topic == _options.Topic);
                if (topic is null || topic.Error.IsError)
                    return new HealthCheckResult(context.Registration.FailureStatus,
                        $"Topic {_options.Topic} is not available: {topic?.Error.Reason ?? "missing"}", data: data);

                return HealthCheckResult.Healthy($"{metadata.Brokers.Count} broker(s) reachable", data);
            }
            catch (KafkaException ex)
            {
                return new HealthCheckResult(context.Registration.FailureStatus, ex.Error.Reason, ex);
            }
        }

        public void Dispose()
        {
            if (_admin.IsValueCreated)
                _admin.Value.Dispose();
        }
    }
}
