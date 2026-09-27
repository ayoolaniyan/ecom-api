using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace EcomAPI.Health
{
    // Round-trips a read through the registered IDistributedCache (Redis), using the same
    // connection and timeouts as the HybridCache L2.
    public class DistributedCacheHealthCheck : IHealthCheck
    {
        private const string ProbeKey = "healthz";

        private readonly IDistributedCache _cache;

        public DistributedCacheHealthCheck(IDistributedCache cache)
        {
            _cache = cache;
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            try
            {
                await _cache.GetAsync(ProbeKey, cancellationToken);
                return HealthCheckResult.Healthy();
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return new HealthCheckResult(context.Registration.FailureStatus, ex.Message, ex);
            }
        }
    }
}
