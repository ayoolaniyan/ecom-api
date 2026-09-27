namespace EcomAPI.Caching
{
    public class CacheOptions
    {
        public const string SectionName = "Cache";

        // "Redis" (in-memory L1 + Redis L2) or "Memory" (in-memory only, no Redis needed).
        public string Provider { get; set; } = "Memory";

        public string RedisConnection { get; set; } = "localhost:6379";

        public string InstanceName { get; set; } = "ecomapi:";

        // Total lifetime of an entry (Redis TTL when Provider = Redis).
        public int OrderTtlSeconds { get; set; } = 300;

        public int SummariesTtlSeconds { get; set; } = 30;

        // Lifetime of the per-instance in-memory copy. Keep it short: invalidations only
        // clear the local copy on the instance that performs them.
        public int LocalTtlSeconds { get; set; } = 10;
    }
}
