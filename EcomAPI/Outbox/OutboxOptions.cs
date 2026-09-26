namespace EcomAPI.Outbox
{
    public class OutboxOptions
    {
        public const string SectionName = "Outbox";

        public int PollingIntervalMs { get; set; } = 1000;

        public int BatchSize { get; set; } = 50;
    }
}
