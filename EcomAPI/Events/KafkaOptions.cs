namespace EcomAPI.Events
{
    public class KafkaOptions
    {
        public const string SectionName = "Kafka";

        public string BootstrapServers { get; set; } = "localhost:9094";

        public string Topic { get; set; } = "orders.order-created";

        public string ConsumerGroupId { get; set; } = "ecomapi-read-projection";

        public int TopicPartitions { get; set; } = 3;

        public short TopicReplicationFactor { get; set; } = 1;
    }
}
