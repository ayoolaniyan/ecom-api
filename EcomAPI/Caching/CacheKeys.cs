namespace EcomAPI.Caching
{
    public static class CacheKeys
    {
        public const string OrderSummaries = "orders:summaries";

        public static string Order(int orderId) => $"order:{orderId}";
    }
}
