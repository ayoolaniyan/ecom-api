using System.Text.Json;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace EcomAPI.Health
{
    // Writes the health report as JSON with one entry per check, instead of the default plain-text status.
    public static class HealthCheckResponseWriter
    {
        private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web) { WriteIndented = true };

        public static Task WriteAsync(HttpContext context, HealthReport report)
        {
            context.Response.ContentType = "application/json";

            var body = new
            {
                status = report.Status.ToString(),
                durationMs = report.TotalDuration.TotalMilliseconds,
                checks = report.Entries.ToDictionary(
                    e => e.Key,
                    e => new
                    {
                        status = e.Value.Status.ToString(),
                        description = e.Value.Description,
                        durationMs = e.Value.Duration.TotalMilliseconds,
                        data = e.Value.Data
                    })
            };

            return context.Response.WriteAsync(JsonSerializer.Serialize(body, JsonOptions));
        }
    }
}
