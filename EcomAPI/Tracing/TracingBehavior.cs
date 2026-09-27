using System.Diagnostics;
using MediatR;

namespace EcomAPI.Tracing
{
    // Wraps every MediatR command/query in a span named after the request type, so handlers
    // show up in traces and can tag it (e.g. cache hit/miss) through Activity.Current.
    public class TracingBehavior<TRequest, TResponse> : IPipelineBehavior<TRequest, TResponse>
        where TRequest : notnull
    {
        private static readonly string SpanName = typeof(TRequest).Name;

        public async Task<TResponse> Handle(TRequest request, RequestHandlerDelegate<TResponse> next, CancellationToken cancellationToken)
        {
            using var activity = Telemetry.Source.StartActivity(SpanName);

            try
            {
                return await next(cancellationToken);
            }
            catch (Exception ex)
            {
                activity.RecordException(ex);
                throw;
            }
        }
    }
}
