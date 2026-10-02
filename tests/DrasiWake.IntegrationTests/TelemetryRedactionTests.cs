using System.Diagnostics;
using System.Diagnostics.Metrics;
using DrasiWake.Core.Domain;
using DrasiWake.Core.Pipeline;

namespace DrasiWake.IntegrationTests;

public sealed class TelemetryRedactionTests
{
    [Fact]
    public void Activities_and_metrics_do_not_expose_query_or_wake_secrets()
    {
        const string secret = "customer-identity-and-private-payload";
        var query = new QueryIdentity(new Uri($"https://user:{secret}@drasi.test/{secret}?token={secret}"), secret, secret);
        var item = new WakeOutboxItem(
            Guid.NewGuid(), secret, secret, secret, secret, new System.Text.Json.Nodes.JsonObject { ["secret"] = secret },
            "1.0.0", secret, 0, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow, WakeOutboxStatus.Pending, null, secret);
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == BridgeTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData
        };
        ActivitySource.AddActivityListener(activityListener);
        var metricTags = new List<KeyValuePair<string, object?>>();
        using var meterListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == BridgeTelemetry.MeterName)
                    listener.EnableMeasurementEvents(instrument);
            }
        };
        meterListener.SetMeasurementEventCallback<long>((_, _, tags, _) => metricTags.AddRange(tags.ToArray()));
        meterListener.Start();

        using var activity = BridgeTelemetry.StartReconciliation(query);
        BridgeTelemetry.RecordRetry(item, "dispatch.transient_failure");

        Assert.NotNull(activity);
        Assert.All(activity!.Tags, tag => Assert.DoesNotContain(secret, tag.Value, StringComparison.Ordinal));
        Assert.NotEmpty(metricTags);
        Assert.All(metricTags, tag => Assert.DoesNotContain(secret, tag.Value?.ToString(), StringComparison.Ordinal));
        Assert.Equal(BridgeTelemetry.StableId(secret), metricTags.Single(tag => tag.Key == "binding.id").Value);
    }
}