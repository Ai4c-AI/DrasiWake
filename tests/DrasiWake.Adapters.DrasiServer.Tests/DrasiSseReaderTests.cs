using System.Text;
using DrasiWake.Adapters.DrasiServer;
using DrasiWake.Core.Domain;

namespace DrasiWake.Adapters.DrasiServer.Tests;

public sealed class DrasiSseReaderTests
{
    [Fact]
    public async Task ReadAsync_emits_signal_for_json_data_and_ignores_keep_alive()
    {
        var query = new QueryIdentity(new Uri("http://localhost:8080"), null, "orders");
        await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(
            ": keep-alive\n\n" +
            "event: heartbeat\n\n" +
            "data: {\"result\":{\"orderId\":\"42\"}}\n\n"));

        var signals = new List<ChangeSignal>();
        await foreach (var emittedSignal in DrasiSseReader.ReadAsync(stream, query, CancellationToken.None))
        {
            signals.Add(emittedSignal);
        }

        var signal = Assert.Single(signals);
        Assert.Equal(query, signal.Query);
    }

    [Fact]
    public async Task ReadAsync_ignores_malformed_frame_and_emits_following_multiline_json()
    {
        var query = new QueryIdentity(new Uri("http://localhost:8080"), null, "orders");
        await using var stream = new MemoryStream(Encoding.UTF8.GetBytes(
            "data: {invalid}\n\n" +
            "id: result-7\n" +
            "data: {\"result\":\n" +
            "data: {\"orderId\":\"42\"}}\n\n"));

        var signals = new List<ChangeSignal>();
        await foreach (var signal in DrasiSseReader.ReadAsync(stream, query, CancellationToken.None))
        {
            signals.Add(signal);
        }

        var emittedSignal = Assert.Single(signals);
        Assert.Equal(query, emittedSignal.Query);
        Assert.Equal("result-7", emittedSignal.SignalId);
    }
}