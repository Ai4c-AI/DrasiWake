using System.Collections.Concurrent;
using System.Net;
using System.Text;
using DrasiWake.Adapters.DrasiServer;
using DrasiWake.Core.Domain;

namespace DrasiWake.Adapters.DrasiServer.Tests;

public sealed class DrasiChangeSourceTests
{
    [Fact]
    public async Task WatchAsync_shares_attach_connection_for_duplicate_query_identity()
    {
        var handler = new DrasiHandler();
        using var httpClient = new HttpClient(handler);
        var server = new Uri("http://drasi.test");
        var source = new DrasiChangeSource(
            new DrasiServerClient(httpClient),
            new DrasiServerOptions(server)
            {
                InitialReconnectDelay = TimeSpan.FromSeconds(2),
                MaxReconnectDelay = TimeSpan.FromSeconds(2)
            });
        await using var signals = source.WatchAsync(TestContext.Current.CancellationToken)
            .GetAsyncEnumerator(TestContext.Current.CancellationToken);

        Assert.True(await signals.MoveNextAsync());
        Assert.Equal(new QueryIdentity(server, "east", "orders"), signals.Current.Query);
        Assert.Single(handler.AttachRequests);

        await signals.DisposeAsync();
    }

    [Fact]
    public async Task WatchAsync_reconnects_and_emits_another_reconciliation_hint()
    {
        var handler = new DrasiHandler();
        using var httpClient = new HttpClient(handler);
        var server = new Uri("http://drasi.test");
        var source = new DrasiChangeSource(
            new DrasiServerClient(httpClient),
            new DrasiServerOptions(server)
            {
                InitialReconnectDelay = TimeSpan.FromMilliseconds(10),
                MaxReconnectDelay = TimeSpan.FromMilliseconds(10)
            });
        await using var signals = source.WatchAsync(TestContext.Current.CancellationToken)
            .GetAsyncEnumerator(TestContext.Current.CancellationToken);

        Assert.True(await signals.MoveNextAsync());
        Assert.True(await signals.MoveNextAsync().AsTask().WaitAsync(
            TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        Assert.Equal(new QueryIdentity(server, "east", "orders"), signals.Current.Query);
        Assert.True(handler.AttachRequests.Count >= 2);
    }

    [Fact]
    public async Task WatchAsync_cancellation_stops_reconnect_worker()
    {
        var handler = new DrasiHandler();
        using var httpClient = new HttpClient(handler);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        var server = new Uri("http://drasi.test");
        var source = new DrasiChangeSource(
            new DrasiServerClient(httpClient),
            new DrasiServerOptions(server)
            {
                InitialReconnectDelay = TimeSpan.FromSeconds(30),
                MaxReconnectDelay = TimeSpan.FromSeconds(30)
            });
        await using var signals = source.WatchAsync(cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);

        Assert.True(await signals.MoveNextAsync());
        var pendingSignal = signals.MoveNextAsync().AsTask();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pendingSignal);
        Assert.Single(handler.AttachRequests);
    }

    private sealed class DrasiHandler : HttpMessageHandler
    {
        public ConcurrentQueue<string> AttachRequests { get; } = new();

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.AbsolutePath;
            var json = path switch
            {
                "/api/v1/instances" => "{\"success\":true,\"data\":[{\"id\":\"east\"}],\"error\":null}",
                "/api/v1/instances/east/queries" => "{\"success\":true,\"data\":[{\"id\":\"orders\"},{\"id\":\"orders\"}],\"error\":null}",
                _ => string.Empty
            };

            if (path.EndsWith("/attach", StringComparison.Ordinal))
            {
                AttachRequests.Enqueue(path);
                var streamResponse = new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent(": keep-alive\n\n", Encoding.UTF8)
                };
                streamResponse.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("text/event-stream");
                return Task.FromResult(streamResponse);
            }

            var response = new HttpResponseMessage(string.IsNullOrEmpty(json) ? HttpStatusCode.NotFound : HttpStatusCode.OK)
            {
                Content = new StringContent(json, Encoding.UTF8, "application/json")
            };
            return Task.FromResult(response);
        }
    }
}