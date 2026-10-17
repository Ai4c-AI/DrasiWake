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

    [Fact]
    public async Task WatchAsync_counts_visible_queries_and_connected_streams()
    {
        var handler = new HeldAttachHandler();
        using var httpClient = new HttpClient(handler);
        var server = new Uri("http://drasi.test");
        var source = new DrasiChangeSource(
            new DrasiServerClient(httpClient),
            new DrasiServerOptions(server)
            {
                InitialReconnectDelay = TimeSpan.FromSeconds(30),
                MaxReconnectDelay = TimeSpan.FromSeconds(30)
            });
        Assert.Equal(0, source.VisibleQueries);
        Assert.Equal(0, source.ConnectedQueries);

        using var watchCancellation = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);
        await using var signals = source.WatchAsync(watchCancellation.Token)
            .GetAsyncEnumerator(watchCancellation.Token);
        var pendingSignal = signals.MoveNextAsync().AsTask();
        await WaitForAsync(() => source.ConnectedQueries == 1, TimeSpan.FromSeconds(5));

        Assert.Equal(1, source.VisibleQueries);
        Assert.Equal(1, source.ConnectedQueries);

        // Attaching may deliver an initial reconciliation hint — drain whatever arrived.
        try
        {
            await pendingSignal.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        }
        catch (OperationCanceledException)
        {
        }

        handler.Release();
        await WaitForAsync(() => source.ConnectedQueries == 0, TimeSpan.FromSeconds(5));
        Assert.Equal(1, source.VisibleQueries);

        await watchCancellation.CancelAsync();
        // The watch loop may either observe cancellation directly (OCE) or see its workers
        // finish first and complete normally — both paths must reset the counters.
        try
        {
            Assert.False(await signals.MoveNextAsync().AsTask().WaitAsync(
                TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        }
        catch (OperationCanceledException)
        {
        }

        Assert.Equal(0, source.VisibleQueries);
        Assert.Equal(0, source.ConnectedQueries);
        await signals.DisposeAsync();
    }

    private static async Task WaitForAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (!condition())
        {
            if (DateTimeOffset.UtcNow >= deadline)
                throw new TimeoutException("The expected change source state was not reached in time.");
            await Task.Delay(TimeSpan.FromMilliseconds(20), TestContext.Current.CancellationToken);
        }
    }

    private sealed class HeldAttachHandler : HttpMessageHandler
    {
        private readonly TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Release() => release.TrySetResult();

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var path = request.RequestUri!.AbsolutePath;
            if (path.EndsWith("/attach", StringComparison.Ordinal))
            {
                var response = new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StreamContent(new HeldStream(release.Task, cancellationToken))
                };
                response.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("text/event-stream");
                return Task.FromResult(response);
            }

            var json = path switch
            {
                "/api/v1/instances" => "{\"success\":true,\"data\":[{\"id\":\"east\"}],\"error\":null}",
                "/api/v1/instances/east/queries" => "{\"success\":true,\"data\":[{\"id\":\"orders\"}],\"error\":null}",
                _ => string.Empty
            };
            var status = string.IsNullOrEmpty(json) ? HttpStatusCode.NotFound : HttpStatusCode.OK;
            return Task.FromResult(new HttpResponseMessage(status)
            {
                Content = new StringContent(json, Encoding.UTF8, "application/json")
            });
        }
    }

    private sealed class HeldStream(Task release, CancellationToken cancellationToken) : Stream
    {
        private bool released;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
        {
            if (!released)
            {
                await release.WaitAsync(cancellationToken).ConfigureAwait(false);
                released = true;
            }

            return 0;
        }

        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
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