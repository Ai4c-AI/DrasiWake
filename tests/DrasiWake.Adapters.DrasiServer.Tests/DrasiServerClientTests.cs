using System.Net;
using System.Text;
using DrasiWake.Adapters.DrasiServer;
using DrasiWake.Core.Domain;

namespace DrasiWake.Adapters.DrasiServer.Tests;

public sealed class DrasiServerClientTests
{
    [Theory]
    [InlineData(null, "/api/v1/queries/orders/results")]
    [InlineData("tenant/a", "/api/v1/instances/tenant%2Fa/queries/orders/results")]
    public async Task ReadSnapshotAsync_uses_route_and_unwraps_results(
        string? instanceId,
        string expectedPath)
    {
        var handler = new RecordingHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(
                "{\"success\":true,\"data\":[{\"orderId\":42}],\"error\":null}",
                Encoding.UTF8,
                "application/json")
        });
        using var httpClient = new HttpClient(handler);
        var client = new DrasiServerClient(httpClient);
        var query = new QueryIdentity(new Uri("http://drasi.test/"), instanceId, "orders");

        var snapshot = await client.ReadSnapshotAsync(query, CancellationToken.None);

        Assert.Equal(expectedPath, handler.RequestUri!.PathAndQuery);
        Assert.Single(snapshot.Rows);
        Assert.Equal(42, snapshot.Rows[0]!["orderId"]!.GetValue<int>());
    }

    [Fact]
    public async Task EnumerateQueriesAsync_lists_each_instance_and_its_queries()
    {
        var handler = new RecordingHandler(request => request.RequestUri!.AbsolutePath switch
        {
            "/proxy/api/v1/instances" => JsonResponse(
                "{\"success\":true,\"data\":[{\"id\":\"east\"},{\"id\":\"west\"}],\"error\":null}"),
            "/proxy/api/v1/instances/east/queries" => JsonResponse(
                "{\"success\":true,\"data\":[{\"id\":\"orders\"}],\"error\":null}"),
            "/proxy/api/v1/instances/west/queries" => JsonResponse(
                "{\"success\":true,\"data\":[{\"id\":\"returns\"}],\"error\":null}"),
            _ => new HttpResponseMessage(HttpStatusCode.NotFound)
        });
        using var httpClient = new HttpClient(handler);
        var client = new DrasiServerClient(httpClient);
        var server = new Uri("http://drasi.test/proxy/");

        var queries = await client.EnumerateQueriesAsync(server, CancellationToken.None);

        Assert.Collection(
            queries,
            query => Assert.Equal(new QueryIdentity(server, "east", "orders"), query),
            query => Assert.Equal(new QueryIdentity(server, "west", "returns"), query));
        Assert.Equal(
            [
                "/proxy/api/v1/instances",
                "/proxy/api/v1/instances/east/queries",
                "/proxy/api/v1/instances/west/queries"
            ],
            handler.RequestPaths);
    }

    [Fact]
    public async Task AttachAsync_uses_attach_route_and_emits_connect_hint()
    {
        var handler = new RecordingHandler(_ =>
        {
            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(": keep-alive\n\n", Encoding.UTF8)
            };
            response.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("text/event-stream");
            return response;
        });
        using var httpClient = new HttpClient(handler);
        var client = new DrasiServerClient(httpClient);
        var query = new QueryIdentity(new Uri("http://drasi.test"), "east", "orders");

        var signals = new List<ChangeSignal>();
        await foreach (var signal in client.AttachAsync(query, CancellationToken.None))
        {
            signals.Add(signal);
        }

        Assert.Single(signals);
        Assert.Null(signals[0].SignalId);
        Assert.Equal("/api/v1/instances/east/queries/orders/attach", handler.RequestUri!.AbsolutePath);
        Assert.Equal("text/event-stream", handler.AcceptMediaType);
    }

    private static HttpResponseMessage JsonResponse(string json) => new(HttpStatusCode.OK)
    {
        Content = new StringContent(json, Encoding.UTF8, "application/json")
    };

    private sealed class RecordingHandler(Func<HttpRequestMessage, HttpResponseMessage> responseFactory)
        : HttpMessageHandler
    {
        public List<Uri> RequestUris { get; } = [];
        public Uri? RequestUri => RequestUris.LastOrDefault();
        public IEnumerable<string> RequestPaths => RequestUris.Select(uri => uri.AbsolutePath);
        public string? AcceptMediaType { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            RequestUris.Add(request.RequestUri!);
            AcceptMediaType = request.Headers.Accept.SingleOrDefault()?.MediaType;
            return Task.FromResult(responseFactory(request));
        }
    }
}