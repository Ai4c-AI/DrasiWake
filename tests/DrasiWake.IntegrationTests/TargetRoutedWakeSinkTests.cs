using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using DrasiWake.Adapters.OpenClaw;
using DrasiWake.Core.Domain;
using DrasiWake.Host;

namespace DrasiWake.IntegrationTests;

public sealed class TargetRoutedWakeSinkTests
{
    [Fact]
    public async Task Invocation_and_status_replay_use_the_persisted_target_configuration()
    {
        var alphaHandler = new RecordingHandler();
        var betaHandler = new RecordingHandler();
        var factory = new RecordingHttpClientFactory(new Dictionary<string, HttpMessageHandler>(StringComparer.Ordinal)
        {
            ["alpha"] = alphaHandler,
            ["beta"] = betaHandler
        });
        using var sink = CreateSink(factory);
        var alphaRequest = CreateRequest("alpha");
        var betaRequest = CreateRequest("beta");

        await sink.InvokeAsync(alphaRequest, CancellationToken.None);
        var alphaStatus = await sink.GetStatusAsync(alphaRequest, CancellationToken.None);
        await sink.InvokeAsync(betaRequest, CancellationToken.None);
        var betaStatus = await sink.GetStatusAsync(betaRequest, CancellationToken.None);

        Assert.Equal("Running", alphaStatus!.State);
        Assert.Equal("Running", betaStatus!.State);
        Assert.Equal(2, alphaHandler.Requests.Count);
        Assert.Equal(2, betaHandler.Requests.Count);
        Assert.All(alphaHandler.Requests, request =>
        {
            Assert.Equal(new Uri("https://alpha.gateway.test/api/integration/meta-invocations"), request.Uri);
            Assert.Equal("Bearer alpha-secret", request.Authorization);
            Assert.Equal("drasiwake:outbox-42", request.IdempotencyKey);
            Assert.Equal("triage-order", request.Skill);
            Assert.Equal("session-1", request.SessionId);
            Assert.Equal("{\"ticket\":\"INC-42\"}", request.Input);
        });
        Assert.All(betaHandler.Requests, request =>
        {
            Assert.Equal(new Uri("https://beta.gateway.test/api/integration/meta-invocations"), request.Uri);
            Assert.Equal("Bearer beta-secret", request.Authorization);
            Assert.Equal("drasiwake:outbox-42", request.IdempotencyKey);
            Assert.Equal("triage-order", request.Skill);
            Assert.Equal("session-1", request.SessionId);
            Assert.Equal("{\"ticket\":\"INC-42\"}", request.Input);
        });
        Assert.DoesNotContain(alphaHandler.Requests, request => request.Authorization == "Bearer beta-secret");
        Assert.DoesNotContain(betaHandler.Requests, request => request.Authorization == "Bearer alpha-secret");
        Assert.Equal(new[] { "alpha", "beta" }, factory.CreatedNames.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task Unknown_target_fails_without_creating_an_http_client()
    {
        var factory = new RecordingHttpClientFactory(new Dictionary<string, HttpMessageHandler>(StringComparer.Ordinal));
        using var sink = CreateSink(factory);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => sink.InvokeAsync(CreateRequest("missing"), CancellationToken.None).AsTask());

        Assert.Empty(factory.CreatedNames);
    }

    private static TargetRoutedWakeSink CreateSink(RecordingHttpClientFactory factory)
        => new(
            factory,
            new OpenClawOptions { MaxRetryAttempts = 0, RetryBaseDelay = TimeSpan.Zero },
            new Dictionary<string, OpenClawTargetOptions>(StringComparer.Ordinal)
            {
                ["alpha"] = new OpenClawTargetOptions(
                    new Uri("https://alpha.gateway.test/"),
                    "alpha-secret",
                    TimeSpan.FromDays(30)),
                ["beta"] = new OpenClawTargetOptions(
                    new Uri("https://beta.gateway.test/"),
                    "beta-secret",
                    TimeSpan.FromDays(30))
            });

    private static WakeRequest CreateRequest(string target) => new(
        "binding-1",
        "session-1",
        "triage-order",
        JsonNode.Parse("""{"ticket":"INC-42"}""")!.AsObject(),
        "drasiwake:outbox-42",
        "1.0",
        null,
        target);

    private sealed class RecordingHttpClientFactory(IReadOnlyDictionary<string, HttpMessageHandler> handlers)
        : IHttpClientFactory
    {
        private readonly ConcurrentQueue<string> createdNames = new();

        public string[] CreatedNames => createdNames.ToArray();

        public HttpClient CreateClient(string name)
        {
            createdNames.Enqueue(name);
            return new HttpClient(handlers[name], disposeHandler: false);
        }
    }

    private sealed class RecordingHandler : HttpMessageHandler
    {
        private const string ResponseJson =
            """{"invocationId":"3a1d3589-2856-4f6b-b572-9f9e060db134","status":"Running","error":null,"createdAtUtc":"2026-10-02T12:34:56Z"}""";

        public List<RecordedRequest> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            using var body = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(cancellationToken));
            Requests.Add(new RecordedRequest(
                request.RequestUri!,
                request.Headers.Authorization?.ToString(),
                request.Headers.GetValues("Idempotency-Key").Single(),
                body.RootElement.GetProperty("skill").GetString()!,
                body.RootElement.GetProperty("sessionId").GetString()!,
                body.RootElement.GetProperty("input").GetString()!));

            return new HttpResponseMessage(HttpStatusCode.Accepted)
            {
                Content = new StringContent(ResponseJson, Encoding.UTF8, "application/json")
            };
        }
    }

    private sealed record RecordedRequest(
        Uri Uri,
        string? Authorization,
        string IdempotencyKey,
        string Skill,
        string SessionId,
        string Input);
}
