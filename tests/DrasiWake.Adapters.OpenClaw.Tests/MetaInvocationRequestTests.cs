using System.Net;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using DrasiWake.Adapters.OpenClaw;
using DrasiWake.Core.Domain;

namespace DrasiWake.Adapters.OpenClaw.Tests;

public sealed class MetaInvocationRequestTests
{
    [Fact]
    public async Task Sends_explicit_skill_session_input_and_existing_key()
    {
        var acceptedAt = DateTimeOffset.Parse("2026-10-02T12:34:56Z");
        var handler = new RecordingHandler(new HttpResponseMessage(HttpStatusCode.Accepted)
        {
            Content = new StringContent(AcceptedResponseJson, Encoding.UTF8, "application/json")
        });
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://gateway.test/") };
        var client = CreateClient(httpClient);
        var request = new WakeRequest(
            "binding-1",
            "session-1",
            "triage-order",
            JsonNode.Parse("""{"ticket":"INC-42"}""")!.AsObject(),
            "drasiwake:outbox-42",
            "1.0",
            null,
            "sample-gateway");

        var acceptance = await client.InvokeAsync(request, CancellationToken.None);

        Assert.Equal("/api/integration/meta-invocations", handler.LastPath);
        Assert.Equal("drasiwake:outbox-42", handler.LastIdempotencyKey);
        Assert.NotNull(handler.LastBody);
        Assert.Equal("triage-order", handler.LastBody.Value.GetProperty("skill").GetString());
        Assert.Equal("session-1", handler.LastBody.Value.GetProperty("sessionId").GetString());
        Assert.Equal("{\"ticket\":\"INC-42\"}", handler.LastBody.Value.GetProperty("input").GetString());
        Assert.Equal("3a1d3589-2856-4f6b-b572-9f9e060db134", acceptance.InvocationId);
        Assert.Equal(acceptedAt, acceptance.AcceptedAtUtc);
    }

    [Fact]
    public async Task Status_lookup_replays_the_original_request_and_key()
    {
        var handler = new RecordingHandler(new HttpResponseMessage(HttpStatusCode.Accepted)
        {
            Content = new StringContent(AcceptedResponseJson, Encoding.UTF8, "application/json")
        });
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://gateway.test/") };
        var client = CreateClient(httpClient);
        var request = new WakeRequest(
            "binding-1",
            "session-1",
            "triage-order",
            JsonNode.Parse("""{"ticket":"INC-42"}""")!.AsObject(),
            "drasiwake:outbox-42",
            "1.0",
            null,
            "sample-gateway");

        var status = await client.GetStatusAsync(request, CancellationToken.None);

        Assert.Equal("/api/integration/meta-invocations", handler.LastPath);
        Assert.Equal("drasiwake:outbox-42", handler.LastIdempotencyKey);
        Assert.Equal("Running", status!.State);
        Assert.Equal("3a1d3589-2856-4f6b-b572-9f9e060db134", status.InvocationId);
    }

    [Fact]
    public async Task Retries_after_gateway_accepted_timeout_with_the_same_key()
    {
        var handler = new ScriptedHandler((attempt, _, _) => attempt == 1
            ? Task.FromException<HttpResponseMessage>(new TaskCanceledException("Gateway response timed out."))
            : Task.FromResult(AcceptedResponse()));
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://gateway.test/") };
        var client = CreateClient(httpClient, new OpenClawOptions
        {
            MaxRetryAttempts = 1,
            RetryBaseDelay = TimeSpan.Zero
        });

        await client.InvokeAsync(CreateRequest(), CancellationToken.None);

        Assert.Equal(2, handler.CallCount);
        Assert.Equal(new[] { "drasiwake:outbox-42", "drasiwake:outbox-42" }, handler.IdempotencyKeys);
    }

    [Fact]
    public async Task Does_not_retry_idempotency_fingerprint_conflict()
    {
        var handler = new ScriptedHandler((_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.Conflict)
        {
            Content = new StringContent("""{"success":false,"error":"Idempotency-Key was already used for a different request."}""")
        }));
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://gateway.test/") };
        var client = CreateClient(httpClient, new OpenClawOptions
        {
            MaxRetryAttempts = 3,
            RetryBaseDelay = TimeSpan.Zero
        });

        await Assert.ThrowsAsync<OpenClawIdempotencyConflictException>(
            () => client.InvokeAsync(CreateRequest(), CancellationToken.None).AsTask());

        Assert.Equal(1, handler.CallCount);
    }

    [Fact]
    public async Task Reports_gateway_uncertain_invocation_instead_of_acceptance()
    {
        var handler = new ScriptedHandler((_, _, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.Conflict)
        {
            Content = new StringContent(
                """{"invocationId":"3a1d3589-2856-4f6b-b572-9f9e060db134","status":"Uncertain","result":null,"error":"Gateway restarted during invocation.","createdAtUtc":"2026-10-02T12:34:56Z"}""")
        }));
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://gateway.test/") };
        var client = CreateClient(httpClient, new OpenClawOptions
        {
            RetryBaseDelay = TimeSpan.Zero
        });

        await Assert.ThrowsAsync<OpenClawInvocationUncertainException>(
            () => client.InvokeAsync(CreateRequest(), CancellationToken.None).AsTask());
    }

    [Fact]
    public async Task Concurrent_duplicate_invocations_rely_on_gateway_idempotency()
    {
        var handler = new ConcurrentGatewayHandler();
        var httpClient = new HttpClient(handler) { BaseAddress = new Uri("https://gateway.test/") };
        var client = CreateClient(httpClient, new OpenClawOptions { MaxRetryAttempts = 0 });
        var request = CreateRequest();

        var acceptances = await Task.WhenAll(
            client.InvokeAsync(request, CancellationToken.None).AsTask(),
            client.InvokeAsync(request, CancellationToken.None).AsTask());

        Assert.Equal(2, handler.RequestCount);
        Assert.Equal(1, handler.ExecutionCount);
        Assert.All(acceptances, acceptance => Assert.Equal(acceptances[0].InvocationId, acceptance.InvocationId));
        var requests = handler.Requests.ToArray();
        Assert.All(requests, sent => Assert.Equal("drasiwake:outbox-42", sent.IdempotencyKey));
        Assert.Equal(requests[0].Body, requests[1].Body);
    }

    private static WakeRequest CreateRequest() => new(
        "binding-1",
        "session-1",
        "triage-order",
        JsonNode.Parse("""{"ticket":"INC-42"}""")!.AsObject(),
        "drasiwake:outbox-42",
        "1.0",
        null,
        "sample-gateway");

    private static OpenClawMetaInvocationClient CreateClient(
        HttpClient httpClient,
        OpenClawOptions? options = null)
        => new(
            httpClient,
            options ?? new OpenClawOptions(),
            new OpenClawTargetOptions(
                new Uri("https://gateway.test/"),
                null,
                TimeSpan.FromDays(30)));

    private static HttpResponseMessage AcceptedResponse() => new(HttpStatusCode.Accepted)
    {
        Content = new StringContent(AcceptedResponseJson, Encoding.UTF8, "application/json")
    };

    private static string AcceptedResponseJson => File.ReadAllText(
        Path.Combine(AppContext.BaseDirectory, "Fixtures", "accepted-running-response.json"));

    private sealed class RecordingHandler(HttpResponseMessage response) : HttpMessageHandler
    {
        public string? LastPath { get; private set; }
        public string? LastIdempotencyKey { get; private set; }
        public JsonElement? LastBody { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            LastPath = request.RequestUri?.AbsolutePath;
            LastIdempotencyKey = request.Headers.GetValues("Idempotency-Key").Single();
            LastBody = JsonDocument.Parse(await request.Content!.ReadAsStringAsync(cancellationToken))
                .RootElement
                .Clone();
            return response;
        }
    }

    private sealed class ScriptedHandler(
        Func<int, HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond) : HttpMessageHandler
    {
        private int callCount;
        public int CallCount => callCount;
        public ConcurrentQueue<string> IdempotencyKeys { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var attempt = Interlocked.Increment(ref callCount);
            IdempotencyKeys.Enqueue(request.Headers.GetValues("Idempotency-Key").Single());
            return await respond(attempt, request, cancellationToken);
        }
    }

    private sealed class ConcurrentGatewayHandler : HttpMessageHandler
    {
        private readonly TaskCompletionSource allRequestsReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly object invocationGate = new();
        private readonly Dictionary<string, string> invocations = [];
        private int requestCount;
        private int executionCount;

        public int RequestCount => requestCount;
        public int ExecutionCount => executionCount;
        public ConcurrentBag<(string IdempotencyKey, string Body)> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var key = request.Headers.GetValues("Idempotency-Key").Single();
            var body = await request.Content!.ReadAsStringAsync(cancellationToken);
            Requests.Add((key, body));

            if (Interlocked.Increment(ref requestCount) == 2)
                allRequestsReceived.TrySetResult();
            await allRequestsReceived.Task.WaitAsync(cancellationToken);

            string invocationId;
            lock (invocationGate)
            {
                if (!invocations.TryGetValue(key, out invocationId!))
                {
                    invocationId = Guid.NewGuid().ToString("D");
                    invocations.Add(key, invocationId);
                    executionCount++;
                }
            }

            var response = new HttpResponseMessage(HttpStatusCode.Accepted)
            {
                Content = new StringContent(
                    $$"""{"invocationId":"{{invocationId}}","status":"Running","result":null,"error":null,"createdAtUtc":"2026-10-02T12:34:56Z"}""",
                    Encoding.UTF8,
                    "application/json")
            };
            return response;
        }
    }
}