using System.Text.Json;
using System.Text.Json.Nodes;
using DrasiWake.Adapters.OpenClaw;
using DrasiWake.Core.Domain;
using DrasiWake.IntegrationTests.Fixtures;

namespace DrasiWake.IntegrationTests;

public sealed class GatewayIdempotencyTests
{
    [Fact]
    public async Task Concurrent_same_key_requests_map_meta_skill_and_create_one_invocation()
    {
        await using var fixture = await BridgeTestFixture.CreateAsync(TestContext.Current.CancellationToken);
        var request = CreateRequest("drasiwake:concurrent-contract");

        var results = await Task.WhenAll(
            fixture.GatewayClient.InvokeAsync(request, TestContext.Current.CancellationToken).AsTask(),
            fixture.GatewayClient.InvokeAsync(request, TestContext.Current.CancellationToken).AsTask());

        Assert.Equal(results[0].InvocationId, results[1].InvocationId);
        Assert.Equal(1, fixture.GatewayHandler.ExecutionCount);
        var sent = fixture.GatewayHandler.Requests.ToArray();
        Assert.Equal(2, sent.Length);
        Assert.All(sent, item => Assert.Equal(request.IdempotencyKey, item.Key));
        Assert.Equal(sent[0].Body, sent[1].Body);
        using var body = JsonDocument.Parse(sent[0].Body);
        Assert.Equal("triage-order", body.RootElement.GetProperty("skill").GetString());
        Assert.Equal("singleton", body.RootElement.GetProperty("sessionId").GetString());
        using var input = JsonDocument.Parse(body.RootElement.GetProperty("input").GetString()!);
        Assert.Equal("order-42", input.RootElement.GetProperty("facts")[0].GetProperty("orderId").GetString());
    }

    [Fact]
    public async Task Reusing_key_with_a_different_request_is_rejected_without_a_second_invocation()
    {
        await using var fixture = await BridgeTestFixture.CreateAsync(TestContext.Current.CancellationToken);
        var request = CreateRequest("drasiwake:conflicting-contract");
        await fixture.GatewayClient.InvokeAsync(request, TestContext.Current.CancellationToken);
        var conflictingRequest = request with
        {
            Input = new JsonObject
            {
                ["facts"] = new JsonArray(new JsonObject
                {
                    ["orderId"] = "order-99",
                    ["state"] = "completed"
                })
            }
        };

        await Assert.ThrowsAsync<OpenClawIdempotencyConflictException>(() =>
            fixture.GatewayClient.InvokeAsync(conflictingRequest, TestContext.Current.CancellationToken).AsTask());

        Assert.Equal(1, fixture.GatewayHandler.ExecutionCount);
    }

    private static WakeRequest CreateRequest(string idempotencyKey) => new(
        "orders-binding",
        "singleton",
        "triage-order",
        new JsonObject
        {
            ["facts"] = new JsonArray(new JsonObject
            {
                ["orderId"] = "order-42",
                ["state"] = "new"
            })
        },
        idempotencyKey,
        "1.0.0",
        null);
}