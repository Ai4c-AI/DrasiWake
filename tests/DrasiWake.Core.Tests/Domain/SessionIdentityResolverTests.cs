using System.Text.Json.Nodes;
using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Tests.Domain;

public sealed class SessionIdentityResolverTests
{
    [Fact]
    public void Missing_key_is_rejected_not_singleton()
    {
        var resolver = new SessionIdentityResolver();
        var result = resolver.Resolve(CreateBinding(), JsonNode.Parse("{}")!.AsObject());

        Assert.Equal("identity.key_missing", result.ErrorCode);
        Assert.Null(result.Identity);
    }

    [Fact]
    public void Per_query_scope_keeps_same_key_isolated_between_queries()
    {
        var resolver = new SessionIdentityResolver();
        var row = JsonNode.Parse("{\"orderId\":\"42\"}")!.AsObject();

        var first = resolver.Resolve(CreateBinding(), row);
        var second = resolver.Resolve(CreateBinding() with { QueryId = "other-orders" }, row);

        Assert.NotEqual(first.Identity, second.Identity);
    }

    [Fact]
    public void Shared_scope_reuses_complete_canonical_identity_across_queries()
    {
        var resolver = new SessionIdentityResolver();
        var row = JsonNode.Parse("{\"orderId\":\"42\"}")!.AsObject();
        var sharedBinding = CreateBinding() with
        {
            SessionScope = "shared-canonical",
            CanonicalIdentity = new CanonicalIdentityDefinition("urn:orders", "Order", "/orderId")
        };

        var first = resolver.Resolve(sharedBinding, row);
        var second = resolver.Resolve(sharedBinding with { QueryId = "other-orders" }, row);

        Assert.True(first.IsSuccess);
        Assert.Equal(first.Identity, second.Identity);
    }

    [Fact]
    public void Shared_scope_without_complete_identity_is_rejected()
    {
        var resolver = new SessionIdentityResolver();
        var binding = CreateBinding() with { SessionScope = "shared-canonical", CanonicalIdentity = null };
        var row = JsonNode.Parse("{\"orderId\":\"42\"}")!.AsObject();

        var result = resolver.Resolve(binding, row);

        Assert.Equal("identity.required", result.ErrorCode);
        Assert.Null(result.Identity);
    }

    private static BridgeBinding CreateBinding() => new(
        "orders",
        "drasi-server",
        new Uri("http://localhost:8080"),
        null,
        "order-changes",
        "converge-latest",
        "per-query",
        "/orderId",
        null,
        "sample-gateway",
        "triage-order",
        new BridgeContract("1.0.0", "order.schema.json"),
        32768,
        Array.Empty<string>(),
        new RetryPolicy(5, TimeSpan.FromDays(1)),
        new RateLimitPolicy(10, TimeSpan.FromSeconds(1)));
}