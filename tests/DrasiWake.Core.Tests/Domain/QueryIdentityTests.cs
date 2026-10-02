using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Tests.Domain;

public sealed class QueryIdentityTests
{
    [Fact]
    public void Server_uri_is_normalized()
    {
        var identity = new QueryIdentity(new Uri("HTTP://Example.COM:80/drasi/"), " default ", " orders ");

        Assert.Equal(new Uri("http://example.com/drasi"), identity.Server);
        Assert.Equal("default", identity.InstanceId);
        Assert.Equal("orders", identity.QueryId);
    }
}