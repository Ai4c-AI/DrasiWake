using System.Net.Http;
using DrasiWake.Core.Abstractions;

namespace DrasiWake.Core.Tests;

public class BoundaryContractTests
{
    [Fact]
    public void Change_source_exposes_no_http_or_drasi_types()
    {
        var signature = typeof(IChangeSource).GetMethods().SelectMany(method => method.GetParameters())
            .Select(parameter => parameter.ParameterType);

        Assert.DoesNotContain(signature, type => type == typeof(HttpRequestMessage));
    }
}