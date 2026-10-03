using DrasiWake.LocalEnvironment;

public sealed class ComposePortParserTests
{
    [Theory]
    [InlineData("127.0.0.1:43210", "http://127.0.0.1:43210/")]
    [InlineData("0.0.0.0:43210", "http://127.0.0.1:43210/")]
    [InlineData(":::43210", "http://127.0.0.1:43210/")]
    [InlineData("[::]:43210", "http://127.0.0.1:43210/")]
    [InlineData("[::1]:43210", "http://[::1]:43210/")]
    [InlineData("localhost:43210", "http://localhost:43210/")]
    public void Parses_single_published_mapping(string output, string expectedAddress)
    {
        var address = ComposePortParser.Parse(output, "service");

        Assert.Equal(new Uri(expectedAddress), address);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-an-address")]
    [InlineData("127.0.0.1:0")]
    [InlineData("127.0.0.1:65536")]
    [InlineData("127.0.0.1:abc")]
    [InlineData("127.0.0.1:123\n127.0.0.1:456")]
    public void Rejects_missing_invalid_or_ambiguous_mappings(string output)
    {
        var exception = Assert.Throws<FormatException>(() => ComposePortParser.Parse(output, "openclaw"));

        Assert.Contains("openclaw", exception.Message, StringComparison.Ordinal);
    }
}