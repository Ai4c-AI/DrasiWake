using DrasiWake.LocalEnvironment;

public sealed class ComposeCommandExecutorTests
{
    [Fact]
    public async Task Uses_child_environment_and_redacts_secret_from_output()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        const string secret = "provider-secret-value";
        var command = new ComposeCommand(
            Environment.GetEnvironmentVariable("ComSpec") ?? "cmd.exe",
            ["/c", "echo %MODEL_PROVIDER_KEY%"],
            Environment.CurrentDirectory,
            new Dictionary<string, string> { ["MODEL_PROVIDER_KEY"] = secret },
            [secret],
            TimeSpan.FromSeconds(5));

        var result = await new ComposeCommandExecutor().ExecuteAsync(
            command,
            TestContext.Current.CancellationToken);

        Assert.Equal(0, result.ExitCode);
        Assert.Contains("[REDACTED]", result.StandardOutput, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, string.Join(" ", command.Arguments), StringComparison.Ordinal);
        Assert.Equal(secret, command.Environment["MODEL_PROVIDER_KEY"]);
    }

    [Fact]
    public async Task Timeout_diagnostic_does_not_include_environment_values()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        const string secret = "gateway-secret-value";
        var command = new ComposeCommand(
            Environment.GetEnvironmentVariable("ComSpec") ?? "cmd.exe",
            ["/c", "ping -n 15 127.0.0.1 > nul"],
            Environment.CurrentDirectory,
            new Dictionary<string, string> { ["OPENCLAW_AUTH_TOKEN"] = secret },
            [secret],
            TimeSpan.FromMilliseconds(100));

        var exception = await Assert.ThrowsAsync<TimeoutException>(() =>
            new ComposeCommandExecutor().ExecuteAsync(command, TestContext.Current.CancellationToken));

        Assert.DoesNotContain(secret, exception.ToString(), StringComparison.Ordinal);
    }
}