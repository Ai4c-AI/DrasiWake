using System.Diagnostics;

namespace DrasiWake.LocalEnvironment;

public sealed class ComposeCommandExecutor : IComposeCommandExecutor
{
    public async Task<ComposeCommandResult> ExecuteAsync(
        ComposeCommand command,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);

        var startInfo = new ProcessStartInfo
        {
            FileName = command.Executable,
            WorkingDirectory = command.WorkingDirectory,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };

        foreach (var argument in command.Arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        foreach (var variableName in command.EnvironmentVariablesToRemove)
        {
            startInfo.Environment.Remove(variableName);
        }

        foreach (var variable in command.Environment)
        {
            startInfo.Environment[variable.Key] = variable.Value;
        }

        using var process = new Process { StartInfo = startInfo };
        try
        {
            if (!process.Start())
            {
                throw new InvalidOperationException($"Unable to start executable '{command.Executable}'.");
            }
        }
        catch (Exception exception) when (exception is not InvalidOperationException)
        {
            throw new InvalidOperationException($"Unable to start executable '{command.Executable}'.", exception);
        }

        var standardOutputTask = process.StandardOutput.ReadToEndAsync();
        var standardErrorTask = process.StandardError.ReadToEndAsync();
        using var timeoutSource = new CancellationTokenSource(command.Timeout);
        using var linkedSource = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeoutSource.Token);

        try
        {
            await process.WaitForExitAsync(linkedSource.Token);
        }
        catch (OperationCanceledException)
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }

            await process.WaitForExitAsync(CancellationToken.None);
            await Task.WhenAll(standardOutputTask, standardErrorTask);

            if (cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException("Compose command was canceled.", cancellationToken);
            }

            throw new TimeoutException($"Compose command exceeded its {command.Timeout} timeout.");
        }

        var standardOutput = await standardOutputTask;
        var standardError = await standardErrorTask;
        return new ComposeCommandResult(
            process.ExitCode,
            Redact(standardOutput, command.SecretValues),
            Redact(standardError, command.SecretValues));
    }

    private static string Redact(string text, IReadOnlyList<string> secretValues)
    {
        foreach (var secret in secretValues.OrderByDescending(value => value.Length))
        {
            text = text.Replace(secret, "[REDACTED]", StringComparison.Ordinal);
        }

        return text;
    }
}