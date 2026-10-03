using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace DrasiWake.LocalEnvironment;

public sealed class ComposeEnvironmentCoordinator(
    ComposeEnvironmentOptions options,
    IReadOnlyList<ComposeStackDefinition> stacks,
    IComposeCommandExecutor executor,
    IComposeResourceNotifier resourceNotifier,
    ComposeEnvironmentState state) : IComposeEnvironmentRuntime
{
    private static readonly TimeSpan PreflightTimeout = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan ComposeTimeout = TimeSpan.FromMinutes(4);
    private readonly SemaphoreSlim _lifecycleGate = new(1, 1);
    private readonly ConcurrentDictionary<string, ComposeStackDefinition> _ownedStacks = new(StringComparer.Ordinal);
    private bool _started;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _lifecycleGate.WaitAsync(cancellationToken);
        try
        {
            if (_started)
            {
                return;
            }

            await PreflightAsync(cancellationToken);
            using var startupCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            try
            {
                await Task.WhenAll(stacks.Select(stack => StartStackAsync(stack, startupCancellation)));

                var drasiStack = stacks.Single(stack => stack.ServiceName == "drasi-server");
                var openClawStack = stacks.Single(stack => stack.ServiceName == "openclaw");
                var drasiAddress = await DiscoverAddressAsync(drasiStack, cancellationToken);
                var openClawAddress = await DiscoverAddressAsync(openClawStack, cancellationToken);
                state.SetAddresses(drasiAddress, openClawAddress);

                foreach (var stack in stacks)
                {
                    await resourceNotifier.MarkReadyAsync(stack.Resource);
                }

                _started = true;
            }
            catch (Exception startupException)
            {
                var rollbackErrors = await DiscoverAndCleanupOwnedStacksAsync();
                state.Clear();
                if (rollbackErrors.Count > 0)
                {
                    throw new AggregateException(
                        "Compose startup failed and one or more owned stacks could not be rolled back.",
                        [startupException, .. rollbackErrors]);
                }

                throw;
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _lifecycleGate.WaitAsync(cancellationToken);
        try
        {
            var cleanupErrors = await CleanupOwnedStacksAsync();
            state.Clear();
            _started = false;
            foreach (var stack in stacks)
            {
                try
                {
                    await resourceNotifier.MarkStoppedAsync(stack.Resource);
                }
                catch (Exception exception)
                {
                    cleanupErrors.Add(exception);
                }
            }

            if (cleanupErrors.Count > 0)
            {
                throw new AggregateException("One or more owned Compose stacks could not be stopped.", cleanupErrors);
            }
        }
        finally
        {
            _lifecycleGate.Release();
        }
    }

    private async Task PreflightAsync(CancellationToken cancellationToken)
    {
        var validationErrors = new ComposeEnvironmentValidator(options).Validate();
        if (validationErrors.Count > 0)
        {
            throw new InvalidOperationException(
                "Local environment preflight failed:" + Environment.NewLine + string.Join(Environment.NewLine, validationErrors));
        }

        var dockerVersion = await ExecuteDockerAsync(
            ["--version"],
            options.RepositoryRoot,
            PreflightTimeout,
            cancellationToken);
        EnsureSuccessful(dockerVersion, "Docker CLI version check");

        var daemon = await ExecuteDockerAsync(
            ["info", "--format", "{{.ServerVersion}}"],
            options.RepositoryRoot,
            PreflightTimeout,
            cancellationToken);
        EnsureSuccessful(daemon, "Docker daemon check");

        var composeVersion = await ExecuteDockerAsync(
            ["compose", "version", "--short"],
            options.RepositoryRoot,
            PreflightTimeout,
            cancellationToken);
        EnsureSuccessful(composeVersion, "Docker Compose version check");

        var composeHelp = await ExecuteDockerAsync(
            ["compose", "up", "--help"],
            options.RepositoryRoot,
            PreflightTimeout,
            cancellationToken);
        EnsureSuccessful(composeHelp, "Docker Compose up capability check");
        var composeHelpText = composeHelp.StandardOutput + composeHelp.StandardError;
        if (!composeHelpText.Contains("--wait", StringComparison.Ordinal) ||
            !composeHelpText.Contains("--wait-timeout", StringComparison.Ordinal))
        {
            throw new InvalidOperationException("Docker Compose must support `up --wait --wait-timeout`.");
        }

        foreach (var stack in stacks)
        {
            await EnsureNoContainerConflictAsync(stack, cancellationToken);
            foreach (var port in ComposeFileInspector.GetDefaultHostPorts(stack.ComposeFilePath))
            {
                if (IsPortInUse(port))
                {
                    throw new InvalidOperationException(
                        $"Default host port {port} declared by {stack.Resource.Name} is already in use.");
                }
            }
        }
    }

    private async Task EnsureNoContainerConflictAsync(
        ComposeStackDefinition stack,
        CancellationToken cancellationToken)
    {
        foreach (var containerName in stack.ContainerNames)
        {
            var result = await ExecuteDockerAsync(
                ["ps", "--all", "--filter", $"name=^/{containerName}$", "--format", "{{.Names}}"],
                options.RepositoryRoot,
                PreflightTimeout,
                cancellationToken);
            EnsureSuccessful(result, "container conflict check");
            if (!string.IsNullOrWhiteSpace(result.StandardOutput))
            {
                throw new InvalidOperationException(
                    $"Container '{containerName}' already exists; stop it manually before starting {stack.Resource.Name}.");
            }
        }
    }

    private async Task StartStackAsync(
        ComposeStackDefinition stack,
        CancellationTokenSource startupCancellation)
    {
        try
        {
            var result = await ExecuteComposeAsync(
                stack,
                ["up", "--detach", "--wait", "--wait-timeout", "180"],
                ComposeTimeout,
                startupCancellation.Token);
            EnsureSuccessful(result, $"starting {stack.Resource.Name}");
            _ownedStacks.TryAdd(stack.ProjectName, stack);
        }
        catch
        {
            startupCancellation.Cancel();
            throw;
        }
    }

    private async Task<Uri> DiscoverAddressAsync(
        ComposeStackDefinition stack,
        CancellationToken cancellationToken)
    {
        var result = await ExecuteComposeAsync(
            stack,
            ["port", stack.ServiceName, stack.ContainerPort.ToString(System.Globalization.CultureInfo.InvariantCulture)],
            PreflightTimeout,
            cancellationToken);
        EnsureSuccessful(result, $"discovering {stack.Resource.Name} address");
        return ComposePortParser.Parse(result.StandardOutput, stack.ServiceName);
    }

    private async Task<ComposeCommandResult> ExecuteComposeAsync(
        ComposeStackDefinition stack,
        IReadOnlyList<string> composeArguments,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var arguments = new List<string>
        {
            "compose",
            "--project-directory",
            stack.RepositoryPath,
            "--file",
            stack.ComposeFilePath,
            "--project-name",
            stack.ProjectName
        };
        arguments.AddRange(composeArguments);

        var command = new ComposeCommand(
            "docker",
            arguments,
            stack.RepositoryPath,
            stack.Environment,
            stack.SecretValues,
            timeout,
            stack.RemovedEnvironmentVariables);
        return await executor.ExecuteAsync(command, cancellationToken);
    }

    private async Task<ComposeCommandResult> ExecuteDockerAsync(
        IReadOnlyList<string> arguments,
        string workingDirectory,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var command = new ComposeCommand(
            "docker",
            arguments,
            workingDirectory,
            new Dictionary<string, string>(),
            [options.ModelProviderKey, options.AuthToken],
            timeout,
            [
                "MODEL_PROVIDER_KEY",
                "OPENCLAW_AUTH_TOKEN",
                "DrasiWake__DevEnvironment__OpenClaw__ModelProviderKey",
                "DrasiWake__DevEnvironment__OpenClaw__AuthToken"
            ]);
        return await executor.ExecuteAsync(command, cancellationToken);
    }

    private async Task<List<Exception>> DiscoverAndCleanupOwnedStacksAsync()
    {
        var errors = new List<Exception>();
        foreach (var stack in stacks)
        {
            if (_ownedStacks.ContainsKey(stack.ProjectName))
            {
                continue;
            }

            try
            {
                var result = await ExecuteDockerAsync(
                    ["ps", "--all", "--filter", $"label=com.docker.compose.project={stack.ProjectName}", "--format", "{{.Names}}"],
                    options.RepositoryRoot,
                    PreflightTimeout,
                    CancellationToken.None);
                EnsureSuccessful(result, $"checking ownership for {stack.Resource.Name}");
                if (!string.IsNullOrWhiteSpace(result.StandardOutput))
                {
                    _ownedStacks.TryAdd(stack.ProjectName, stack);
                }
            }
            catch (Exception exception)
            {
                errors.Add(exception);
            }
        }

        errors.AddRange(await CleanupOwnedStacksAsync());
        return errors;
    }

    private async Task<List<Exception>> CleanupOwnedStacksAsync()
    {
        var errors = new List<Exception>();
        foreach (var stack in stacks.Reverse())
        {
            if (!_ownedStacks.ContainsKey(stack.ProjectName))
            {
                continue;
            }

            try
            {
                var result = await ExecuteComposeAsync(
                    stack,
                    ["down"],
                    ComposeTimeout,
                    CancellationToken.None);
                EnsureSuccessful(result, $"stopping {stack.Resource.Name}");
                _ownedStacks.TryRemove(stack.ProjectName, out _);
            }
            catch (Exception exception)
            {
                errors.Add(exception);
            }
        }

        return errors;
    }

    private static void EnsureSuccessful(ComposeCommandResult result, string stage)
    {
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException($"Docker operation failed during {stage} (exit code {result.ExitCode}).");
        }
    }

    private static bool IsPortInUse(int port)
    {
        using var listener = new TcpListener(IPAddress.Any, port);
        try
        {
            listener.Start();
            return false;
        }
        catch (SocketException)
        {
            return true;
        }
    }
}