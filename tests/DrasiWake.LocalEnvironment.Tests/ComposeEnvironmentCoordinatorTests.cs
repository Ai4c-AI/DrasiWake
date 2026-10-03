using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using DrasiWake.LocalEnvironment;

public sealed class ComposeEnvironmentCoordinatorTests
{
    [Fact]
    public async Task Starts_stacks_concurrently_discovers_addresses_and_stops_in_reverse_order()
    {
        using var fixture = new CoordinatorFixture();
        var executor = new RecordingCommandExecutor(waitForBothStarts: true);
        var notifier = new RecordingResourceNotifier();
        var state = new ComposeEnvironmentState();
        var stacks = fixture.CreateStacks();
        var coordinator = fixture.CreateCoordinator(stacks, executor, notifier, state);

        await coordinator.StartAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, executor.MaximumConcurrentStarts);
        Assert.Equal(new Uri("http://127.0.0.1:45001/"), state.DrasiServerUri);
        Assert.Equal(new Uri("http://127.0.0.1:45002/"), state.OpenClawBaseAddress);
        Assert.Equal(2, notifier.ReadyResources.Count);

        var drasi = stacks[0];
        var openClaw = stacks[1];
        var upCommands = executor.Commands.Where(IsComposeUp).ToArray();
        Assert.Equal(2, upCommands.Length);
        Assert.All(upCommands, command => Assert.Equal(TimeSpan.FromMinutes(15), command.Timeout));
        Assert.DoesNotContain(upCommands[0].Arguments, argument =>
            argument.Contains("provider-secret", StringComparison.Ordinal) ||
            argument.Contains("gateway-secret", StringComparison.Ordinal));
        Assert.Single(drasi.Environment);
        Assert.Contains("DRASIWAKE_FIXTURE_CONFIG_PATH", drasi.Environment.Keys);
        Assert.Contains("MODEL_PROVIDER_KEY", drasi.RemovedEnvironmentVariables);
        Assert.Contains(
            "DrasiWake__DevEnvironment__OpenClaw__AuthToken",
            drasi.RemovedEnvironmentVariables);
        Assert.Equal("provider-secret", openClaw.Environment["MODEL_PROVIDER_KEY"]);
        Assert.Equal("gateway-secret", openClaw.Environment["OPENCLAW_AUTH_TOKEN"]);

        await coordinator.StopAsync(TestContext.Current.CancellationToken);

        var downCommands = executor.Commands.Where(IsComposeDown).ToArray();
        Assert.Equal([openClaw.ProjectName, drasi.ProjectName], downCommands.Select(GetProjectName));
        Assert.All(downCommands, command => Assert.Equal(TimeSpan.FromMinutes(4), command.Timeout));
        Assert.All(downCommands, command =>
        {
            Assert.DoesNotContain("--volumes", command.Arguments);
            Assert.DoesNotContain("-v", command.Arguments);
        });
        Assert.Null(state.DrasiServerUri);
        Assert.Equal(2, notifier.StoppedResources.Count);
    }

    [Fact]
    public async Task Uses_repository_local_drasi_config_and_openclaw_workspace_fixtures()
    {
        using var fixture = new CoordinatorFixture();
        var executor = new RecordingCommandExecutor();
        var stacks = fixture.CreateStacks();
        var coordinator = fixture.CreateCoordinator(
            stacks,
            executor,
            new RecordingResourceNotifier(),
            new ComposeEnvironmentState());

        await coordinator.StartAsync(TestContext.Current.CancellationToken);

        var drasiUp = executor.Commands.Single(command =>
            IsComposeUp(command) && GetProjectName(command) == stacks[0].ProjectName);
        var openClawUp = executor.Commands.Single(command =>
            IsComposeUp(command) && GetProjectName(command) == stacks[1].ProjectName);

        Assert.Contains(
            Path.Combine(fixture.Root, "dev", "fixtures", "drasi", "compose.override.yml"),
            drasiUp.Arguments);
        Assert.Equal(
            Path.Combine(fixture.Root, "dev", "fixtures", "openclaw", "workspace"),
            openClawUp.Environment["OPENCLAW_WORKSPACE"]);

        await coordinator.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Existing_container_conflict_prevents_all_start_and_cleanup_commands()
    {
        using var fixture = new CoordinatorFixture();
        var executor = new RecordingCommandExecutor(conflictingContainer: "drasi-server");
        var stacks = fixture.CreateStacks();
        var coordinator = fixture.CreateCoordinator(
            stacks,
            executor,
            new RecordingResourceNotifier(),
            new ComposeEnvironmentState());

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.StartAsync(TestContext.Current.CancellationToken));

        Assert.Contains("drasi-server", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(executor.Commands, IsComposeUp);
        Assert.DoesNotContain(executor.Commands, IsComposeDown);
    }

    [Fact]
    public async Task Preflight_conflict_does_not_mark_unstarted_resources_stopped()
    {
        using var fixture = new CoordinatorFixture();
        var executor = new RecordingCommandExecutor(conflictingContainer: "openclaw-gateway");
        var notifier = new RecordingResourceNotifier();
        var coordinator = fixture.CreateCoordinator(
            fixture.CreateStacks(),
            executor,
            notifier,
            new ComposeEnvironmentState());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.StartAsync(TestContext.Current.CancellationToken));
        await coordinator.StopAsync(TestContext.Current.CancellationToken);

        Assert.Empty(notifier.StoppedResources);
    }

    [Fact]
    public async Task Failed_start_rolls_back_only_the_stack_created_by_this_attempt()
    {
        using var fixture = new CoordinatorFixture();
        var executor = new RecordingCommandExecutor(failFirstStart: true);
        var stacks = fixture.CreateStacks();
        var coordinator = fixture.CreateCoordinator(
            stacks,
            executor,
            new RecordingResourceNotifier(),
            new ComposeEnvironmentState());

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.StartAsync(TestContext.Current.CancellationToken));

        Assert.Contains("compose-up-diagnostic", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("provider-secret", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("gateway-secret", exception.Message, StringComparison.Ordinal);
        var downCommands = executor.Commands.Where(IsComposeDown).ToArray();
        Assert.Single(downCommands);
        Assert.Equal(stacks[0].ProjectName, GetProjectName(downCommands[0]));
        Assert.DoesNotContain(executor.Commands, command =>
            command.Arguments.Contains("--volumes", StringComparer.Ordinal) ||
            command.Arguments.Contains("-v", StringComparer.Ordinal));
    }

    [Fact]
    public async Task Address_discovery_failure_rolls_back_both_stacks_without_marking_them_ready()
    {
        using var fixture = new CoordinatorFixture();
        var executor = new RecordingCommandExecutor(invalidOpenClawPort: true);
        var notifier = new RecordingResourceNotifier();
        var stacks = fixture.CreateStacks();
        var coordinator = fixture.CreateCoordinator(stacks, executor, notifier, new ComposeEnvironmentState());

        await Assert.ThrowsAsync<FormatException>(() =>
            coordinator.StartAsync(TestContext.Current.CancellationToken));

        Assert.Equal(2, executor.Commands.Count(IsComposeDown));
        Assert.Empty(notifier.ReadyResources);
        Assert.Equal([stacks[1].ProjectName, stacks[0].ProjectName],
            executor.Commands.Where(IsComposeDown).Select(GetProjectName));
    }

    [Fact]
    public async Task Cleanup_continues_after_failure_and_retries_only_the_unstopped_stack()
    {
        using var fixture = new CoordinatorFixture();
        var executor = new RecordingCommandExecutor(failFirstDown: true);
        var stacks = fixture.CreateStacks();
        var coordinator = fixture.CreateCoordinator(
            stacks,
            executor,
            new RecordingResourceNotifier(),
            new ComposeEnvironmentState());
        await coordinator.StartAsync(TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<AggregateException>(() =>
            coordinator.StopAsync(TestContext.Current.CancellationToken));
        Assert.Equal(2, executor.Commands.Count(IsComposeDown));

        await coordinator.StopAsync(TestContext.Current.CancellationToken);

        var downCommands = executor.Commands.Where(IsComposeDown).ToArray();
        Assert.Equal(3, downCommands.Length);
        Assert.Equal(stacks[1].ProjectName, GetProjectName(downCommands[0]));
        Assert.Equal(stacks[0].ProjectName, GetProjectName(downCommands[1]));
        Assert.Equal(stacks[1].ProjectName, GetProjectName(downCommands[2]));
    }

    [Fact]
    public async Task Occupied_default_host_port_prevents_starting_either_stack()
    {
        using var fixture = new CoordinatorFixture();
        using var listener = new TcpListener(IPAddress.Any, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        fixture.SetDrasiHostPort(port);
        var executor = new RecordingCommandExecutor();
        var coordinator = fixture.CreateCoordinator(
            fixture.CreateStacks(),
            executor,
            new RecordingResourceNotifier(),
            new ComposeEnvironmentState());

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            coordinator.StartAsync(TestContext.Current.CancellationToken));

        Assert.Contains(port.ToString(), exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(executor.Commands, IsComposeUp);
        Assert.DoesNotContain(executor.Commands, IsComposeDown);
    }

    private static bool IsComposeUp(ComposeCommand command) =>
        command.Arguments.Count > 1 && command.Arguments[0] == "compose" &&
        command.Arguments.Contains("up") && !command.Arguments.Contains("--help");

    private static bool IsComposeDown(ComposeCommand command) =>
        command.Arguments.Count > 1 && command.Arguments[0] == "compose" && command.Arguments.Contains("down");

    private static string GetProjectName(ComposeCommand command) =>
        command.Arguments[command.Arguments.ToList().IndexOf("--project-name") + 1];

    private sealed class CoordinatorFixture : IDisposable
    {
        private readonly DirectoryInfo _root = Directory.CreateTempSubdirectory("drasiwake-coordinator-");
        private readonly string _drasiPath;
        private readonly string _openClawPath;

        public string Root => _root.FullName;

        public CoordinatorFixture()
        {
            _drasiPath = CreateRepository("drasi", "${DRASI_API_PORT:-0}:8080");
            _openClawPath = CreateRepository("openclaw", "0:18789");
            var options = new ComposeEnvironmentOptions(Root, _drasiPath, _openClawPath, "", "");
            Directory.CreateDirectory(options.DrasiFixtureConfigPath);
            Directory.CreateDirectory(Path.GetDirectoryName(options.OpenClawMetaSkillPath)!);
            File.WriteAllText(options.DrasiComposeOverridePath, "services: {}\n");
            File.WriteAllText(options.DrasiComposeFilePath, "services:\n  drasi:\n    ports:\n      - '${DRASI_API_PORT:-0}:8080'\n");
            File.WriteAllText(options.OpenClawComposeFilePath, "services:\n  openclaw:\n    ports:\n      - '0:18789'\n");
            File.WriteAllText(options.DrasiServerConfigPath, "apiVersion: drasi.io/v1\n");
            File.WriteAllText(options.OpenClawMetaSkillPath, "---\nname: test\nkind: meta\n---\n");
        }

        public IReadOnlyList<ComposeStackDefinition> CreateStacks() => ComposeStackDefinition.Create(
            new ComposeEnvironmentOptions(_root.FullName, _drasiPath, _openClawPath, "provider-secret", "gateway-secret"),
            new ComposeStackResource("drasi-compose"),
            new ComposeStackResource("openclaw-compose"));

        public void SetDrasiHostPort(int port) => File.WriteAllText(
            new ComposeEnvironmentOptions(Root, _drasiPath, _openClawPath, "", "").DrasiComposeFilePath,
            $"services:\n  drasi:\n    ports:\n      - '{port}:8080'\n");

        public ComposeEnvironmentCoordinator CreateCoordinator(
            IReadOnlyList<ComposeStackDefinition> stacks,
            IComposeCommandExecutor executor,
            IComposeResourceNotifier notifier,
            ComposeEnvironmentState state) => new(
                new ComposeEnvironmentOptions(_root.FullName, _drasiPath, _openClawPath, "provider-secret", "gateway-secret"),
                stacks,
                executor,
                notifier,
                state);

        private string CreateRepository(string name, string publishedPort)
        {
            var path = Path.Combine(_root.FullName, name);
            Directory.CreateDirectory(path);
            File.WriteAllText(
                Path.Combine(path, "docker-compose.yml"),
                $"services:\n  {name}:\n    ports:\n      - '{publishedPort}'\n");
            return path;
        }

        public void Dispose() => _root.Delete(recursive: true);
    }

    private sealed class RecordingCommandExecutor(
        bool waitForBothStarts = false,
        string? conflictingContainer = null,
        bool failFirstStart = false,
        bool invalidOpenClawPort = false,
        bool failFirstDown = false) : IComposeCommandExecutor
    {
        private readonly ConcurrentQueue<ComposeCommand> _commands = new();
        private readonly ConcurrentDictionary<string, string[]> _containersByProject = new(StringComparer.Ordinal);
        private readonly TaskCompletionSource _bothStarts = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _activeStarts;
        private int _maximumConcurrentStarts;
        private int _startCount;
        private int _downFailureUsed;

        public IReadOnlyList<ComposeCommand> Commands => _commands.ToArray();

        public int MaximumConcurrentStarts => _maximumConcurrentStarts;

        public async Task<ComposeCommandResult> ExecuteAsync(
            ComposeCommand command,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _commands.Enqueue(command);
            if (command.Arguments.Contains("up") && command.Arguments.Contains("--help"))
            {
                return new ComposeCommandResult(0, "--wait --wait-timeout", string.Empty);
            }

            if (command.Arguments.Count > 0 && command.Arguments[0] == "compose")
            {
                return await ExecuteComposeAsync(command, cancellationToken);
            }

            if (command.Arguments.Count > 0 && command.Arguments[0] == "ps")
            {
                return ExecutePs(command);
            }

            if (command.Arguments.Contains("up") && command.Arguments.Contains("--help"))
            {
                return new ComposeCommandResult(0, "--wait --wait-timeout", string.Empty);
            }

            return new ComposeCommandResult(0, "ok", string.Empty);
        }

        private async Task<ComposeCommandResult> ExecuteComposeAsync(
            ComposeCommand command,
            CancellationToken cancellationToken)
        {
            if (command.Arguments.Contains("up"))
            {
                var projectName = GetProjectName(command);
                var active = Interlocked.Increment(ref _activeStarts);
                UpdateMaximum(active);
                var startNumber = Interlocked.Increment(ref _startCount);
                if (!failFirstStart || startNumber == 1)
                {
                    _containersByProject[projectName] = ["created-container"];
                }

                if (waitForBothStarts && startNumber == 2)
                {
                    _bothStarts.TrySetResult();
                }

                try
                {
                    if (waitForBothStarts)
                    {
                        await _bothStarts.Task.WaitAsync(cancellationToken);
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref _activeStarts);
                }

                if (failFirstStart && startNumber == 1)
                {
                    return new ComposeCommandResult(
                        1,
                        string.Empty,
                        new string('x', 600) + " compose-up-diagnostic provider-secret gateway-secret");
                }

                return new ComposeCommandResult(0, "healthy", string.Empty);
            }

            if (command.Arguments.Contains("port"))
            {
                var service = command.Arguments[command.Arguments.ToList().IndexOf("port") + 1];
                if (invalidOpenClawPort && service == "openclaw")
                {
                    return new ComposeCommandResult(0, "not-a-port", string.Empty);
                }

                return new ComposeCommandResult(
                    0,
                    service == "drasi-server" ? "0.0.0.0:45001" : ":::45002",
                    string.Empty);
            }

            if (command.Arguments.Contains("down"))
            {
                if (failFirstDown && Interlocked.Exchange(ref _downFailureUsed, 1) == 0)
                {
                    return new ComposeCommandResult(1, string.Empty, "down failed");
                }

                _containersByProject.TryRemove(GetProjectName(command), out _);
            }

            return new ComposeCommandResult(0, string.Empty, string.Empty);
        }

        private ComposeCommandResult ExecutePs(ComposeCommand command)
        {
            var filterIndex = command.Arguments.ToList().IndexOf("--filter");
            var filter = command.Arguments[filterIndex + 1];
            if (filter.StartsWith("name=", StringComparison.Ordinal))
            {
                var containerName = filter["name=^/".Length..].TrimEnd('$');
                return new ComposeCommandResult(
                    0,
                    containerName == conflictingContainer ? containerName : string.Empty,
                    string.Empty);
            }

            var projectName = filter["label=com.docker.compose.project=".Length..];
            var names = _containersByProject.TryGetValue(projectName, out var containers)
                ? string.Join(Environment.NewLine, containers)
                : string.Empty;
            return new ComposeCommandResult(0, names, string.Empty);
        }

        private void UpdateMaximum(int active)
        {
            int current;
            do
            {
                current = _maximumConcurrentStarts;
                if (current >= active)
                {
                    return;
                }
            }
            while (Interlocked.CompareExchange(ref _maximumConcurrentStarts, active, current) != current);
        }
    }

    private sealed class RecordingResourceNotifier : IComposeResourceNotifier
    {
        private readonly ConcurrentQueue<string> _ready = new();
        private readonly ConcurrentQueue<string> _stopped = new();

        public IReadOnlyList<string> ReadyResources => _ready.ToArray();

        public IReadOnlyList<string> StoppedResources => _stopped.ToArray();

        public Task MarkReadyAsync(ComposeStackResource resource)
        {
            _ready.Enqueue(resource.Name);
            return Task.CompletedTask;
        }

        public Task MarkStoppedAsync(ComposeStackResource resource)
        {
            _stopped.Enqueue(resource.Name);
            return Task.CompletedTask;
        }
    }
}