namespace DrasiWake.LocalEnvironment;

public sealed class ComposeCommand
{
    public ComposeCommand(
        string executable,
        IReadOnlyList<string> arguments,
        string workingDirectory,
        IReadOnlyDictionary<string, string> environment,
        IReadOnlyList<string> secretValues,
        TimeSpan timeout,
        IReadOnlyList<string>? environmentVariablesToRemove = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(executable);
        ArgumentNullException.ThrowIfNull(arguments);
        ArgumentException.ThrowIfNullOrWhiteSpace(workingDirectory);
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentNullException.ThrowIfNull(secretValues);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);

        Executable = executable;
        Arguments = arguments.ToArray();
        WorkingDirectory = workingDirectory;
        Environment = new Dictionary<string, string>(environment, StringComparer.OrdinalIgnoreCase);
        SecretValues = secretValues.Where(value => !string.IsNullOrEmpty(value)).Distinct(StringComparer.Ordinal).ToArray();
        Timeout = timeout;
        EnvironmentVariablesToRemove = environmentVariablesToRemove?.ToArray() ?? [];
    }

    public string Executable { get; }

    public IReadOnlyList<string> Arguments { get; }

    public string WorkingDirectory { get; }

    public IReadOnlyDictionary<string, string> Environment { get; }

    public IReadOnlyList<string> SecretValues { get; }

    public TimeSpan Timeout { get; }

    public IReadOnlyList<string> EnvironmentVariablesToRemove { get; }
}