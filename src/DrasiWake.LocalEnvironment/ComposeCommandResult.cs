namespace DrasiWake.LocalEnvironment;

public sealed record ComposeCommandResult(int ExitCode, string StandardOutput, string StandardError);