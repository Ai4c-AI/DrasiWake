namespace DrasiWake.LocalEnvironment;

public interface IComposeCommandExecutor
{
    Task<ComposeCommandResult> ExecuteAsync(ComposeCommand command, CancellationToken cancellationToken);
}