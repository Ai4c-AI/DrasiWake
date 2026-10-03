namespace DrasiWake.LocalEnvironment;

public static class AppHostRunGuard
{
    public static async Task RunWithCleanupAsync(
        Func<Task> runAsync,
        Func<CancellationToken, Task> stopAsync,
        Func<CancellationToken, Task> fallbackCleanupAsync)
    {
        ArgumentNullException.ThrowIfNull(runAsync);
        ArgumentNullException.ThrowIfNull(stopAsync);
        ArgumentNullException.ThrowIfNull(fallbackCleanupAsync);

        try
        {
            await runAsync();
        }
        catch (Exception runException)
        {
            Exception? stopException = null;
            try
            {
                await stopAsync(CancellationToken.None);
            }
            catch (Exception exception)
            {
                stopException = exception;
            }

            try
            {
                await fallbackCleanupAsync(CancellationToken.None);
            }
            catch (Exception cleanupException)
            {
                var failures = new List<Exception> { runException };
                if (stopException is not null)
                {
                    failures.Add(stopException);
                }

                failures.Add(cleanupException);
                throw new AggregateException("AppHost execution failed and shutdown cleanup also failed.", failures);
            }

            if (stopException is not null and not OperationCanceledException)
            {
                throw new AggregateException("AppHost execution failed and Host shutdown also failed.", runException, stopException);
            }

            throw;
        }
    }
}