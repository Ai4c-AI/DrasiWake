namespace DrasiWake.Persistence.SonnetDB;

public sealed class SonnetDatabaseOwner : IDisposable
{
    private readonly FileStream _lockFile;

    private SonnetDatabaseOwner(FileStream lockFile)
    {
        _lockFile = lockFile;
    }

    public static SonnetDatabaseOwner Acquire(string databaseDirectory)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseDirectory);
        var fullPath = Path.GetFullPath(databaseDirectory);
        Directory.CreateDirectory(fullPath);
        var lockPath = Path.Combine(fullPath, ".drasiwake-owner.lock");

        try
        {
            var lockFile = new FileStream(
                lockPath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None);
            return new SonnetDatabaseOwner(lockFile);
        }
        catch (IOException exception)
        {
            throw new InvalidOperationException(
                $"SonnetDB directory '{fullPath}' is already owned by another active bridge instance.",
                exception);
        }
    }

    public void Dispose() => _lockFile.Dispose();
}