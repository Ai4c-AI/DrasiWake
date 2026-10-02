using DrasiWake.Persistence.SonnetDB;

namespace DrasiWake.Persistence.SonnetDB.Tests;

public sealed class SonnetDatabaseOwnerTests
{
    [Fact]
    public void Database_directory_allows_only_one_active_owner()
    {
        var databaseDirectory = Path.Combine(Path.GetTempPath(), $"DrasiWakeOwner-{Guid.NewGuid():N}");
        try
        {
            using (SonnetDatabaseOwner.Acquire(databaseDirectory))
            {
                Assert.Throws<InvalidOperationException>(() => SonnetDatabaseOwner.Acquire(databaseDirectory));
            }

            using var reopenedOwner = SonnetDatabaseOwner.Acquire(databaseDirectory);
        }
        finally
        {
            if (Directory.Exists(databaseDirectory))
            {
                Directory.Delete(databaseDirectory, recursive: true);
            }
        }
    }
}