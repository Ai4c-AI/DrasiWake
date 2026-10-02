using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace DrasiWake.Persistence.SonnetDB;

public sealed class BridgeDbContextFactory : IDesignTimeDbContextFactory<BridgeDbContext>
{
    public BridgeDbContext CreateDbContext(string[] args)
    {
        var databasePath = Environment.GetEnvironmentVariable("DRASIWAKE_DB_PATH") ??
                           Path.Combine(Path.GetTempPath(), "DrasiWake-design-time");
        var options = new DbContextOptionsBuilder<BridgeDbContext>()
            .UseSonnetDB($"Data Source={databasePath}")
            .Options;
        return new BridgeDbContext(options);
    }
}