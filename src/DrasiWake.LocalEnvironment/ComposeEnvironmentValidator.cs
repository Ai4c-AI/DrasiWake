namespace DrasiWake.LocalEnvironment;

public sealed class ComposeEnvironmentValidator(ComposeEnvironmentOptions options)
{
    public IReadOnlyList<string> Validate()
    {
        ArgumentNullException.ThrowIfNull(options);

        var errors = new List<string>();
        ValidateRepository(
            options.DrasiRepositoryPath,
            "DrasiWake:DevEnvironment:DrasiRepositoryPath",
            errors);
        ValidateRepository(
            options.OpenClawRepositoryPath,
            "DrasiWake:DevEnvironment:OpenClawRepositoryPath",
            errors);

        if (string.IsNullOrWhiteSpace(options.ModelProviderKey))
        {
            errors.Add("Required setting is missing: DrasiWake:DevEnvironment:OpenClaw:ModelProviderKey.");
        }

        if (string.IsNullOrWhiteSpace(options.AuthToken))
        {
            errors.Add("Required setting is missing: DrasiWake:DevEnvironment:OpenClaw:AuthToken.");
        }

        return errors;
    }

    private static void ValidateRepository(string path, string settingName, ICollection<string> errors)
    {
        if (!Directory.Exists(path))
        {
            errors.Add($"Repository configured by {settingName} does not exist: {path}.");
            return;
        }

        if (!File.Exists(Path.Combine(path, "docker-compose.yml")))
        {
            errors.Add($"Compose file docker-compose.yml is missing from repository configured by {settingName}: {path}.");
        }
    }
}