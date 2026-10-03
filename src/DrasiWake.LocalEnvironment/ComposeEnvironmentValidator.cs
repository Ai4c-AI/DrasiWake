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
        ValidateFixtureFile(options.DrasiComposeFilePath, "Drasi Docker Compose", errors);
        ValidateFixtureFile(options.DrasiComposeOverridePath, "DrasiWake Drasi Compose override", errors);
        ValidateFixtureFile(options.DrasiServerConfigPath, "DrasiWake Drasi server config", errors);
        ValidateFixtureFile(options.OpenClawComposeFilePath, "OpenClaw Docker Compose", errors);
        ValidateFixtureFile(options.OpenClawMetaSkillPath, "DrasiWake OpenClaw MetaSkill", errors);

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

    }

    private static void ValidateFixtureFile(string path, string description, ICollection<string> errors)
    {
        if (!File.Exists(path))
        {
            errors.Add($"Required local fixture file for {description} is missing: {path}.");
        }
    }
}