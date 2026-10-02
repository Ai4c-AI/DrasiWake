using System.Text.Json;
using System.Text.Json.Nodes;
using Json.Schema;
using YamlDotNet.Core;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace DrasiWake.Core.Contracts;

public sealed class ContractRegistryLoader
{
    private readonly IDeserializer _deserializer = new DeserializerBuilder()
        .WithNamingConvention(CamelCaseNamingConvention.Instance)
        .Build();

    public async Task<ContractRegistryCandidate> LoadCandidateAsync(string path, CancellationToken cancellationToken)
    {
        var errors = new List<ContractValidationError>();
        RegistryDocument? document;

        try
        {
            var yaml = await File.ReadAllTextAsync(path, cancellationToken);
            document = _deserializer.Deserialize<RegistryDocument>(yaml);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or YamlException)
        {
            return Invalid("registry.read_failed", exception.Message);
        }

        if (document?.Bindings is null || document.Bindings.Count == 0)
        {
            return Invalid("registry.bindings_required", "The registry must contain at least one binding.");
        }

        var bindings = new List<BridgeBinding>(document.Bindings.Count);
        foreach (var item in document.Bindings)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var before = errors.Count;
            var binding = await ValidateBindingAsync(item, Path.GetDirectoryName(Path.GetFullPath(path))!, errors, cancellationToken);
            if (binding is not null && errors.Count == before)
            {
                bindings.Add(binding);
            }
        }

        ValidateRegistry(bindings, errors);

        if (errors.Count > 0)
        {
            return new ContractRegistryCandidate(null, errors);
        }

        return new ContractRegistryCandidate(
            new ContractRegistry(document.Version ?? "1.0.0", bindings),
            Array.Empty<ContractValidationError>());
    }

    private static async Task<BridgeBinding?> ValidateBindingAsync(
        BindingDocument item,
        string registryDirectory,
        List<ContractValidationError> errors,
        CancellationToken cancellationToken)
    {
        var id = item.Id;
        void AddError(string code, string message) => errors.Add(new ContractValidationError(code, message, id));

        if (string.IsNullOrWhiteSpace(item.Id)) AddError("binding.id_required", "Binding id is required.");
        if (string.IsNullOrWhiteSpace(item.Source)) AddError("binding.source_required", "Source is required.");
        if (string.Equals(item.Source, "drasi-server", StringComparison.OrdinalIgnoreCase) &&
            string.Equals(item.DeliveryMode, "retain-every-change", StringComparison.OrdinalIgnoreCase))
        {
            AddError("delivery.unsupported", "Drasi attach does not support retain-every-change delivery.");
        }
        if (!Uri.TryCreate(item.Server, UriKind.Absolute, out var server) ||
            (server.Scheme != Uri.UriSchemeHttp && server.Scheme != Uri.UriSchemeHttps))
        {
            AddError("binding.server_invalid", "Server must be an absolute HTTP or HTTPS URI.");
        }

        var isShared = string.Equals(item.SessionScope, "shared-canonical", StringComparison.OrdinalIgnoreCase);
        if (!string.Equals(item.SessionScope, "per-query", StringComparison.OrdinalIgnoreCase) && !isShared &&
            !string.Equals(item.SessionScope, "singleton", StringComparison.OrdinalIgnoreCase))
        {
            AddError("session.scope_invalid", "Session scope must be per-query, shared-canonical, or explicitly singleton.");
        }

        if (isShared && (item.CanonicalIdentity is null ||
                         string.IsNullOrWhiteSpace(item.CanonicalIdentity.OntologyContext) ||
                         string.IsNullOrWhiteSpace(item.CanonicalIdentity.EntityType) ||
                         !IsValidJsonPointer(item.CanonicalIdentity.EntityIdPointer)))
        {
            AddError("identity.required", "Shared-canonical scope requires ontology/context, entity type, and a valid entity-id pointer.");
        }

        if (!string.IsNullOrWhiteSpace(item.AggregateKeyPointer) && !IsValidJsonPointer(item.AggregateKeyPointer))
        {
            AddError("identity.pointer_invalid", "Aggregate key must be a valid RFC 6901 JSON Pointer.");
        }

        if (string.IsNullOrWhiteSpace(item.MetaSkill)) AddError("routing.skill_required", "MetaSkill name is required.");
        if (item.MaxPayloadBytes <= 0) AddError("payload.limit_invalid", "Maximum payload size must be positive.");
        if (item.Retry is null || item.Retry.MaxAttempts <= 0 || item.Retry.MaxAgeSeconds <= 0)
            AddError("retry.policy_invalid", "Retry attempts and maximum retry age must be positive.");
        if (item.RateLimit is null || item.RateLimit.PermitLimit <= 0 || item.RateLimit.WindowMilliseconds <= 0)
            AddError("rate.policy_invalid", "Rate permit limit and window must be positive.");

        var schemaPath = item.FactSchemaPath;
        string? resolvedSchemaPath = null;
        if (string.IsNullOrWhiteSpace(schemaPath))
        {
            AddError("schema.path_required", "Fact schema path is required.");
        }
        else
        {
            resolvedSchemaPath = Path.GetFullPath(Path.Combine(registryDirectory, schemaPath));
            if (!File.Exists(resolvedSchemaPath))
            {
                AddError("schema.missing", $"Fact schema does not exist: {schemaPath}");
            }
            else
            {
                try
                {
                    var schemaText = await File.ReadAllTextAsync(resolvedSchemaPath, cancellationToken);
                    _ = JsonSchema.FromText(schemaText);
                }
                catch (Exception exception) when (exception is IOException or UnauthorizedAccessException or JsonException)
                {
                    AddError("schema.invalid", $"Fact schema is not valid JSON Schema: {exception.Message}");
                }
            }
        }

        if (errors.Any(error => error.BindingId == id))
        {
            return null;
        }

        var canonicalIdentity = item.CanonicalIdentity is null
            ? null
            : new CanonicalIdentityDefinition(
                item.CanonicalIdentity.OntologyContext!,
                item.CanonicalIdentity.EntityType!,
                item.CanonicalIdentity.EntityIdPointer!);

        return new BridgeBinding(
            item.Id!,
            item.Source!,
            server!,
            item.InstanceId,
            item.QueryId,
            item.DeliveryMode ?? "converge-latest",
            item.SessionScope ?? "per-query",
            item.AggregateKeyPointer,
            canonicalIdentity,
            item.MetaSkill!,
            new BridgeContract(item.ContractVersion ?? "1.0.0", resolvedSchemaPath!),
            item.MaxPayloadBytes,
            item.TriggerPhrases is { } triggerPhrases ? triggerPhrases : Array.Empty<string>(),
            new RetryPolicy(item.Retry!.MaxAttempts, TimeSpan.FromSeconds(item.Retry.MaxAgeSeconds)),
            new RateLimitPolicy(item.RateLimit!.PermitLimit, TimeSpan.FromMilliseconds(item.RateLimit.WindowMilliseconds)));
    }

    private static void ValidateRegistry(
        IReadOnlyList<BridgeBinding> bindings,
        List<ContractValidationError> errors)
    {
        var ids = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var querySkills = new Dictionary<string, BridgeBinding>(StringComparer.OrdinalIgnoreCase);
        var triggerSkills = new Dictionary<string, BridgeBinding>(StringComparer.OrdinalIgnoreCase);

        foreach (var binding in bindings)
        {
            if (!ids.Add(binding.Id))
            {
                errors.Add(new ContractValidationError("binding.id_conflict", "Binding ids must be unique.", binding.Id));
            }

            if (binding.QueryId is not null)
            {
                var queryKey = string.Join('|', binding.Source, binding.Server.GetLeftPart(UriPartial.Authority),
                    binding.InstanceId ?? string.Empty, binding.QueryId);
                if (querySkills.TryGetValue(queryKey, out var previousQuery) &&
                    !string.Equals(previousQuery.MetaSkill, binding.MetaSkill, StringComparison.Ordinal))
                {
                    errors.Add(new ContractValidationError(
                        "binding.query_conflict",
                        "A query cannot be bound to different MetaSkills.",
                        binding.Id));
                }
                else
                {
                    querySkills.TryAdd(queryKey, binding);
                }
            }

            foreach (var phrase in binding.TriggerPhrases)
            {
                var normalizedPhrase = phrase.Trim();
                if (normalizedPhrase.Length == 0) continue;
                if (triggerSkills.TryGetValue(normalizedPhrase, out var previousTrigger) &&
                    !string.Equals(previousTrigger.MetaSkill, binding.MetaSkill, StringComparison.Ordinal))
                {
                    errors.Add(new ContractValidationError(
                        "trigger.conflict",
                        $"Trigger phrase '{normalizedPhrase}' maps to different MetaSkills.",
                        binding.Id));
                }
                else
                {
                    triggerSkills.TryAdd(normalizedPhrase, binding);
                }
            }
        }
    }

    private static bool IsValidJsonPointer(string? pointer)
    {
        if (pointer is null || (pointer.Length > 0 && pointer[0] != '/')) return false;

        for (var index = 0; index < pointer.Length; index++)
        {
            if (pointer[index] != '~') continue;
            if (index + 1 >= pointer.Length || (pointer[index + 1] != '0' && pointer[index + 1] != '1')) return false;
            index++;
        }

        return true;
    }

    private static ContractRegistryCandidate Invalid(string code, string message) =>
        new(null, [new ContractValidationError(code, message)]);

    private sealed class RegistryDocument
    {
        public string? Version { get; set; }
        public List<BindingDocument>? Bindings { get; set; }
    }

    private sealed class BindingDocument
    {
        public string? Id { get; set; }
        public string? Source { get; set; }
        public string? Server { get; set; }
        public string? InstanceId { get; set; }
        public string? QueryId { get; set; }
        public string? DeliveryMode { get; set; }
        public string? SessionScope { get; set; }
        public string? AggregateKeyPointer { get; set; }
        public CanonicalIdentityDocument? CanonicalIdentity { get; set; }
        public string? MetaSkill { get; set; }
        public string? ContractVersion { get; set; }
        public string? FactSchemaPath { get; set; }
        public int MaxPayloadBytes { get; set; }
        public List<string>? TriggerPhrases { get; set; }
        public RetryDocument? Retry { get; set; }
        public RateLimitDocument? RateLimit { get; set; }
    }

    private sealed class CanonicalIdentityDocument
    {
        public string? OntologyContext { get; set; }
        public string? EntityType { get; set; }
        public string? EntityIdPointer { get; set; }
    }

    private sealed class RetryDocument
    {
        public int MaxAttempts { get; set; }
        public int MaxAgeSeconds { get; set; }
    }

    private sealed class RateLimitDocument
    {
        public int PermitLimit { get; set; }
        public int WindowMilliseconds { get; set; }
    }
}