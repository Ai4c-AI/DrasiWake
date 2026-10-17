using System.Globalization;
using System.Text.Json;

namespace DrasiWake.Cli;

public static class DrasiWakeCli
{
    public const int ExitSuccess = 0;
    public const int ExitUsage = 1;
    public const int ExitConnection = 2;
    public const int ExitServer = 3;

    private const string DefaultUrl = "http://127.0.0.1:5103";
    private const string Usage = """
        drasiwakectl [--url <uri>] [--token <token>] [--json] <command>

        commands:
          outbox list        [--status <s>]... [--binding <id>] [--session <id>] [--limit <n>] [--cursor <c>]
          outbox get         <id>
          outbox dead-letters [--limit <n>] [--cursor <c>]
          outbox replay      <id> [--reason <text>] [--yes]
          cluster status
          downstreams
          health             [--json]

        options:
          --url <uri>        admin API base address (default: http://127.0.0.1:5103, env: DRASIWAKE_ADMIN_URL)
          --token <token>    admin bearer token (env: DRASIWAKE_ADMIN_TOKEN)
          --json             machine-readable JSON output (pipe to jq)

        exit codes: 0 success, 1 usage error, 2 connection/service error, 3 server-side business error
        """;

    public static int Run(string[] args, TextWriter stdout, TextWriter stderr)
    {
        try
        {
            return RunCore(args, stdout, stderr);
        }
        catch (AdminClient.AdminApiException exception)
        {
            stderr.WriteLine(exception.Message);
            return ExitServer;
        }
        catch (HttpRequestException exception)
        {
            stderr.WriteLine($"Cannot reach the DrasiWake admin API: {exception.Message}");
            return ExitConnection;
        }
        catch (TaskCanceledException)
        {
            stderr.WriteLine("The admin API request timed out.");
            return ExitConnection;
        }
        catch (OperationCanceledException)
        {
            return ExitConnection;
        }
    }

    private static int RunCore(string[] args, TextWriter stdout, TextWriter stderr)
    {
        string? url = Environment.GetEnvironmentVariable("DRASIWAKE_ADMIN_URL");
        string? token = Environment.GetEnvironmentVariable("DRASIWAKE_ADMIN_TOKEN");
        var json = false;
        var positional = new List<string>();
        var options = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        var flags = new HashSet<string>(StringComparer.Ordinal);

        for (var index = 0; index < args.Length; index++)
        {
            var arg = args[index];
            switch (arg)
            {
                case "--url" when index + 1 < args.Length:
                    url = args[++index];
                    break;
                case "--token" when index + 1 < args.Length:
                    token = args[++index];
                    break;
                case "--json":
                    json = true;
                    break;
                default:
                    if (arg.StartsWith("--", StringComparison.Ordinal))
                    {
                        if (index + 1 < args.Length && !args[index + 1].StartsWith("--", StringComparison.Ordinal))
                        {
                            options.TryAdd(arg, []);
                            options[arg].Add(args[++index]);
                        }
                        else
                        {
                            flags.Add(arg);
                        }
                    }
                    else
                    {
                        positional.Add(arg);
                    }

                    break;
            }
        }

        if (positional.Count == 0)
        {
            stderr.WriteLine(Usage);
            return ExitUsage;
        }

        // Help must work without a reachable or even valid --url.
        if (positional[0] is "help" or "--help" or "-h")
            return WriteUsage(stdout);

        // Report an unknown top-level command before complaining about the URL.
        if (positional[0] is not ("outbox" or "cluster" or "downstreams" or "health"))
            return UnknownCommand(positional[0], stderr);

        if (!Uri.TryCreate(url, UriKind.Absolute, out var baseUrl) ||
            (baseUrl.Scheme != Uri.UriSchemeHttp && baseUrl.Scheme != Uri.UriSchemeHttps))
        {
            stderr.WriteLine($"--url must be an absolute HTTP or HTTPS URI (got '{url}').");
            return ExitUsage;
        }

        var client = new AdminClient(baseUrl, token);
        var command = positional[0];
        return command switch
        {
            "outbox" => RunOutbox(client, positional, options, flags, json, stdout, stderr),
            "cluster" => RunCluster(client, positional, json, stdout, stderr),
            "downstreams" => WriteRaw(client, "/admin/downstreams", json, true, stdout, stderr),
            "health" => WriteRaw(client, "/admin/health", json, json, stdout, stderr),
            _ => UnknownCommand(command, stderr)
        };
    }

    private static int RunOutbox(
        AdminClient client,
        List<string> positional,
        Dictionary<string, List<string>> options,
        HashSet<string> flags,
        bool json,
        TextWriter stdout,
        TextWriter stderr)
    {
        if (positional.Count < 2)
        {
            stderr.WriteLine(Usage);
            return ExitUsage;
        }

        var subCommand = positional[1];
        var jsonOutput = json || flags.Contains("--json");
        switch (subCommand)
        {
            case "list":
            {
                var limit = ReadLimit(options);
                if (limit is null)
                    return ExitUsage;
                return WriteOutboxListAsync(
                    client.ListOutboxAsync(
                        options.TryGetValue("--status", out var statuses) ? statuses : [],
                        options.TryGetValue("--binding", out var binding) ? binding.FirstOrDefault() : null,
                        options.TryGetValue("--session", out var session) ? session.FirstOrDefault() : null,
                        limit.Value,
                        options.TryGetValue("--cursor", out var cursor) ? cursor.FirstOrDefault() : null,
                        default),
                    jsonOutput, stdout, stderr);
            }
            case "dead-letters":
            {
                var limit = ReadLimit(options);
                if (limit is null)
                    return ExitUsage;
                return WriteOutboxListAsync(
                    client.ListDeadLettersAsync(
                        limit.Value,
                        options.TryGetValue("--cursor", out var cursor) ? cursor.FirstOrDefault() : null,
                        default),
                    jsonOutput, stdout, stderr);
            }
            case "get" when positional.Count == 3:
            {
                if (!Guid.TryParse(positional[2], out var id))
                {
                    stderr.WriteLine($"'<id>' must be a GUID (got '{positional[2]}').");
                    return ExitUsage;
                }

                return WriteRaw(() => client.GetOutboxAsync(id, default), jsonOutput, stdout, stderr);
            }
            case "replay" when positional.Count >= 3:
            {
                if (!Guid.TryParse(positional[2], out var id))
                {
                    stderr.WriteLine($"'<id>' must be a GUID (got '{positional[2]}').");
                    return ExitUsage;
                }

                return RunReplay(
                    client, id,
                    options.TryGetValue("--reason", out var reason) ? reason.FirstOrDefault() : null,
                    confirm: flags.Contains("--yes"),
                    jsonOutput, stdout, stderr);
            }
            case "replay":
            case "get":
                stderr.WriteLine(Usage);
                return ExitUsage;
            default:
                return UnknownCommand($"outbox {subCommand}", stderr);
        }
    }

    private static int RunReplay(
        AdminClient client,
        Guid id,
        string? reason,
        bool confirm,
        bool json,
        TextWriter stdout,
        TextWriter stderr)
    {
        if (!confirm)
        {
            // Print the row summary first so operators see what they are about to replay.
            var detailJson = client.GetOutboxAsync(id, default).ConfigureAwait(false).GetAwaiter().GetResult();
            using var document = JsonDocument.Parse(detailJson);
            var root = document.RootElement;
            stdout.WriteLine($"Replay target : {id}");
            stdout.WriteLine($"Status        : {root.GetProperty("status").GetString()}");
            stdout.WriteLine($"Binding       : {root.GetProperty("bindingId").GetString()}");
            stdout.WriteLine($"Skill         : {root.GetProperty("skill").GetString()}");
            stdout.WriteLine($"Replays so far: {root.GetProperty("replayCount").GetInt32()}");
            stdout.WriteLine("Re-run with --yes to confirm the replay.");
            return ExitUsage;
        }

        return WriteRaw(() => client.ReplayAsync(id, reason, default), json, stdout, stderr);
    }

    private static int RunCluster(
        AdminClient client,
        List<string> positional,
        bool json,
        TextWriter stdout,
        TextWriter stderr)
    {
        if (positional.Count == 1 || positional[1] != "status")
        {
            stderr.WriteLine("Usage: drasiwakectl cluster status");
            return ExitUsage;
        }

        return WriteRaw(() => client.GetClusterAsync(default), json, stdout, stderr);
    }

    private static int WriteOutboxListAsync(
        Task<OutboxListResponse> request,
        bool json,
        TextWriter stdout,
        TextWriter stderr)
    {
        var response = request.ConfigureAwait(false).GetAwaiter().GetResult();
        if (json)
        {
            stdout.WriteLine(JsonSerializer.Serialize(response, AdminClient.JsonOptions));
            return ExitSuccess;
        }

        WriteLineTable(stdout,
            ["ID", "STATUS", "BINDING", "SESSION", "SKILL", "REPLAYS", "ATTEMPTS", "CREATED (UTC)", "NEXT ATTEMPT (UTC)", "TARGET"],
            response.Items.Select(item => new[]
            {
                ShortId(item.Id),
                item.Status.ToString(),
                item.BindingId,
                item.SessionId,
                item.Skill,
                item.ReplayCount.ToString(CultureInfo.InvariantCulture),
                item.AttemptCount.ToString(CultureInfo.InvariantCulture),
                FormatTimestamp(item.CreatedAtUtc),
                FormatTimestamp(item.NextAttemptAtUtc),
                item.OpenClawTarget
            }));

        if (response.NextCursor is { } cursor)
        {
            stdout.WriteLine();
            stdout.WriteLine($"Next page cursor: {cursor}");
        }
        else
        {
            stdout.WriteLine();
            stdout.WriteLine($"{response.Items.Length} row(s).");
        }

        return ExitSuccess;
    }

    private static int WriteRaw(Func<Task<string>> request, bool json, TextWriter stdout, TextWriter stderr)
    {
        var body = request().ConfigureAwait(false).GetAwaiter().GetResult();
        if (json)
        {
            stdout.WriteLine(body);
            return ExitSuccess;
        }

        WriteReadableJson(stdout, body);
        return ExitSuccess;
    }

    private static int WriteRaw(AdminClient client, string requestUri, bool json, bool rawJson, TextWriter stdout, TextWriter stderr)
        => WriteRaw(() => client.GetStringAsync(requestUri, default), json || rawJson, stdout, stderr);

    private static int WriteUsage(TextWriter stdout)
    {
        stdout.WriteLine(Usage);
        return ExitSuccess;
    }

    private static int UnknownCommand(string command, TextWriter stderr)
    {
        stderr.WriteLine($"Unknown command '{command}'.");
        stderr.WriteLine(Usage);
        return ExitUsage;
    }

    private static int? ReadLimit(Dictionary<string, List<string>> options)
    {
        if (!options.TryGetValue("--limit", out var values) || values.Count == 0)
            return 50;
        if (!int.TryParse(values[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ||
            parsed < 1 || parsed > 500)
        {
            return null;
        }

        return parsed;
    }

    private static string ShortId(Guid id) => id.ToString("N")[..8];

    private static string FormatTimestamp(DateTimeOffset timestamp) =>
        timestamp.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss 'Z'", CultureInfo.InvariantCulture);

    private static void WriteLineTable(TextWriter writer, string[] headers, IEnumerable<string[]> rows)
    {
        var materialized = rows.ToList();
        var widths = new int[headers.Length];
        for (var column = 0; column < headers.Length; column++)
        {
            widths[column] = headers[column].Length;
            foreach (var row in materialized)
                widths[column] = Math.Max(widths[column], row[column].Length);
        }

        writer.WriteLine(FormatRow(headers, widths));
        writer.WriteLine(string.Join("  ", widths.Select(width => new string('-', width))));
        foreach (var row in materialized)
            writer.WriteLine(FormatRow(row, widths));
    }

    private static string FormatRow(string[] cells, int[] widths) =>
        string.Join("  ", cells.Select((cell, index) => cell.PadRight(widths[index])));

    private static void WriteReadableJson(TextWriter writer, string body)
    {
        try
        {
            using var document = JsonDocument.Parse(body);
            writer.WriteLine(JsonSerializer.Serialize(document.RootElement, new JsonSerializerOptions(AdminClient.JsonOptions)
            {
                WriteIndented = true
            }));
        }
        catch (JsonException)
        {
            writer.WriteLine(body);
        }
    }
}
