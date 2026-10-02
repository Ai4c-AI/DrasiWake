using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using DrasiWake.Core.Domain;

namespace DrasiWake.Adapters.DrasiServer;

public static class DrasiSseReader
{
    public static async IAsyncEnumerable<ChangeSignal> ReadAsync(
        Stream stream,
        QueryIdentity query,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(query);

        using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
        var dataLines = new List<string>();
        string? signalId = null;

        while (true)
        {
            var line = await reader.ReadLineAsync(cancellationToken);
            if (line is null || line.Length == 0)
            {
                var signal = CreateSignal(query, dataLines, signalId);
                dataLines.Clear();
                signalId = null;
                if (signal is not null)
                {
                    yield return signal;
                }

                if (line is null)
                {
                    yield break;
                }

                continue;
            }

            if (line[0] == ':')
            {
                continue;
            }

            var separator = line.IndexOf(':');
            var field = separator < 0 ? line : line[..separator];
            var value = separator < 0 ? string.Empty : line[(separator + 1)..];
            if (value.StartsWith(' '))
            {
                value = value[1..];
            }

            if (field == "data")
            {
                dataLines.Add(value);
            }
            else if (field == "id" && !value.Contains('\0'))
            {
                signalId = value;
            }
        }
    }

    private static ChangeSignal? CreateSignal(QueryIdentity query, List<string> dataLines, string? signalId)
    {
        if (dataLines.Count == 0)
        {
            return null;
        }

        try
        {
            using var _ = JsonDocument.Parse(string.Join('\n', dataLines));
            return new ChangeSignal(query, DateTimeOffset.UtcNow, signalId);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}