using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using DrasiWake.Core.Abstractions;
using DrasiWake.Core.Domain;

namespace DrasiWake.Adapters.DrasiServer;

public sealed class DrasiChangeSource : IChangeSource
{
    private readonly DrasiServerClient _client;
    private readonly DrasiServerOptions _options;
    private readonly TimeProvider _timeProvider;

    public DrasiChangeSource(
        DrasiServerClient client,
        DrasiServerOptions options,
        TimeProvider? timeProvider = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _options.Validate();
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public async IAsyncEnumerable<ChangeSignal> WatchAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var queries = (await EnumerateQueriesAsync(cancellationToken)).Distinct().ToArray();
        if (queries.Length == 0)
        {
            yield break;
        }

        var channel = Channel.CreateBounded<ChangeSignal>(new BoundedChannelOptions(_options.SignalCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = queries.Length == 1
        });
        using var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var workers = queries
            .Select(query => RunQueryAsync(query, channel.Writer, linkedCancellation.Token))
            .ToArray();
        var workersCompletion = Task.WhenAll(workers);

        try
        {
            while (true)
            {
                var waitToRead = channel.Reader.WaitToReadAsync(cancellationToken).AsTask();
                var completed = await Task.WhenAny(waitToRead, workersCompletion);
                if (completed == workersCompletion)
                {
                    await workersCompletion;
                    yield break;
                }

                if (!await waitToRead)
                {
                    yield break;
                }

                while (channel.Reader.TryRead(out var signal))
                {
                    yield return signal;
                }
            }
        }
        finally
        {
            linkedCancellation.Cancel();
            channel.Writer.TryComplete();
            await workersCompletion;
        }
    }

    public ValueTask<IReadOnlyList<QueryIdentity>> EnumerateQueriesAsync(CancellationToken cancellationToken) =>
        _client.EnumerateQueriesAsync(_options.ServerUri, cancellationToken);

    public ValueTask<QuerySnapshot> ReadSnapshotAsync(QueryIdentity query, CancellationToken cancellationToken) =>
        _client.ReadSnapshotAsync(query, cancellationToken);

    private async Task RunQueryAsync(
        QueryIdentity query,
        ChannelWriter<ChangeSignal> writer,
        CancellationToken cancellationToken)
    {
        var reconnectDelay = _options.InitialReconnectDelay;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await foreach (var signal in _client.AttachAsync(query, cancellationToken)
                                   .WithCancellation(cancellationToken))
                {
                    await writer.WriteAsync(signal, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception) when (IsRecoverable(exception))
            {
            }

            try
            {
                await Task.Delay(reconnectDelay, _timeProvider, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            reconnectDelay = TimeSpan.FromMilliseconds(
                Math.Min(_options.MaxReconnectDelay.TotalMilliseconds, reconnectDelay.TotalMilliseconds * 2));
        }
    }

    private static bool IsRecoverable(Exception exception) => exception is
        HttpRequestException or IOException or JsonException ||
        exception is OperationCanceledException;
}