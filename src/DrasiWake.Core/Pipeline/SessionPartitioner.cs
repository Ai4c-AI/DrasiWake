using DrasiWake.Core.Contracts;
using DrasiWake.Core.Domain;

namespace DrasiWake.Core.Pipeline;

public sealed class SessionPartitioner : IAsyncDisposable
{
    private readonly int workerCount;
    private readonly ContractRegistryManager registry;
    private readonly Func<WakeOutboxItem, CancellationToken, ValueTask> processAsync;
    private readonly TimeProvider timeProvider;
    private readonly SemaphoreSlim capacitySlots;
    private readonly SemaphoreSlim readySignal = new(0);
    private readonly object stateLock = new();
    private readonly Dictionary<SessionKey, Queue<WakeOutboxItem>> sessionQueues = [];
    private readonly HashSet<Guid> queuedOutboxIds = [];
    private readonly Queue<SessionKey> readySessions = new();
    private readonly HashSet<SessionKey> scheduledSessions = [];
    private readonly HashSet<SessionKey> activeSessions = [];
    private readonly Dictionary<string, BindingRateLimiter> bindingLimiters = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource shutdown = new();
    private int runStarted;
    private bool disposed;
    private Task? runTask;

    public SessionPartitioner(
        int capacity,
        int maxConcurrency,
        ContractRegistryManager registry,
        Func<WakeOutboxItem, CancellationToken, ValueTask> processAsync,
        TimeProvider? timeProvider = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrency, 1);
        this.registry = registry ?? throw new ArgumentNullException(nameof(registry));
        this.processAsync = processAsync ?? throw new ArgumentNullException(nameof(processAsync));
        this.timeProvider = timeProvider ?? TimeProvider.System;
        capacitySlots = new SemaphoreSlim(capacity, capacity);
        workerCount = maxConcurrency;
    }

    public async ValueTask EnqueueAsync(WakeOutboxItem item, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(item);
        ObjectDisposedException.ThrowIf(disposed, this);
        lock (stateLock)
        {
            if (queuedOutboxIds.Contains(item.Id))
                return;
        }

        await capacitySlots.WaitAsync(cancellationToken);
        try
        {
            var sessionKey = new SessionKey(item.BindingId, item.SessionId);
            lock (stateLock)
            {
                if (!queuedOutboxIds.Add(item.Id))
                {
                    capacitySlots.Release();
                    return;
                }
                if (!sessionQueues.TryGetValue(sessionKey, out var queue))
                {
                    queue = new Queue<WakeOutboxItem>();
                    sessionQueues.Add(sessionKey, queue);
                }
                queue.Enqueue(item);
                ScheduleSession(sessionKey);
            }
        }
        catch
        {
            capacitySlots.Release();
            throw;
        }
    }

    public Task RunAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref runStarted, 1) != 0)
            throw new InvalidOperationException("SessionPartitioner can only be run once.");

        runTask = RunWorkersAsync(cancellationToken);
        return runTask;
    }

    public async Task StopAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);
        shutdown.Cancel();
        if (runTask is null)
            return;

        try
        {
            await runTask.WaitAsync(timeout, timeProvider, cancellationToken);
        }
        catch (OperationCanceledException) when (shutdown.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
        }
    }

    public ValueTask DisposeAsync()
    {
        if (disposed)
            return ValueTask.CompletedTask;

        disposed = true;
        shutdown.Cancel();
        foreach (var limiter in bindingLimiters.Values)
            limiter.Dispose();
        capacitySlots.Dispose();
        readySignal.Dispose();
        shutdown.Dispose();
        return ValueTask.CompletedTask;
    }

    private async Task RunWorkersAsync(CancellationToken cancellationToken)
    {
        using var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, shutdown.Token);
        var workers = Enumerable.Range(0, workerCount)
            .Select(_ => RunWorkerAsync(linkedCancellation.Token))
            .ToArray();
        try
        {
            var firstWorker = await Task.WhenAny(workers);
            if (!linkedCancellation.IsCancellationRequested && (firstWorker.IsFaulted || firstWorker.IsCanceled))
                linkedCancellation.Cancel();
            await Task.WhenAll(workers);
        }
        catch
        {
            linkedCancellation.Cancel();
            try
            {
                await Task.WhenAll(workers);
            }
            catch
            {
            }

            throw;
        }
    }

    private async Task RunWorkerAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            await readySignal.WaitAsync(cancellationToken);
            WakeOutboxItem? item = null;
            SessionKey? sessionKey = null;
            lock (stateLock)
            {
                while (readySessions.Count > 0)
                {
                    var candidate = readySessions.Dequeue();
                    scheduledSessions.Remove(candidate);
                    if (!activeSessions.Add(candidate))
                        continue;
                    if (!sessionQueues.TryGetValue(candidate, out var queue) || queue.Count == 0)
                    {
                        activeSessions.Remove(candidate);
                        sessionQueues.Remove(candidate);
                        continue;
                    }

                    sessionKey = candidate;
                    item = queue.Dequeue();
                    break;
                }
            }

            if (item is null || sessionKey is null)
                continue;

            try
            {
                await WaitUntilDueAsync(item, cancellationToken);
                await WaitForBindingPermitAsync(item, cancellationToken);
                await processAsync(item, cancellationToken);
            }
            finally
            {
                lock (stateLock)
                {
                    queuedOutboxIds.Remove(item.Id);
                    activeSessions.Remove(sessionKey.Value);
                    if (sessionQueues.TryGetValue(sessionKey.Value, out var queue) && queue.Count > 0)
                    {
                        ScheduleSession(sessionKey.Value);
                    }
                    else
                    {
                        sessionQueues.Remove(sessionKey.Value);
                    }
                }
                capacitySlots.Release();
            }
        }
    }

    private async ValueTask WaitForBindingPermitAsync(WakeOutboxItem item, CancellationToken cancellationToken)
    {
        var binding = registry.Active.Bindings.FirstOrDefault(candidate => candidate.Id == item.BindingId);
        if (binding is null)
            return;

        BindingRateLimiter limiter;
        lock (stateLock)
        {
            if (!bindingLimiters.TryGetValue(binding.Id, out limiter!))
            {
                limiter = new BindingRateLimiter(binding.RateLimit, timeProvider);
                bindingLimiters.Add(binding.Id, limiter);
            }
        }
        await limiter.WaitAsync(cancellationToken);
    }

    private async ValueTask WaitUntilDueAsync(WakeOutboxItem item, CancellationToken cancellationToken)
    {
        var maximumDelay = TimeSpan.FromMilliseconds(int.MaxValue);
        while (true)
        {
            var delay = item.NextAttemptAtUtc - timeProvider.GetUtcNow();
            if (delay <= TimeSpan.Zero)
                return;
            await Task.Delay(delay > maximumDelay ? maximumDelay : delay, timeProvider, cancellationToken);
        }
    }

    private void ScheduleSession(SessionKey sessionKey)
    {
        if (activeSessions.Contains(sessionKey) || !scheduledSessions.Add(sessionKey))
            return;
        readySessions.Enqueue(sessionKey);
        readySignal.Release();
    }

    private readonly record struct SessionKey(string BindingId, string SessionId);

    private sealed class BindingRateLimiter : IDisposable
    {
        private readonly int permitLimit;
        private readonly TimeSpan window;
        private readonly TimeProvider timeProvider;
        private readonly SemaphoreSlim gate = new(1, 1);
        private DateTimeOffset windowStartedAtUtc;
        private int permitsUsed;
        private bool hasWindow;

        public BindingRateLimiter(RateLimitPolicy policy, TimeProvider timeProvider)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(policy.PermitLimit, 1);
            ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(policy.Window, TimeSpan.Zero);
            permitLimit = policy.PermitLimit;
            window = policy.Window;
            this.timeProvider = timeProvider;
        }

        public async ValueTask WaitAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                TimeSpan delay;
                await gate.WaitAsync(cancellationToken);
                try
                {
                    var now = timeProvider.GetUtcNow();
                    if (!hasWindow || now - windowStartedAtUtc >= window)
                    {
                        windowStartedAtUtc = now;
                        permitsUsed = 0;
                        hasWindow = true;
                    }

                    if (permitsUsed < permitLimit)
                    {
                        permitsUsed++;
                        return;
                    }

                    delay = window - (now - windowStartedAtUtc);
                }
                finally
                {
                    gate.Release();
                }

                await Task.Delay(delay, timeProvider, cancellationToken);
            }
        }

        public void Dispose() => gate.Dispose();
    }
}