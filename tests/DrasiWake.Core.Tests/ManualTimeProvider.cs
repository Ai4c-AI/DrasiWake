namespace DrasiWake.Core.Tests;

internal sealed class ManualTimeProvider(DateTimeOffset initialUtc) : TimeProvider
{
    private readonly object gate = new();
    private readonly List<ManualTimer> timers = [];
    private DateTimeOffset utcNow = initialUtc;
    private TaskCompletionSource timerCreated = NewSignal();
    private int createdTimerCount;

    public override DateTimeOffset GetUtcNow()
    {
        lock (gate)
            return utcNow;
    }

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp()
    {
        lock (gate)
            return utcNow.UtcTicks;
    }

    public override ITimer CreateTimer(
        TimerCallback callback,
        object? state,
        TimeSpan dueTime,
        TimeSpan period)
    {
        ArgumentNullException.ThrowIfNull(callback);
        var timer = new ManualTimer(this, callback, state);
        lock (gate)
        {
            timers.Add(timer);
            createdTimerCount++;
            timer.SetDueTime(dueTime, period, utcNow);
            timerCreated.TrySetResult();
        }
        return timer;
    }

    public async Task WaitForTimerAsync(CancellationToken cancellationToken)
    {
        Task wait;
        lock (gate)
        {
            if (createdTimerCount > 0)
                return;
            wait = timerCreated.Task;
        }
        await wait.WaitAsync(TimeSpan.FromSeconds(3), cancellationToken);
    }

    public void Advance(TimeSpan amount)
    {
        List<(TimerCallback Callback, object? State)> callbacks = [];
        lock (gate)
        {
            utcNow = utcNow.Add(amount);
            foreach (var timer in timers)
            {
                if (timer.TryFire(utcNow, out var callback))
                    callbacks.Add(callback);
            }
        }

        foreach (var (callback, state) in callbacks)
            callback(state);
    }

    private static TaskCompletionSource NewSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class ManualTimer(
        ManualTimeProvider owner,
        TimerCallback callback,
        object? state) : ITimer
    {
        private DateTimeOffset? dueAtUtc;
        private TimeSpan period;
        private bool disposed;

        public bool Change(TimeSpan dueTime, TimeSpan newPeriod)
        {
            lock (owner.gate)
            {
                if (disposed)
                    return false;
                SetDueTime(dueTime, newPeriod, owner.utcNow);
                return true;
            }
        }

        public void Dispose()
        {
            lock (owner.gate)
            {
                disposed = true;
                dueAtUtc = null;
            }
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }

        public bool TryFire(DateTimeOffset nowUtc, out (TimerCallback Callback, object? State) result)
        {
            if (disposed || dueAtUtc is null || dueAtUtc > nowUtc)
            {
                result = default;
                return false;
            }

            if (period > TimeSpan.Zero && period != Timeout.InfiniteTimeSpan)
                dueAtUtc = nowUtc.Add(period);
            else
                dueAtUtc = null;
            result = (callback, state);
            return true;
        }

        public void SetDueTime(TimeSpan dueTime, TimeSpan newPeriod, DateTimeOffset nowUtc)
        {
            period = newPeriod;
            dueAtUtc = dueTime == Timeout.InfiniteTimeSpan ? null : nowUtc.Add(dueTime);
        }
    }
}