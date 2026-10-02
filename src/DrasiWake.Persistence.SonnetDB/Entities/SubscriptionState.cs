namespace DrasiWake.Persistence.SonnetDB.Entities;

public sealed class SubscriptionState
{
    public string QueryKey { get; set; } = string.Empty;
    public string ServerUri { get; set; } = string.Empty;
    public string? InstanceId { get; set; }
    public string QueryId { get; set; } = string.Empty;
    public bool IsConnected { get; set; }
    public DateTimeOffset? LastConnectedAtUtc { get; set; }
    public DateTimeOffset? LastReconciledAtUtc { get; set; }
    public string? LastErrorCode { get; set; }
}