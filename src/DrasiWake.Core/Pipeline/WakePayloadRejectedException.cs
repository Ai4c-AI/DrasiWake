namespace DrasiWake.Core.Pipeline;

public sealed class WakePayloadRejectedException(string errorCode, string message) : Exception(message)
{
    public string ErrorCode { get; } = errorCode;
}