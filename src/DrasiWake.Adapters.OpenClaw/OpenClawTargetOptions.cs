using System.Net.Http.Headers;

namespace DrasiWake.Adapters.OpenClaw;

public sealed record OpenClawTargetOptions(
    Uri BaseAddress,
    string? BearerToken,
    TimeSpan GatewayIdempotencyRetention)
{
    public void Validate(string targetName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetName);
        if (!BaseAddress.IsAbsoluteUri ||
            (BaseAddress.Scheme != Uri.UriSchemeHttp && BaseAddress.Scheme != Uri.UriSchemeHttps) ||
            !string.IsNullOrEmpty(BaseAddress.UserInfo))
        {
            throw new InvalidOperationException(
                $"OpenClaw target '{targetName}' BaseAddress must be an absolute HTTP or HTTPS URI without embedded credentials.");
        }

        if (GatewayIdempotencyRetention <= TimeSpan.Zero)
            throw new InvalidOperationException($"OpenClaw target '{targetName}' GatewayIdempotencyRetention must be positive.");

        if (BearerToken is null)
            return;
        if (string.IsNullOrWhiteSpace(BearerToken))
            throw new InvalidOperationException($"OpenClaw target '{targetName}' BearerToken cannot be empty.");
        if (!IsValidBearerToken(BearerToken))
            throw new InvalidOperationException($"OpenClaw target '{targetName}' BearerToken format is invalid.");

        try
        {
            _ = new AuthenticationHeaderValue("Bearer", BearerToken);
        }
        catch (FormatException exception)
        {
            throw new InvalidOperationException(
                $"OpenClaw target '{targetName}' BearerToken format is invalid.",
                exception);
        }
    }

    private static bool IsValidBearerToken(string token)
    {
        var paddingStart = token.IndexOf('=');
        var tokenLength = paddingStart < 0 ? token.Length : paddingStart;
        if (tokenLength == 0)
            return false;

        for (var index = 0; index < tokenLength; index++)
        {
            var character = token[index];
            if (!char.IsAsciiLetterOrDigit(character) &&
                character is not '-' and not '.' and not '_' and not '~' and not '+' and not '/')
            {
                return false;
            }
        }

        for (var index = tokenLength; index < token.Length; index++)
        {
            if (token[index] != '=')
                return false;
        }

        return true;
    }
}
