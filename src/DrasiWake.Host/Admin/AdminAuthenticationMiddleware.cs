using System.Net;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Http;

namespace DrasiWake.Host.Admin;

/// <summary>
/// Admin plane authentication: probe endpoints (<c>/healthz</c>, <c>/readyz</c>) are always
/// anonymous; otherwise a configured token is mandatory for every caller and loopback callers
/// are only allowed when no token is configured.
/// </summary>
public sealed class AdminAuthenticationMiddleware
{
    public const string UnauthorizedCode = "admin.unauthorized";

    private static readonly byte[] MissingToken = [];
    private readonly RequestDelegate next;
    private readonly byte[]? expectedTokenDigest;

    public AdminAuthenticationMiddleware(RequestDelegate next, string? bearerToken)
    {
        this.next = next ?? throw new ArgumentNullException(nameof(next));
        expectedTokenDigest = string.IsNullOrEmpty(bearerToken)
            ? null
            : SHA256.HashData(Encoding.UTF8.GetBytes(bearerToken));
    }

    public async Task InvokeAsync(HttpContext context)
    {
        var path = context.Request.Path.Value ?? string.Empty;
        if (path.Equals("/healthz", StringComparison.OrdinalIgnoreCase) ||
            path.Equals("/readyz", StringComparison.OrdinalIgnoreCase))
        {
            await next(context).ConfigureAwait(false);
            return;
        }

        if (expectedTokenDigest is null)
        {
            if (IsLoopback(context))
            {
                await next(context).ConfigureAwait(false);
                return;
            }

            await RejectAsync(context).ConfigureAwait(false);
            return;
        }

        if (!TryReadBearer(context.Request.Headers.Authorization.ToString(), out var suppliedToken) ||
            !TokenMatches(suppliedToken))
        {
            await RejectAsync(context).ConfigureAwait(false);
            return;
        }

        await next(context).ConfigureAwait(false);
    }

    private bool TokenMatches(byte[] suppliedToken)
    {
        var suppliedDigest = SHA256.HashData(suppliedToken);
        return CryptographicOperations.FixedTimeEquals(expectedTokenDigest!, suppliedDigest);
    }

    private static bool IsLoopback(HttpContext context) =>
        context.Connection.RemoteIpAddress is { } remoteIp && IPAddress.IsLoopback(remoteIp);

    private static bool TryReadBearer(string? authorization, out byte[] token)
    {
        token = MissingToken;
        const string scheme = "Bearer ";
        if (string.IsNullOrEmpty(authorization) ||
            authorization.Length <= scheme.Length ||
            !authorization.StartsWith(scheme, StringComparison.Ordinal) ||
            authorization.Length > 8192)
        {
            return false;
        }

        var tokenText = authorization[scheme.Length..];
        if (string.IsNullOrWhiteSpace(tokenText) || tokenText.Any(char.IsWhiteSpace))
            return false;

        token = Encoding.UTF8.GetBytes(tokenText);
        return true;
    }

    private static async Task RejectAsync(HttpContext context)
    {
        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        context.Response.ContentType = "application/json; charset=utf-8";
        await context.Response.WriteAsync(
            """{"error":{"code":"admin.unauthorized","message":"A valid admin bearer token is required."}}""",
            context.RequestAborted).ConfigureAwait(false);
    }
}
