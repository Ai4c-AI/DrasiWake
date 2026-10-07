using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using ProtoBuf.Grpc;
using DrasiWake.Core.Pipeline;

namespace DrasiWake.Host;

public sealed class ClusterMembershipGrpcService(
    RaftClusterSettings settings,
    IClusterCompatibilityProvider compatibilityProvider,
    IRaftMembershipManager membershipManager,
    ILogger<ClusterMembershipGrpcService> logger,
    TimeProvider timeProvider) : IClusterMembershipService
{
    private const string AuthorizationHeader = "authorization";
    private const string AuthenticationFailure = "Management authentication failed.";
    private static readonly byte[] AuthenticationScheme = "Bearer "u8.ToArray();
    private static readonly byte[] MissingSecret = [];
    private readonly RaftClusterSettings _settings = settings ?? throw new ArgumentNullException(nameof(settings));
    private readonly IClusterCompatibilityProvider _compatibilityProvider =
        compatibilityProvider ?? throw new ArgumentNullException(nameof(compatibilityProvider));
    private readonly IRaftMembershipManager _membershipManager =
        membershipManager ?? throw new ArgumentNullException(nameof(membershipManager));
    private readonly ILogger<ClusterMembershipGrpcService> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));
    private readonly TimeProvider _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));

    public async Task<ClusterCompatibilityResponse> GetCompatibility(
        ClusterCompatibilityRequest request,
        CallContext context = default)
    {
        _ = request;
        EnsureAuthorized(context);
        try
        {
            return await _compatibilityProvider.GetLocalCompatibilityAsync(context.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw new RpcException(new Status(StatusCode.Cancelled, "Management request was cancelled."));
        }
        catch
        {
            throw new RpcException(new Status(StatusCode.Internal, "Management request failed."));
        }
    }

    public Task<ClusterManagementResponse> Add(ClusterMemberRequest request, CallContext context = default)
        => ChangeMembershipAsync(request, context, add: true);

    public Task<ClusterManagementResponse> Remove(ClusterMemberRequest request, CallContext context = default)
        => ChangeMembershipAsync(request, context, add: false);

    private async Task<ClusterManagementResponse> ChangeMembershipAsync(
        ClusterMemberRequest request,
        CallContext context,
        bool add)
    {
        var operation = add ? "Add" : "Remove";
        var memberId = GetStableMemberIdentifier(request?.Endpoint);
        using var activity = BridgeTelemetry.StartMembership(add, request?.Endpoint);
        var result = "failed";
        try
        {
            EnsureAuthorized(context);
            if (request is null ||
                !Uri.TryCreate(request.Endpoint, UriKind.Absolute, out var endpoint) ||
                endpoint.Scheme != Uri.UriSchemeHttps ||
                !string.IsNullOrWhiteSpace(endpoint.UserInfo) ||
                !string.IsNullOrWhiteSpace(endpoint.Query) ||
                !string.IsNullOrWhiteSpace(endpoint.Fragment) ||
                string.IsNullOrWhiteSpace(endpoint.Host))
            {
                throw new ClusterMembershipException(StatusCode.InvalidArgument, "invalid-endpoint");
            }

            if (request.ExpectedLeaderTerm is not null || request.ExpectedLeaderNodeId is not null)
            {
                if (_membershipManager is not IForwardedRaftMembershipManager forwardedManager ||
                    request.ExpectedLeaderTerm is null ||
                    string.IsNullOrWhiteSpace(request.ExpectedLeaderNodeId))
                {
                    throw new ClusterMembershipException(StatusCode.Aborted, "stale-leader");
                }

                if (add)
                {
                    await forwardedManager.AddForwardedMemberAsync(
                        endpoint,
                        request.ExpectedLeaderNodeId,
                        request.ExpectedLeaderTerm.Value,
                        context.CancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await forwardedManager.RemoveForwardedMemberAsync(
                        endpoint,
                        request.ExpectedLeaderNodeId,
                        request.ExpectedLeaderTerm.Value,
                        context.CancellationToken).ConfigureAwait(false);
                }
            }
            else if (add)
            {
                await _membershipManager.AddMemberAsync(endpoint, context.CancellationToken).ConfigureAwait(false);
            }
            else
            {
                await _membershipManager.RemoveMemberAsync(endpoint, context.CancellationToken).ConfigureAwait(false);
            }

            LogMembershipOperation(operation, memberId, "succeeded");
            result = "succeeded";
            return new ClusterManagementResponse { Succeeded = true };
        }
        catch (ClusterMembershipException exception)
        {
            result = SafeResultCategory(exception.ResultCategory);
            LogMembershipOperation(operation, memberId, result);
            throw SafeRpcException(exception.StatusCode);
        }
        catch (RpcException exception) when (exception.StatusCode is StatusCode.Unauthenticated or StatusCode.Unimplemented)
        {
            result = exception.StatusCode == StatusCode.Unauthenticated ? "unauthenticated" : "disabled";
            throw new RpcException(new Status(exception.StatusCode,
                exception.StatusCode == StatusCode.Unauthenticated ? AuthenticationFailure : "Management is not available."));
        }
        catch (OperationCanceledException)
        {
            result = "cancelled";
            LogMembershipOperation(operation, memberId, "cancelled");
            throw new RpcException(new Status(StatusCode.Cancelled, "Management request was cancelled."));
        }
        catch
        {
            LogMembershipOperation(operation, memberId, "failed");
            throw new RpcException(new Status(StatusCode.Internal, "Management request failed."));
        }
        finally
        {
            activity?.SetTag("result", result);
            BridgeTelemetry.RecordMembership(add, result);
        }
    }

    private static string SafeResultCategory(string category) => category switch
    {
        "invalid-endpoint" or "invalid-management-endpoint" or "stale-leader" or "no-quorum" or
        "duplicate-endpoint" or "duplicate-node-id" or "member-not-found" or "last-member" or
        "incompatible-member" or "tls-trust-failure" or "remote-auth-failure" or "remote-incompatible" or
        "compatibility-unavailable" or "leader-unavailable" or "membership-not-committed" or
        "membership-change-failed" => category,
        _ => "failed"
    };

    private void EnsureAuthorized(CallContext context)
    {
        if (_settings.Mode != RaftClusterMode.Cluster)
            throw new RpcException(new Status(StatusCode.Unimplemented, "Management is not available."));

        var headers = context.RequestHeaders;
        var authorizationHeaders = headers?.Where(header =>
            string.Equals(header.Key, AuthorizationHeader, StringComparison.OrdinalIgnoreCase)).ToArray();
        if (authorizationHeaders is not { Length: 1 } ||
            authorizationHeaders[0].IsBinary ||
            !TryReadBearer(authorizationHeaders[0].Value, out var suppliedToken))
        {
            throw new RpcException(new Status(StatusCode.Unauthenticated, AuthenticationFailure));
        }

        var expectedToken = Encoding.UTF8.GetBytes(_settings.ManagementBearerToken ?? string.Empty);
        var expectedDigest = SHA256.HashData(expectedToken);
        var suppliedDigest = SHA256.HashData(suppliedToken);
        if (!CryptographicOperations.FixedTimeEquals(expectedDigest, suppliedDigest))
            throw new RpcException(new Status(StatusCode.Unauthenticated, AuthenticationFailure));
    }

    private static bool TryReadBearer(string? authorization, out byte[] token)
    {
        token = MissingSecret;
        if (string.IsNullOrEmpty(authorization) ||
            authorization.Length < AuthenticationScheme.Length ||
            !authorization.StartsWith("Bearer ", StringComparison.Ordinal) ||
            authorization.Length > 8192)
        {
            return false;
        }

        var tokenText = authorization[AuthenticationScheme.Length..];
        if (string.IsNullOrWhiteSpace(tokenText) || tokenText.Any(char.IsWhiteSpace))
            return false;
        token = Encoding.UTF8.GetBytes(tokenText);
        return true;
    }

    private void LogMembershipOperation(string operation, string memberId, string result)
        => _logger.LogInformation(
            "Raft membership {Operation} for member {MemberId} completed with {Result} at {Timestamp}.",
            operation,
            memberId,
            result,
            _timeProvider.GetUtcNow().ToString("O", CultureInfo.InvariantCulture));

    private static string GetStableMemberIdentifier(string? endpoint)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(endpoint ?? string.Empty));
        return Convert.ToHexString(bytes.AsSpan(0, 8)).ToLowerInvariant();
    }

    private static RpcException SafeRpcException(StatusCode statusCode)
    {
        var message = statusCode switch
        {
            StatusCode.InvalidArgument => "Invalid management request.",
            StatusCode.AlreadyExists => "Cluster member already exists.",
            StatusCode.NotFound => "Cluster member was not found.",
            StatusCode.FailedPrecondition => "Cluster membership precondition failed.",
            StatusCode.Aborted => "Cluster leadership changed; retry the request.",
            StatusCode.Unavailable => "Cluster membership change is unavailable.",
            _ => "Management request failed."
        };
        return new RpcException(new Status(statusCode, message));
    }
}

public sealed class ClusterMembershipException : Exception
{
    internal ClusterMembershipException(StatusCode statusCode, string resultCategory)
    {
        StatusCode = statusCode;
        ResultCategory = resultCategory;
    }

    public StatusCode StatusCode { get; }
    public string ResultCategory { get; }
}

internal interface IForwardedRaftMembershipManager : IRaftMembershipManager
{
    Task AddForwardedMemberAsync(
        Uri endpoint,
        string expectedLeaderNodeId,
        long expectedLeaderTerm,
        CancellationToken cancellationToken);

    Task RemoveForwardedMemberAsync(
        Uri endpoint,
        string expectedLeaderNodeId,
        long expectedLeaderTerm,
        CancellationToken cancellationToken);
}
