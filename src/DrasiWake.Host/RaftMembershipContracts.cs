using System.Runtime.Serialization;
using System.ServiceModel;
using ProtoBuf;
using ProtoBuf.Grpc;

namespace DrasiWake.Host;

public sealed record ClusterCompatibility(
    string NodeId,
    string ApplicationVersion,
    string ConfigurationFingerprint,
    int CommandSchemaVersion = ClusterCompatibility.CurrentCommandSchemaVersion,
    int EnvelopeSchemaVersion = 1)
{
    public const int CurrentCommandSchemaVersion = 1;
}

public interface IRaftMembershipManager
{
    Task AddMemberAsync(Uri endpoint, CancellationToken cancellationToken);
    Task RemoveMemberAsync(Uri endpoint, CancellationToken cancellationToken);
}

public interface IClusterCompatibilityProvider
{
    Task<ClusterCompatibilityResponse> GetLocalCompatibilityAsync(CancellationToken cancellationToken);
}

public interface IClusterCompatibilityProbe
{
    Task<ClusterCompatibility> GetCompatibilityAsync(Uri endpoint, CancellationToken cancellationToken);
}

public interface IClusterMembershipForwarder
{
    Task ForwardAsync(
        Uri managementEndpoint,
        ClusterMemberRequest request,
        bool add,
        CancellationToken cancellationToken);
}

[ServiceContract(Name = "DrasiWakeClusterMembership")]
public interface IClusterMembershipService
{
    [OperationContract]
    Task<ClusterCompatibilityResponse> GetCompatibility(
        ClusterCompatibilityRequest request,
        CallContext context = default);

    [OperationContract]
    Task<ClusterManagementResponse> Add(
        ClusterMemberRequest request,
        CallContext context = default);

    [OperationContract]
    Task<ClusterManagementResponse> Remove(
        ClusterMemberRequest request,
        CallContext context = default);
}

[ProtoContract]
[DataContract]
public sealed class ClusterCompatibilityRequest
{
}

[ProtoContract]
[DataContract]
public sealed class ClusterCompatibilityResponse
{
    [ProtoMember(1), DataMember(Order = 1)]
    public string NodeId { get; set; } = string.Empty;

    [ProtoMember(2), DataMember(Order = 2)]
    public string ApplicationVersion { get; set; } = string.Empty;

    [ProtoMember(3), DataMember(Order = 3)]
    public string ConfigurationFingerprint { get; set; } = string.Empty;

    [ProtoMember(4), DataMember(Order = 4)]
    public int CommandSchemaVersion { get; set; }

    [ProtoMember(5), DataMember(Order = 5)]
    public int EnvelopeSchemaVersion { get; set; }
}

[ProtoContract]
[DataContract]
public sealed class ClusterMemberRequest
{
    [ProtoMember(1), DataMember(Order = 1)]
    public string Endpoint { get; set; } = string.Empty;

    [ProtoMember(2), DataMember(Order = 2)]
    public string? ExpectedLeaderNodeId { get; set; }

    [ProtoMember(3), DataMember(Order = 3)]
    public long? ExpectedLeaderTerm { get; set; }
}

[ProtoContract]
[DataContract]
public sealed class ClusterManagementResponse
{
    [ProtoMember(1), DataMember(Order = 1)]
    public bool Succeeded { get; set; }
}
