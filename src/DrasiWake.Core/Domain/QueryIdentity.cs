namespace DrasiWake.Core.Domain;

public sealed record QueryIdentity
{
	public Uri Server { get; }
	public string? InstanceId { get; }
	public string QueryId { get; }

	public QueryIdentity(Uri server, string? instanceId, string queryId)
	{
		ArgumentNullException.ThrowIfNull(server);
		ArgumentException.ThrowIfNullOrWhiteSpace(queryId);
		if (!server.IsAbsoluteUri)
		{
			throw new ArgumentException("Server URI must be absolute.", nameof(server));
		}

		var builder = new UriBuilder(server)
		{
			Scheme = server.Scheme.ToLowerInvariant(),
			Host = server.IdnHost.ToLowerInvariant(),
			Query = string.Empty,
			Fragment = string.Empty,
			Path = server.AbsolutePath.TrimEnd('/')
		};
		if (builder.Path.Length == 0)
		{
			builder.Path = "/";
		}

		Server = builder.Uri;
		InstanceId = string.IsNullOrWhiteSpace(instanceId) ? null : instanceId.Trim();
		QueryId = queryId.Trim();
	}
}