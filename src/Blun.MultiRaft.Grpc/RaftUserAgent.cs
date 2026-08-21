// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström
//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Blun.MultiRaft.Grpc;

/// <summary>
/// The <c>User-Agent</c> this transport presents on every peer connection: which client library, which
/// version, on which runtime and OS. Two independent reasons to send it, not one: a WAF or reverse proxy
/// commonly filters out HTTP traffic carrying no user agent at all, and grpc-dotnet's own generic
/// <c>grpc-dotnet/x.y</c> token does not say which library is calling through it, which matters once more than
/// one system on a node shares the same runtime.
/// </summary>
/// <remarks>
/// Shaped like a normal HTTP user agent — <c>product/version (comment)</c>, most significant product first —
/// because that is what every log pipeline and dashboard already knows how to parse. grpc-dotnet's own token is
/// kept and appended rather than replaced (see <see cref="UserAgentHandler"/>): it identifies the transport,
/// which is a different question from who the caller is.
/// </remarks>
public static class RaftUserAgent
{
    private const string ProductName = "Blun.MultiRaft.Grpc";

    private static string? _default;

    /// <summary>
    /// This library's own user agent, e.g. <c>Blun.MultiRaft.Grpc/1.0.0 (.NET 10.0.0; Windows)</c>. Computed
    /// once — the parts it is built from cannot change within a process.
    /// </summary>
    public static string Default => _default ??= Build();

    /// <summary>
    /// Combines <paramref name="application"/> with <see cref="Default"/>, so a host can name itself without
    /// losing which transport library it is calling through. Returns <see cref="Default"/> alone when the
    /// application supplies nothing.
    /// </summary>
    public static string ForApplication(string? application)
        => string.IsNullOrWhiteSpace(application) ? Default : $"{Sanitize(application)} {Default}";

    /// <summary>
    /// Strips what must never reach a header value. Control characters — CR and LF above all — would either
    /// split the header or be rejected outright by <c>HttpHeaders</c>, and this value can come from application
    /// configuration, so it is not this library's to trust. Silently cleaned rather than rejected: a user agent
    /// is diagnostic metadata, and failing a connection over a stray character in it would be the worse outcome.
    /// </summary>
    [return: NotNullIfNotNull(nameof(value))]
    internal static string? Sanitize(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return value;
        }

        Span<char> buffer = value.Length <= 256 ? stackalloc char[value.Length] : new char[value.Length];
        var length = 0;

        foreach (char c in value)
        {
            // Printable ASCII only. Anything else (control characters, and non-ASCII, which HttpHeaders would
            // mangle into Latin-1) becomes a space, so word boundaries in the original survive instead of two
            // tokens being glued together.
            buffer[length++] = c is >= ' ' and <= '~' ? c : ' ';
        }

        return new string(buffer[..length]).Trim();
    }

    private static string Build()
    {
        string? version = typeof(RaftUserAgent).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? typeof(RaftUserAgent).Assembly.GetName().Version?.ToString()
            ?? "0.0.0";

        // SourceLink appends "+<commit sha>" to the informational version. Useful in a build log, noise in a
        // user agent -- and it makes the version fail to parse for anyone grouping by it.
        int plus = version.IndexOf('+');
        if (plus >= 0)
        {
            version = version[..plus];
        }

        // RuntimeInformation.OSDescription carries the full build string ("Microsoft Windows 10.0.26200"); the
        // platform alone is what one actually filters on, and it does not turn the agent into a fingerprint.
        string os = OperatingSystem.IsWindows() ? "Windows"
            : OperatingSystem.IsLinux() ? "Linux"
            : OperatingSystem.IsMacOS() ? "macOS"
            : "unknown";

        return Sanitize($"{ProductName}/{version} ({RuntimeInformation.FrameworkDescription}; {os})");
    }
}
