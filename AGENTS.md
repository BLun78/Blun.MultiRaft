# Blun.MultiRaft

Multi-Raft consensus library with leader-driven asymmetric replication and a pluggable write-ahead log.

## Project Structure

- **Core library:** `src/Blun.MultiRaft/`
- **Write-ahead log:** `src/Blun.MultiRaft.Wal/`
- **gRPC transport:** `src/Blun.MultiRaft.Grpc/`
- **Demo node:** `demo/Blun.MultiRaft.Node/`
- **Tests:** `test/Blun.MultiRaft.Tests/`
- **Benchmarks:** `benchmark/Blun.MultiRaft.Benchmarks/`

## Build & Test

```bash
dotnet build src/Blun.MultiRaft/Blun.MultiRaft.csproj
dotnet test test/Blun.MultiRaft.Tests/
```

## Benchmarks

Benchmarks must always run for **both** `net10.0` and `net11.0` in a single invocation to enable runtime-to-runtime comparison of speed and memory allocations:

```bash
dotnet run --project benchmark/Blun.MultiRaft.Benchmarks/Blun.MultiRaft.Benchmarks.csproj -c Release -f net11.0 -- --runtimes net10.0 net11.0
```

When invoked without arguments, the benchmark runner defaults to `--runtimes net10.0 net11.0`.

## Code Conventions

- **Language:** C# with `LangVersion=preview`
- **Frameworks:** .NET 10.0 and .NET 11.0 (multi-target for core libraries)
- **Logging:** ALL logging must use source-generated `[LoggerMessage]` attribute pattern with `partial` methods. Traditional `LogInformation()`, `LogWarning()`, etc. are prohibited. See `src/Blun.MultiRaft/Core/RaftGroupInstanceLog.cs` for examples.
- **AOT compatibility:** Code must be trim-safe and AOT-compatible
- **Error handling:** Throw `InvalidOperationException` for integrity violations; use `IOException` for transport failures

## Logging Pattern

```csharp
internal static partial class MyLog
{
    [LoggerMessage(Level = LogLevel.Information, EventId = 1000, Message = "...")]
    public static partial void SomeEvent(ILogger logger, /* params */);
}

// Call site:
MyLog.SomeEvent(_logger, param1, param2);
```

EventId ranges:
- 1000–1015: Core Raft (`RaftGroupInstance.Log`)
- 1100+: Host (`HostLog`)
- 1200+: Cluster coordinator (`ClusterLog`)
- 2000+: Demo node (`NodeStartedLog`)
