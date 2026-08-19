// Blun.MultiRaft - multi-Raft consensus with a pluggable write-ahead log
// Copyright (c) 2026 Björn Lundström

//
// Licensed under the MIT License. See the LICENSE file in the repository root
// for the full license text.

using System.IO.MemoryMappedFiles;
using Microsoft.Win32.SafeHandles;

namespace Blun.MultiRaft.Wal;

/// <summary>How the bytes of a segment actually reach the file.</summary>
public enum WalSegmentAccess : byte
{
    /// <summary>
    /// One <see cref="System.IO.RandomAccess"/> write per append or batch. A syscall per write, but the file
    /// is never larger than its contents and the per-segment cost is a single handle.
    /// </summary>
    RandomAccess = 0,

    /// <summary>
    /// The active segment is preallocated and mapped; an append is a memcpy into the mapped view with no
    /// syscall at all. Much faster per append, at the price of reserving a segment's worth of virtual address
    /// space and page-table entries for as long as the segment is being written.
    /// </summary>
    /// <remarks>
    /// That price is the reason this is not the default. With one log per queue, ten thousand groups times a
    /// 4 MB segment is 40 GB of reserved address space — cheap on x64 in principle, but the page tables and
    /// working set are not free, and it works directly against the per-group footprint the whole design is
    /// built around. Turn it on for the queues that are actually hot, not for all of them.
    /// </remarks>
    MemoryMapped = 1,
}

/// <summary>
/// The write side of one segment file. Exists so the choice between a syscall and a mapped view is a
/// per-group option rather than a second <see cref="IRaftWal"/> — segment rolling, the index map, recovery
/// and truncation are identical either way and must not be duplicated.
/// </summary>
internal interface IWalSegmentDevice : IDisposable
{
    ValueTask WriteAsync(ReadOnlyMemory<byte> data, long offset, CancellationToken cancellationToken);

    /// <summary>Forces written bytes to stable storage.</summary>
    void FlushToDisk();
}

/// <summary>One write syscall per append. The conservative default.</summary>
internal sealed class RandomAccessSegmentDevice : IWalSegmentDevice
{
    private readonly SafeFileHandle _handle;

    public RandomAccessSegmentDevice(string path)
        => _handle = File.OpenHandle(
            path,
            FileMode.OpenOrCreate,
            FileAccess.Write,
            FileShare.ReadWrite,
            FileOptions.Asynchronous);

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, long offset, CancellationToken cancellationToken)
        => await RandomAccess.WriteAsync(_handle, data, offset, cancellationToken).ConfigureAwait(false);

    public void FlushToDisk() => RandomAccess.FlushToDisk(_handle);

    public void Dispose() => _handle.Dispose();
}

/// <summary>
/// A preallocated, memory-mapped segment. Appending is a copy into the mapped view — no syscall, no async
/// state machine, no write buffer.
/// </summary>
/// <remarks>
/// Preallocation is what makes this fit rather than fight the format: segments are already fixed-size, and
/// the zero-filled tail of a preallocated file is recognised by recovery as the clean end of the segment
/// (a record header of all zeros), not as a torn write.
/// <para>
/// Durability takes two steps, not one. <see cref="MemoryMappedViewAccessor.Flush"/> pushes the dirty pages
/// of the view into the file, but on Windows that is <c>FlushViewOfFile</c>, which does not by itself flush
/// the drive's cache. The handle is fsynced afterwards, and skipping either half would leave "committed"
/// meaning less than it claims.
/// </para>
/// </remarks>
internal sealed unsafe class MemoryMappedSegmentDevice : IWalSegmentDevice
{
    private readonly SafeFileHandle _handle;
    private readonly MemoryMappedFile _map;
    private readonly MemoryMappedViewAccessor _view;
    private readonly long _capacity;
    private byte* _base;
    private bool _pointerHeld;

    public MemoryMappedSegmentDevice(string path, long capacity)
    {
        _capacity = capacity;
        _handle = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);

        // Grow, never shrink: reopening an active segment must not discard what is already in it.
        if (RandomAccess.GetLength(_handle) < capacity)
        {
            RandomAccess.SetLength(_handle, capacity);
        }

        _map = MemoryMappedFile.CreateFromFile(
            _handle,
            mapName: null,
            capacity,
            MemoryMappedFileAccess.ReadWrite,
            HandleInheritability.None,
            leaveOpen: true);
        _view = _map.CreateViewAccessor(0, capacity, MemoryMappedFileAccess.ReadWrite);
        _view.SafeMemoryMappedViewHandle.AcquirePointer(ref _base);
        _pointerHeld = true;
    }

    public ValueTask WriteAsync(ReadOnlyMemory<byte> data, long offset, CancellationToken cancellationToken)
    {
        if (offset < 0 || offset + data.Length > _capacity)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), offset, "Write runs past the end of the segment.");
        }

        // Synchronous by construction: this is a memcpy into mapped memory, so returning a completed
        // ValueTask is honest rather than a shortcut. Page faults on first touch are the only stall, and
        // they are what preallocation exists to bound.
        data.Span.CopyTo(new Span<byte>(_base + offset, data.Length));
        return ValueTask.CompletedTask;
    }

    public void FlushToDisk()
    {
        _view.Flush();
        RandomAccess.FlushToDisk(_handle);
    }

    public void Dispose()
    {
        if (_pointerHeld)
        {
            _view.SafeMemoryMappedViewHandle.ReleasePointer();
            _pointerHeld = false;
            _base = null;
        }

        _view.Dispose();
        _map.Dispose();
        _handle.Dispose();
    }
}
