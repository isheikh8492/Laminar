using System.Diagnostics;
using System.Threading.Channels;
using Laminar.Core;

namespace Laminar.Ingestion;

/// <summary>
/// The waiting area between sources and the ingestion worker (ADR 0002): a fixed-size queue of
/// batches that absorbs bursts and applies an explicit <see cref="FullBufferPolicy"/> when full.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
/// <remarks>
/// <para>
/// <b>Write side:</b> sources push through <see cref="WriteAsync"/> (this class is their
/// <see cref="IBatchWriter{T}"/>). Several sources may write at the same time.
/// </para>
/// <para>
/// <b>Read side:</b> exactly one reader, the ingestion worker, takes batches in order with
/// <see cref="ReadAsync"/>.
/// </para>
/// <para>
/// <b>Never silent:</b> every batch that is dropped, for any reason, is disposed and counted.
/// Counters are updated under one lock, so <see cref="GetCounters"/> always returns numbers that
/// add up. The lock is taken once per batch, which is cheap at batch rates.
/// </para>
/// <para>
/// Built on <see cref="System.Threading.Channels"/>, which supplies the thread-safe queue and
/// waiting. This class adds the policy mapping, drop handling and accounting.
/// </para>
/// </remarks>
public sealed class BoundedBuffer<T> : IBatchWriter<T>
{
    private readonly Channel<Batch<T>> _channel;

    // Guards the counters together with every TryWrite and TryRead on the channel, so a snapshot
    // never sees a batch counted on one side but not the other.
    private readonly object _gate = new();

    private long _batchesWritten;
    private long _recordsWritten;
    private long _batchesDropped;
    private long _recordsDropped;
    private long _batchesTaken;
    private long _recordsTaken;

    /// <summary>
    /// Creates an empty buffer.
    /// </summary>
    /// <param name="capacity">How many batches the buffer holds before the policy applies.</param>
    /// <param name="policy">What to do when a batch arrives and the buffer is full.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="capacity"/> is zero or negative, or <paramref name="policy"/> is not a defined value.
    /// </exception>
    public BoundedBuffer(int capacity, FullBufferPolicy policy)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);

        var options = new BoundedChannelOptions(capacity)
        {
            FullMode = ToChannelMode(policy),
            SingleReader = true,
            SingleWriter = false,

            // Don't run the reader's continuation on the writer's thread: a source must never be
            // slowed down by ingestion work.
            AllowSynchronousContinuations = false,
        };

        Capacity = capacity;
        Policy = policy;
        _channel = Channel.CreateBounded<Batch<T>>(options, OnBatchDropped);
    }

    /// <summary>How many batches the buffer holds before the policy applies.</summary>
    public int Capacity { get; }

    /// <summary>What the buffer does when a batch arrives and it is full.</summary>
    public FullBufferPolicy Policy { get; }

    /// <summary>
    /// Pushes a batch into the buffer, applying <see cref="Policy"/> if the buffer is full.
    /// </summary>
    /// <param name="batch">
    /// The batch to push. Ownership passes to the buffer on every call: if the batch is dropped,
    /// the write is cancelled, or the buffer is already completed, the buffer disposes it.
    /// </param>
    /// <param name="cancellationToken">
    /// Stops waiting for room under <see cref="FullBufferPolicy.Wait"/>. A cancelled write counts
    /// the batch as dropped.
    /// </param>
    /// <exception cref="OperationCanceledException">The write was cancelled while waiting for room.</exception>
    /// <exception cref="InvalidOperationException"><see cref="Complete"/> was already called.</exception>
    public async ValueTask WriteAsync(Batch<T> batch, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batch);

        while (true)
        {
            lock (_gate)
            {
                // Count before writing: under DropIncoming or DropOldest, TryWrite may call
                // OnBatchDropped right away, and the drop must be counted against a written batch.
                int count = batch.Count;
                _batchesWritten++;
                _recordsWritten += count;

                if (_channel.Writer.TryWrite(batch))
                {
                    return;
                }

                // Not written after all. Undo, and decide below whether to wait or reject.
                _batchesWritten--;
                _recordsWritten -= count;
            }

            // Only reachable under Wait with a full buffer, or after Complete.
            bool canWrite;
            try
            {
                canWrite = await _channel.Writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                DropRejected(batch);
                throw;
            }

            if (!canWrite)
            {
                DropRejected(batch);
                throw new InvalidOperationException("The buffer has been completed; no more batches can be written.");
            }
        }
    }

    /// <summary>
    /// Takes the next batch, waiting until one is available.
    /// </summary>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>
    /// The next batch, now owned by the caller, who must dispose it. <see langword="null"/> once
    /// the buffer is completed and empty.
    /// </returns>
    /// <remarks>Only one reader may call this; the buffer is built for a single ingestion worker.</remarks>
    /// <exception cref="OperationCanceledException">The wait was cancelled.</exception>
    public async ValueTask<Batch<T>?> ReadAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            lock (_gate)
            {
                if (_channel.Reader.TryRead(out Batch<T>? batch))
                {
                    _batchesTaken++;
                    _recordsTaken += batch.Count;
                    return batch;
                }
            }

            if (!await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                return null;
            }
        }
    }

    /// <summary>
    /// Marks the buffer as finished: no more writes are accepted. The reader still receives every
    /// batch already buffered, then <see cref="ReadAsync"/> returns <see langword="null"/>.
    /// </summary>
    /// <param name="error">
    /// The reason, if the buffer is completed because a source failed. The reader sees it as an
    /// exception after draining the buffer.
    /// </param>
    /// <returns><see langword="false"/> if the buffer was already completed.</returns>
    public bool Complete(Exception? error = null) => _channel.Writer.TryComplete(error);

    /// <summary>
    /// Returns a consistent copy of the buffer's counters.
    /// </summary>
    public BoundedBufferCounters GetCounters()
    {
        lock (_gate)
        {
            return new BoundedBufferCounters(
                _batchesWritten,
                _recordsWritten,
                _batchesDropped,
                _recordsDropped,
                _batchesTaken,
                _recordsTaken,
                Capacity);
        }
    }

    // Called by the channel when a policy drops a batch: the incoming one under DropIncoming, the
    // oldest buffered one under DropOldest. The channel only drops during TryWrite, which always
    // runs under _gate on this same thread, so the counters are already protected here.
    private void OnBatchDropped(Batch<T> batch)
    {
        Debug.Assert(Monitor.IsEntered(_gate), "Drops must happen inside WriteAsync's lock.");

        _batchesDropped++;
        _recordsDropped += batch.Count;
        batch.Dispose();
    }

    // A batch the buffer took ownership of but could not write (cancelled, or buffer completed).
    // Counted as written and dropped, so it is never lost silently.
    private void DropRejected(Batch<T> batch)
    {
        lock (_gate)
        {
            _batchesWritten++;
            _recordsWritten += batch.Count;
            _batchesDropped++;
            _recordsDropped += batch.Count;
        }

        batch.Dispose();
    }

    private static BoundedChannelFullMode ToChannelMode(FullBufferPolicy policy) => policy switch
    {
        FullBufferPolicy.Wait => BoundedChannelFullMode.Wait,

        // DropWrite drops the item being written. (DropNewest would drop the newest item already
        // in the queue, which is not what DropIncoming means.)
        FullBufferPolicy.DropIncoming => BoundedChannelFullMode.DropWrite,

        FullBufferPolicy.DropOldest => BoundedChannelFullMode.DropOldest,
        _ => throw new ArgumentOutOfRangeException(nameof(policy), policy, "Unknown full-buffer policy."),
    };
}
