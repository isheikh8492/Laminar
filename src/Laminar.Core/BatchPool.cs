namespace Laminar.Core;

/// <summary>
/// A fixed set of batches allocated up front and reused for a whole run (ADR 0005). A pipeline
/// creates one in <c>Prepare</c>; sources rent from it, and disposing a batch returns it here.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
/// <remarks>
/// <para>
/// <b>Sizing:</b> a pipeline needs at most <c>buffer capacity + 1 + one per source</c> batches at
/// once: the buffer full, the worker holding one, and each source filling one.
/// </para>
/// <para>
/// <b>Never stalls, never silent:</b> if the pool is ever empty, <see cref="Rent"/> creates a new
/// batch rather than making the source wait, keeps it for reuse, and counts it in
/// <see cref="BatchesAllocatedAfterPrepare"/>. A payload that outgrows its buffer is counted in
/// <see cref="PayloadGrowths"/>. Either count above zero means the sizing was too small.
/// </para>
/// <para>
/// <b>Threading:</b> any number of threads may rent and return at the same time.
/// </para>
/// </remarks>
public sealed class BatchPool<T>
    where T : unmanaged
{
    private readonly object _gate = new();

    // Free batches, used as a stack: rent takes from the end, return puts back at the end.
    private Batch<T>[] _free;
    private int _freeCount;

    private long _batchesAllocatedAfterPrepare;
    private long _payloadGrowths;

    /// <summary>
    /// Creates the pool and allocates all its batches now.
    /// </summary>
    /// <param name="batches">How many batches to allocate.</param>
    /// <param name="batchCapacity">Records per batch.</param>
    /// <param name="payloadBytesPerBatch">Payload bytes per batch, for nested data. 0 for none.</param>
    /// <exception cref="ArgumentOutOfRangeException">A count is zero or negative, or the payload size is negative.</exception>
    public BatchPool(int batches, int batchCapacity, int payloadBytesPerBatch)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batches);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchCapacity);
        ArgumentOutOfRangeException.ThrowIfNegative(payloadBytesPerBatch);

        BatchCapacity = batchCapacity;
        PayloadBytesPerBatch = payloadBytesPerBatch;
        TotalBatches = batches;

        _free = new Batch<T>[batches];
        for (int i = 0; i < batches; i++)
        {
            _free[i] = new Batch<T>(this, batchCapacity, payloadBytesPerBatch);
        }

        _freeCount = batches;
    }

    /// <summary>Records per batch.</summary>
    public int BatchCapacity { get; }

    /// <summary>Payload bytes each batch starts with.</summary>
    public int PayloadBytesPerBatch { get; }

    /// <summary>How many batches the pool owns, including any created after it ran dry.</summary>
    public int TotalBatches { get; private set; }

    /// <summary>How many batches are free right now.</summary>
    public int Available
    {
        get
        {
            lock (_gate)
            {
                return _freeCount;
            }
        }
    }

    /// <summary>Batches created because the pool was empty. Above zero means it was sized too small.</summary>
    public long BatchesAllocatedAfterPrepare => Interlocked.Read(ref _batchesAllocatedAfterPrepare);

    /// <summary>Times a batch's payload outgrew its buffer. Above zero means the payload size was too small.</summary>
    public long PayloadGrowths => Interlocked.Read(ref _payloadGrowths);

    /// <summary>
    /// Takes an empty batch, ready to fill.
    /// </summary>
    /// <param name="sequence">The batch's position in the source's stream.</param>
    public Batch<T> Rent(long sequence)
    {
        Batch<T>? batch = null;

        lock (_gate)
        {
            if (_freeCount > 0)
            {
                _freeCount--;
                batch = _free[_freeCount];
                _free[_freeCount] = null!; // Don't keep a second reference to a batch in use.
            }
        }

        if (batch is null)
        {
            // Empty: make one more rather than stall the source, and count it.
            batch = new Batch<T>(this, BatchCapacity, PayloadBytesPerBatch);
            Interlocked.Increment(ref _batchesAllocatedAfterPrepare);

            lock (_gate)
            {
                TotalBatches++;
            }
        }

        batch.Reset(sequence);
        return batch;
    }

    // Called by Batch.Dispose.
    internal void Return(Batch<T> batch)
    {
        lock (_gate)
        {
            if (_freeCount == _free.Length)
            {
                // Only possible after extra batches were created; grow the stack to hold them.
                Array.Resize(ref _free, _free.Length * 2);
            }

            _free[_freeCount] = batch;
            _freeCount++;
        }
    }

    // Called by Batch when its payload buffer had to grow.
    internal void OnPayloadGrown() => Interlocked.Increment(ref _payloadGrowths);
}
