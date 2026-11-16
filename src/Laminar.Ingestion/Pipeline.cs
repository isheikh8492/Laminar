using Laminar.Core;

namespace Laminar.Ingestion;

/// <summary>
/// Where a pipeline is in its life (ADR 0005). It only moves forward.
/// </summary>
public enum PipelineState
{
    /// <summary>Constructed. Nothing is allocated yet.</summary>
    Created,

    /// <summary>Everything the run needs is allocated; the source hasn't started.</summary>
    Prepared,

    /// <summary>The source is pushing and the worker is storing.</summary>
    Running,

    /// <summary>Fully stopped, for any reason. A pipeline can't be restarted; make a new one.</summary>
    Stopped,
}

/// <summary>
/// Wires one source to one store (ADR 0002):
/// <c>source ──push──▶ [bounded buffer] ──▶ ingestion worker ──▶ store</c>.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
/// <remarks>
/// <para>
/// <b>Lifetime (ADR 0005):</b> create, <see cref="Prepare"/>, <see cref="Start"/>, then
/// <see cref="StopAsync"/> (or dispose). A push source can't wait for warm-up, so
/// <see cref="Prepare"/> allocates everything up front, before the source may push: the batch pool
/// and the store's reserved room. After that, the only expected allocation is the store growing
/// beyond what was reserved. Anything else is counted in <see cref="PipelineMetrics"/>.
/// </para>
/// <para>
/// The source and the worker each run on their own thread-pool thread, never on the caller's
/// thread, so <see cref="Start"/> returns at once and is safe to call from a UI thread.
/// </para>
/// <para>
/// <b>Stopping:</b>
/// </para>
/// <list type="bullet">
///   <item><see cref="StopAsync"/> is graceful: the source is told to stop, then everything already
///   buffered is appended. A write the source was waiting on is cancelled and counted as dropped.</item>
///   <item>Cancelling the token given to <see cref="Start"/> is an abort: the worker stops at once.</item>
///   <item>If the source or the store fails, the other side is stopped and the error comes out of
///   <see cref="Completion"/> and <see cref="StopAsync"/>.</item>
/// </list>
/// <para>
/// However it stops, any batch left in the buffer is counted as dropped and returned to the pool.
/// </para>
/// <para>
/// <b>Threading:</b> call <see cref="Prepare"/>, <see cref="Start"/> and <see cref="StopAsync"/>
/// from one thread. <see cref="GetMetrics"/> and <see cref="State"/> are safe from any thread.
/// </para>
/// </remarks>
public sealed class Pipeline<T> : IAsyncDisposable
    where T : unmanaged
{
    private readonly ISource<T> _source;
    private readonly IRecordStore<T> _store;
    private readonly BoundedBuffer<T> _buffer;
    private readonly IngestionWorker<T> _worker;

    // Cancelled by StopAsync to tell the source to finish.
    private readonly CancellationTokenSource _stopSource = new();

    private BatchPool<T>? _pool;
    private Task? _completion;
    private int _state; // A PipelineState, as an int so it can be read safely from any thread.
    private bool _disposed;

    /// <summary>
    /// Creates a pipeline. Nothing is allocated or run until <see cref="Prepare"/> and <see cref="Start"/>.
    /// </summary>
    /// <param name="source">Where the records come from.</param>
    /// <param name="store">Where the records go. Only the pipeline may call its <c>Append</c>.</param>
    /// <param name="options">Sizing and the full-buffer policy, fixed for the whole run.</param>
    /// <exception cref="ArgumentOutOfRangeException">A size in <paramref name="options"/> is out of range.</exception>
    public Pipeline(ISource<T> source, IRecordStore<T> store, PipelineOptions options)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.BatchCapacity);
        ArgumentOutOfRangeException.ThrowIfNegative(options.PayloadBytesPerBatch);
        ArgumentOutOfRangeException.ThrowIfNegative(options.ReserveRecords);

        _source = source;
        _store = store;
        Options = options;
        _buffer = new BoundedBuffer<T>(options.BufferCapacity, options.Policy);
        _worker = new IngestionWorker<T>(_buffer, store);
    }

    /// <summary>The sizing and policy this pipeline was created with.</summary>
    public PipelineOptions Options { get; }

    /// <summary>Where the pipeline is in its life.</summary>
    public PipelineState State => (PipelineState)Volatile.Read(ref _state);

    /// <summary>
    /// Completes when the pipeline has fully stopped, for any reason. Faults if the source or the
    /// store failed; is cancelled if the pipeline was aborted.
    /// </summary>
    /// <exception cref="InvalidOperationException">The pipeline has not been started.</exception>
    public Task Completion => _completion ?? throw new InvalidOperationException("The pipeline has not been started.");

    /// <summary>
    /// Allocates everything the run needs, before any data arrives: the batch pool (every batch
    /// with its record array and payload buffer), and the store's reserved room.
    /// </summary>
    /// <exception cref="InvalidOperationException">The pipeline was already prepared.</exception>
    public void Prepare()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        RequireState(PipelineState.Created, "Prepare can only be called once, on a new pipeline.");

        _pool = new BatchPool<T>(Options.PoolSize, Options.BatchCapacity, Options.PayloadBytesPerBatch);

        if (Options.ReserveRecords > 0)
        {
            _store.Reserve(Options.ReserveRecords);
        }

        Volatile.Write(ref _state, (int)PipelineState.Prepared);
    }

    /// <summary>
    /// Starts the source and the worker in the background and returns immediately.
    /// </summary>
    /// <param name="abortToken">Cancelling it aborts the pipeline without draining the buffer.</param>
    /// <exception cref="InvalidOperationException">
    /// The pipeline isn't prepared (call <see cref="Prepare"/> first), or was already started.
    /// </exception>
    public void Start(CancellationToken abortToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        RequireState(
            PipelineState.Prepared,
            "Call Prepare before Start: a push source can't wait for warm-up, so everything is allocated first.");

        Volatile.Write(ref _state, (int)PipelineState.Running);
        _completion = RunAsync(abortToken);
    }

    /// <summary>
    /// Stops the source, appends everything already buffered, and waits until the pipeline has stopped.
    /// </summary>
    /// <exception cref="InvalidOperationException">The pipeline has not been started.</exception>
    /// <exception cref="Exception">The source or the store failed; that error is rethrown.</exception>
    public async Task StopAsync()
    {
        Task completion = Completion;
        await _stopSource.CancelAsync().ConfigureAwait(false);
        await completion.ConfigureAwait(false);
    }

    /// <summary>
    /// Returns a consistent copy of the pipeline's counters. Safe to call from any thread, at any time.
    /// </summary>
    public PipelineMetrics GetMetrics()
    {
        PipelineMetrics metrics = _worker.GetMetrics();
        BatchPool<T>? pool = Volatile.Read(ref _pool);

        return pool is null
            ? metrics
            : metrics with
            {
                BatchesAllocatedAfterPrepare = pool.BatchesAllocatedAfterPrepare,
                PayloadGrowths = pool.PayloadGrowths,
            };
    }

    /// <summary>
    /// Stops the pipeline gracefully if it is running and waits for it. Never throws: errors are
    /// reported through <see cref="Completion"/> and <see cref="StopAsync"/>.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        if (_completion is not null)
        {
            await _stopSource.CancelAsync().ConfigureAwait(false);

            try
            {
                await _completion.ConfigureAwait(false);
            }
#pragma warning disable CA1031 // Disposal must not throw; the error is still visible on Completion.
            catch (Exception)
#pragma warning restore CA1031
            {
            }
        }

        _stopSource.Dispose();
    }

    private void RequireState(PipelineState expected, string message)
    {
        if (State != expected)
        {
            throw new InvalidOperationException($"{message} The pipeline is {State}.");
        }
    }

    private async Task RunAsync(CancellationToken abortToken)
    {
        // The source stops on StopAsync or on abort, whichever comes first.
        using var sourceStop = CancellationTokenSource.CreateLinkedTokenSource(_stopSource.Token, abortToken);
        var writer = new Writer(_pool!, _buffer);

        // Task.Run moves each loop onto a thread-pool thread, so neither runs on the caller's thread.
        Task source = Task.Run(() => RunSourceAsync(writer, sourceStop.Token), CancellationToken.None);
        Task worker = Task.Run(() => RunWorkerAsync(sourceStop, abortToken), CancellationToken.None);

        try
        {
            await Task.WhenAll(source, worker).ConfigureAwait(false);
        }
        finally
        {
            // Both sides have stopped. Anything still buffered will never be read.
            _buffer.Complete();
            _buffer.DiscardBuffered();
            Volatile.Write(ref _state, (int)PipelineState.Stopped);
        }
    }

    private async Task RunSourceAsync(Writer writer, CancellationToken stopToken)
    {
        try
        {
            await _source.RunAsync(writer, stopToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stopToken.IsCancellationRequested)
        {
            // Asked to stop: a normal ending, not a failure.
        }
        finally
        {
            // No more writes. The worker appends what is buffered, then finishes.
            _buffer.Complete();
        }
    }

    private async Task RunWorkerAsync(CancellationTokenSource sourceStop, CancellationToken abortToken)
    {
        try
        {
            await _worker.RunAsync(abortToken).ConfigureAwait(false);
        }
        catch
        {
            // The worker died, so nobody will read: stop the source too, even if it is idle and
            // would otherwise never find out.
            await sourceStop.CancelAsync().ConfigureAwait(false);
            throw;
        }
    }

    // What the source sees: batches come from the prepared pool, and go into the buffer.
    private sealed class Writer(BatchPool<T> pool, BoundedBuffer<T> buffer) : IBatchWriter<T>
    {
        public Batch<T> RentBatch(long sequence) => pool.Rent(sequence);

        public ValueTask WriteAsync(Batch<T> batch, CancellationToken cancellationToken = default) =>
            buffer.WriteAsync(batch, cancellationToken);
    }
}
