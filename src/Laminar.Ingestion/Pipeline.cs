using Laminar.Core;

namespace Laminar.Ingestion;

/// <summary>
/// Wires one source to one store (ADR 0002):
/// <c>source ──push──▶ [bounded buffer] ──▶ ingestion worker ──▶ store</c>.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
/// <remarks>
/// <para>
/// <b>Lifetime:</b> create, <see cref="Start"/> once, then <see cref="StopAsync"/> (or dispose).
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
/// </remarks>
public sealed class Pipeline<T> : IAsyncDisposable
{
    private readonly ISource<T> _source;
    private readonly BoundedBuffer<T> _buffer;
    private readonly IngestionWorker<T> _worker;

    // Cancelled by StopAsync to tell the source to finish.
    private readonly CancellationTokenSource _stopSource = new();

    private Task? _completion;
    private bool _disposed;

    /// <summary>
    /// Creates a pipeline. Nothing runs until <see cref="Start"/>.
    /// </summary>
    /// <param name="source">Where the records come from.</param>
    /// <param name="store">Where the records go. Only the pipeline may call its <c>Append</c>.</param>
    /// <param name="bufferCapacity">How many batches the buffer holds before <paramref name="policy"/> applies.</param>
    /// <param name="policy">What to do when the buffer is full. There is no default (ADR 0002).</param>
    public Pipeline(ISource<T> source, IRecordStore<T> store, int bufferCapacity, FullBufferPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(source);

        _source = source;
        _buffer = new BoundedBuffer<T>(bufferCapacity, policy);
        _worker = new IngestionWorker<T>(_buffer, store);
    }

    /// <summary>How many batches the buffer holds before the policy applies.</summary>
    public int BufferCapacity => _buffer.Capacity;

    /// <summary>What the buffer does when it is full.</summary>
    public FullBufferPolicy Policy => _buffer.Policy;

    /// <summary>
    /// Completes when the pipeline has fully stopped, for any reason. Faults if the source or the
    /// store failed; is cancelled if the pipeline was aborted.
    /// </summary>
    /// <exception cref="InvalidOperationException">The pipeline has not been started.</exception>
    public Task Completion => _completion ?? throw new InvalidOperationException("The pipeline has not been started.");

    /// <summary>
    /// Starts the source and the worker in the background and returns immediately.
    /// </summary>
    /// <param name="abortToken">Cancelling it aborts the pipeline without draining the buffer.</param>
    /// <exception cref="InvalidOperationException">The pipeline was already started.</exception>
    public void Start(CancellationToken abortToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completion is not null)
        {
            throw new InvalidOperationException("The pipeline has already been started.");
        }

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
    public PipelineMetrics GetMetrics() => _worker.GetMetrics();

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

    private async Task RunAsync(CancellationToken abortToken)
    {
        // The source stops on StopAsync or on abort, whichever comes first.
        using var sourceStop = CancellationTokenSource.CreateLinkedTokenSource(_stopSource.Token, abortToken);

        // Task.Run moves each loop onto a thread-pool thread, so neither runs on the caller's thread.
        Task source = Task.Run(() => RunSourceAsync(sourceStop.Token), CancellationToken.None);
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
        }
    }

    private async Task RunSourceAsync(CancellationToken stopToken)
    {
        try
        {
            await _source.RunAsync(_buffer, stopToken).ConfigureAwait(false);
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
}
