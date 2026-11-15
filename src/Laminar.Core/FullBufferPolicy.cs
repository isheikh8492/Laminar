namespace Laminar.Core;

/// <summary>
/// What the bounded buffer does when a batch arrives and the buffer is already full.
/// </summary>
/// <remarks>
/// There is deliberately no default: every pipeline must choose its policy explicitly (ADR 0002).
/// Every batch dropped by a policy is disposed by the buffer and counted in
/// <see cref="PipelineMetrics"/>, never lost silently.
/// </remarks>
public enum FullBufferPolicy
{
    /// <summary>
    /// The writer waits until there is room. Nothing is dropped.
    /// </summary>
    /// <remarks>
    /// Only safe for sources that can wait, such as files or simulators. A hardware source that
    /// must never block should use a drop policy instead.
    /// </remarks>
    Wait,

    /// <summary>
    /// The incoming batch is dropped and counted. The buffer keeps the batches it already has.
    /// </summary>
    /// <remarks>Keeps the oldest data. Use when the earliest records matter most.</remarks>
    DropIncoming,

    /// <summary>
    /// The oldest batch in the buffer is dropped and counted, making room for the incoming one.
    /// </summary>
    /// <remarks>Keeps the newest data. Use when freshness matters most, such as live display.</remarks>
    DropOldest,
}
