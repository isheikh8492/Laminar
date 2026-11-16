using Laminar.Core;

namespace Laminar.Sources.Simulation;

/// <summary>
/// A ready-made nested record for <see cref="SimulatedSource{T}"/>, shaped like an instrument
/// event: a few scalars, one value per detector, and a variable-length waveform (ADR 0004).
/// </summary>
/// <param name="Index">The event's position in the stream: 0, 1, 2, …</param>
/// <param name="Time">When the event happened, in microseconds since the start.</param>
/// <param name="Fsc">A simulated scatter value.</param>
/// <param name="Ssc">A second simulated scatter value.</param>
/// <param name="Parameters">One value per detector, in the batch's payload.</param>
/// <param name="Waveform">The captured samples, in the batch's payload. Length varies per event.</param>
public readonly record struct SimulatedEvent(
    long Index,
    long Time,
    float Fsc,
    float Ssc,
    PayloadSlice<float> Parameters,
    PayloadSlice<short> Waveform);

/// <summary>
/// Makes <see cref="SimulatedEvent"/> records with a fixed number of detectors and a waveform of
/// varying length. Pass <see cref="Generate"/> to <see cref="SimulatedSource{T}"/>.
/// </summary>
/// <remarks>
/// Values are derived from the event's index with a cheap hash, not a random number generator, so
/// they are the same on every run and the generator is safe to share between threads.
/// </remarks>
public sealed class SimulatedEventGenerator
{
    /// <summary>
    /// Creates a generator.
    /// </summary>
    /// <param name="detectors">How many detector values each event carries.</param>
    /// <param name="minSamples">The shortest waveform, in samples.</param>
    /// <param name="maxSamples">The longest waveform, in samples.</param>
    /// <param name="eventsPerSecond">The rate the events are generated at, for their timestamps.</param>
    /// <exception cref="ArgumentOutOfRangeException">A count is negative, or the sample range is reversed.</exception>
    public SimulatedEventGenerator(int detectors, int minSamples, int maxSamples, double eventsPerSecond)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(detectors);
        ArgumentOutOfRangeException.ThrowIfNegative(minSamples);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxSamples, minSamples);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(eventsPerSecond);

        Detectors = detectors;
        MinSamples = minSamples;
        MaxSamples = maxSamples;
        MicrosecondsPerEvent = 1_000_000 / eventsPerSecond;
    }

    /// <summary>How many detector values each event carries.</summary>
    public int Detectors { get; }

    /// <summary>The shortest waveform, in samples.</summary>
    public int MinSamples { get; }

    /// <summary>The longest waveform, in samples.</summary>
    public int MaxSamples { get; }

    private double MicrosecondsPerEvent { get; }

    /// <summary>Roughly how many payload bytes a batch of <paramref name="batchSize"/> events needs.</summary>
    public int PayloadBytesPerBatch(int batchSize)
    {
        long perEvent = (Detectors * sizeof(float)) + ((MinSamples + MaxSamples) / 2 * sizeof(short)) + 16;
        return (int)Math.Min(perEvent * batchSize, Array.MaxLength);
    }

    /// <summary>
    /// Makes the event with the given index, putting its nested data in <paramref name="batch"/>.
    /// </summary>
    public SimulatedEvent Generate(long index, Batch<SimulatedEvent> batch)
    {
        ArgumentNullException.ThrowIfNull(batch);

        ulong h = SplitMix64.Hash(index);

        PayloadSlice<float> parameters = batch.AllocatePayload<float>(Detectors, out Span<float> values);
        for (int d = 0; d < values.Length; d++)
        {
            // A log-normal-ish spread, like fluorescence intensities.
            values[d] = MathF.Exp(((SplitMix64.Hash(h + (ulong)d) & 0xFFFF) / 6553.6f) - 2f) * 100f;
        }

        int range = MaxSamples - MinSamples + 1;
        int sampleCount = MinSamples + (int)(h % (ulong)range);
        PayloadSlice<short> waveform = batch.AllocatePayload<short>(sampleCount, out Span<short> samples);
        for (int s = 0; s < samples.Length; s++)
        {
            // A pulse: rises and falls once across the waveform.
            float x = (float)s / Math.Max(1, samples.Length - 1);
            samples[s] = (short)(MathF.Sin(x * MathF.PI) * 30_000);
        }

        return new SimulatedEvent(
            index,
            (long)(index * MicrosecondsPerEvent),
            Fsc: (h >> 16 & 0xFFFF) / 65.535f,
            Ssc: (h >> 32 & 0xFFFF) / 65.535f,
            parameters,
            waveform);
    }
}
