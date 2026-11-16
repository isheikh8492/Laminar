using Laminar.Core;

namespace Laminar.Sources.Simulation;

/// <summary>
/// A simulated spectral flow cytometry event: one cell passing the lasers, seen by every channel.
/// Made by <see cref="SpectralEventGenerator"/>.
/// </summary>
/// <param name="Index">The event's position in the stream: 0, 1, 2, …</param>
/// <param name="Time">When the cell arrived, in nanoseconds since the start.</param>
/// <param name="Population">Which simulated population the cell belongs to; 0 is unstained.
/// The true answer, so gating and unmixing can be checked against it later.</param>
/// <param name="Features">Every channel's features, channel by channel:
/// <c>[channel × features + feature]</c>, in the batch's payload.</param>
/// <param name="Waveforms">Every channel's captured pulse, channel by channel:
/// <c>[channel × samples + sample]</c>, in the batch's payload. Empty if samples is 0.</param>
public readonly record struct SpectralEvent(
    long Index,
    long Time,
    int Population,
    PayloadSlice<float> Features,
    PayloadSlice<short> Waveforms);

/// <summary>
/// Makes <see cref="SpectralEvent"/> records shaped like a spectral cytometer's output: lasers,
/// channels spread across them, several features per channel, and a waveform per channel.
/// Pass <see cref="Generate"/> to <see cref="SimulatedSource{T}"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>What it simulates:</b> each population has a spectral signature, a brightness per channel,
/// with one to three emission peaks over a low autofluorescence floor. Population 0 is unstained
/// (the floor only). Each cell picks a population, a log-normal overall brightness, and a little
/// noise per channel. The result is clipped at the detector's full scale (32,767), as a real
/// detector saturates. Feature 0 is the pulse height; the others are the height scaled by a fixed
/// gain. They stand in for area, width and so on, which this simulator doesn't model.
/// </para>
/// <para>
/// <b>Arrival times</b> follow a Poisson process: the gaps between cells are random, with the mean
/// set by the rate, as cells in a fluid stream arrive. They fill <see cref="SpectralEvent.Time"/>.
/// (The source still pushes batches at a steady pace. At hundreds of events per batch, Poisson
/// arrivals change a batch's duration by only a few percent.)
/// </para>
/// <para>
/// <b>Speed:</b> waveforms are copied from pulse shapes computed once, at 256 heights spaced on a
/// log scale (each within about 2 % of the exact height), so making a waveform is a memory copy,
/// not maths per sample. Features use the exact height. The simulator stays far faster than the
/// rates it simulates.
/// </para>
/// <para>
/// <b>Threading:</b> keeps a running clock, so call <see cref="Generate"/> from one thread, in
/// index order. <see cref="SimulatedSource{T}"/> does.
/// </para>
/// </remarks>
public sealed class SpectralEventGenerator
{
    /// <summary>The largest value a detector reports: the top of a 16-bit signed range.</summary>
    public const float FullScale = short.MaxValue;

    // How many precomputed pulse heights there are. A height is rounded to the nearest one.
    private const int PulseHeights = 256;

    private readonly float[][] _signatures;   // [population][channel], brightness 0..1
    private readonly float[] _featureGains;   // [feature]
    private readonly short[] _pulses;         // [height × Samples + sample]
    private readonly int[] _laserOfChannel;   // [channel]
    private readonly float[] _heights;        // Scratch: this event's height per channel.
    private readonly double _meanGapNanoseconds;
    private double _clock;

    /// <summary>
    /// Creates a generator.
    /// </summary>
    /// <param name="eventsPerSecond">The average arrival rate, for the events' timestamps.</param>
    /// <param name="lasers">How many lasers. Channels are split between them as evenly as possible.</param>
    /// <param name="channels">How many detector channels, across all lasers.</param>
    /// <param name="features">How many features each channel reports per event.</param>
    /// <param name="samples">How many samples each channel's waveform has. 0 for no waveforms.</param>
    /// <param name="populations">How many cell populations, including the unstained one.</param>
    /// <exception cref="ArgumentOutOfRangeException">A count is out of range.</exception>
    public SpectralEventGenerator(
        double eventsPerSecond,
        int lasers = 7,
        int channels = 60,
        int features = 9,
        int samples = 64,
        int populations = 4)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(eventsPerSecond);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(lasers);
        ArgumentOutOfRangeException.ThrowIfLessThan(channels, lasers);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(features);
        ArgumentOutOfRangeException.ThrowIfNegative(samples);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(populations);

        Lasers = lasers;
        Channels = channels;
        Features = features;
        Samples = samples;
        Populations = populations;
        _meanGapNanoseconds = 1e9 / eventsPerSecond;

        _laserOfChannel = new int[channels];
        for (int c = 0; c < channels; c++)
        {
            // Laser l owns channels l × channels / lasers up to (l + 1) × channels / lasers.
            _laserOfChannel[c] = c * lasers / channels;
        }

        _signatures = new float[populations][];
        for (int p = 0; p < populations; p++)
        {
            _signatures[p] = MakeSignature(p, channels);
        }

        _featureGains = new float[features];
        for (int f = 0; f < features; f++)
        {
            _featureGains[f] = 1f / (f + 1);
        }

        _pulses = MakePulses(samples);
        _heights = new float[channels];
    }

    /// <summary>How many lasers.</summary>
    public int Lasers { get; }

    /// <summary>How many detector channels, across all lasers.</summary>
    public int Channels { get; }

    /// <summary>How many features each channel reports per event.</summary>
    public int Features { get; }

    /// <summary>How many samples each channel's waveform has.</summary>
    public int Samples { get; }

    /// <summary>How many cell populations, including the unstained one (population 0).</summary>
    public int Populations { get; }

    /// <summary>The bytes one event carries: its features and its waveforms.</summary>
    public int PayloadBytesPerEvent => (Channels * Features * sizeof(float)) + (Channels * Samples * sizeof(short));

    /// <summary>Which laser (0-based) a channel (0-based) belongs to.</summary>
    /// <exception cref="IndexOutOfRangeException"><paramref name="channel"/> is out of range.</exception>
    public int LaserOf(int channel) => _laserOfChannel[channel];

    /// <summary>
    /// How many payload bytes a batch of <paramref name="batchSize"/> events needs, including the
    /// padding that aligns each allocation to 8 bytes. Give it to the pipeline's options.
    /// </summary>
    public int PayloadBytesPerBatch(int batchSize)
    {
        // Two allocations per event, each padded by at most 7 bytes.
        long perEvent = PayloadBytesPerEvent + 16;
        return (int)Math.Min(perEvent * batchSize, Array.MaxLength);
    }

    /// <summary>
    /// Makes the event with the given index, putting its features and waveforms in
    /// <paramref name="batch"/>'s payload.
    /// </summary>
    public SpectralEvent Generate(long index, Batch<SpectralEvent> batch)
    {
        ArgumentNullException.ThrowIfNull(batch);

        ulong h = SplitMix64.Hash(index);
        int population = (int)(h % (ulong)Populations);
        float brightness = Brightness(SplitMix64.Hash(h));
        float[] signature = _signatures[population];

        // Heights first, into scratch space: both payload allocations below need them, and a span
        // from the first allocation isn't valid after the second (the buffer may move).
        for (int c = 0; c < Channels; c++)
        {
            // ±5 % noise per channel, from 16 bits of this channel's own hash.
            ulong n = SplitMix64.Hash(h + (ulong)c + 1);
            float noise = 1f + ((((n & 0xFFFF) / 65535f) - 0.5f) * 0.1f);
            _heights[c] = Math.Min(signature[c] * brightness * noise, FullScale);
        }

        PayloadSlice<float> featureSlice = batch.AllocatePayload<float>(Channels * Features, out Span<float> features);
        for (int c = 0; c < Channels; c++)
        {
            Span<float> channel = features.Slice(c * Features, Features);
            for (int f = 0; f < Features; f++)
            {
                channel[f] = _heights[c] * _featureGains[f];
            }
        }

        PayloadSlice<short> waveSlice = batch.AllocatePayload<short>(Channels * Samples, out Span<short> waveforms);
        if (Samples > 0)
        {
            for (int c = 0; c < Channels; c++)
            {
                int height = PulseIndex(_heights[c]);
                _pulses.AsSpan(height * Samples, Samples).CopyTo(waveforms.Slice(c * Samples, Samples));
            }
        }

        // Poisson arrivals: the gap to the next cell is exponential. 1 - u keeps Log away from 0.
        double u = SplitMix64.ToUnit(SplitMix64.Hash(~h));
        _clock += -Math.Log(1 - u) * _meanGapNanoseconds;

        return new SpectralEvent(index, (long)_clock, population, featureSlice, waveSlice);
    }

    // A log-normal brightness around 6,000 counts: most cells near it, a long tail of bright ones
    // that saturate, as real stained cells look on a log axis.
    private static float Brightness(ulong hash)
    {
        // Adding four uniform numbers gives a bell curve (Irwin–Hall). Shifted and scaled so it
        // averages 0 with a spread of 1, it stands in for a normal random number.
        float sum = 0;
        for (int i = 0; i < 4; i++)
        {
            sum += ((hash >> (16 * i)) & 0xFFFF) / 65535f;
        }

        float normal = (sum - 2f) * MathF.Sqrt(3f);
        return 6_000f * MathF.Exp(0.8f * normal);
    }

    // One population's brightness per channel, peaking at 1. Population 0 is unstained: only the
    // autofluorescence floor, so it stays dim on every channel.
    private static float[] MakeSignature(int population, int channels)
    {
        const float Floor = 0.02f;
        var signature = new float[channels];
        Array.Fill(signature, Floor);

        if (population == 0)
        {
            return signature;
        }

        ulong h = SplitMix64.Hash(population * 7919L);
        int peaks = 1 + (int)(h % 3);
        for (int k = 0; k < peaks; k++)
        {
            h = SplitMix64.Hash(h);
            float center = (float)(h % (ulong)channels);
            float width = 1.5f + ((h >> 32) % 40 / 10f);
            float strength = k == 0 ? 1f : 0.2f + ((h >> 48) % 60 / 100f);

            for (int c = 0; c < channels; c++)
            {
                float z = (c - center) / width;
                signature[c] += strength * MathF.Exp(-0.5f * z * z);
            }
        }

        float max = signature.Max();
        for (int c = 0; c < channels; c++)
        {
            signature[c] /= max;
        }

        return signature;
    }

    // The precomputed pulse nearest to a height. Pulse heights are spaced on a log scale, each
    // about 4 % above the one before, from 1 count up to full scale: evenly spaced heights would
    // be 128 counts apart and turn every dim pulse into a flat line. Index 0 is no pulse at all.
    private static int PulseIndex(float height)
    {
        if (height < 1f)
        {
            return 0;
        }

        int index = 1 + (int)MathF.Round(MathF.Log(height) / MathF.Log(FullScale) * (PulseHeights - 2));
        return Math.Min(index, PulseHeights - 1);
    }

    // The height of precomputed pulse i: the inverse of PulseIndex.
    private static float PulseHeight(int index) =>
        index == 0 ? 0f : MathF.Exp((index - 1) / (float)(PulseHeights - 2) * MathF.Log(FullScale));

    // A bell-shaped pulse centred in the window, precomputed at every height:
    // pulses[height × samples + s].
    private static short[] MakePulses(int samples)
    {
        var pulses = new short[PulseHeights * samples];
        float center = (samples - 1) / 2f;
        float sigma = Math.Max(1f, samples / 6f);

        for (int height = 0; height < PulseHeights; height++)
        {
            float peak = PulseHeight(height);
            for (int s = 0; s < samples; s++)
            {
                float z = (s - center) / sigma;
                pulses[(height * samples) + s] = (short)(peak * MathF.Exp(-0.5f * z * z));
            }
        }

        return pulses;
    }
}
