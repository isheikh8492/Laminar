namespace Laminar.Sources.Simulation;

/// <summary>
/// A simple ready-made record for <see cref="SimulatedSource{T}"/>: one value per index, such as
/// one point of a waveform.
/// </summary>
/// <param name="Index">The record's position in the stream: 0, 1, 2, …</param>
/// <param name="Value">The simulated measurement.</param>
public readonly record struct SimulatedSample(long Index, double Value)
{
    /// <summary>
    /// Makes the sample at <paramref name="index"/> on a sine wave, one full cycle every
    /// 10,000 samples. Pass this method to <see cref="SimulatedSource{T}"/> as its generator.
    /// </summary>
    public static SimulatedSample SineWave(long index) =>
        new(index, Math.Sin(index * (2 * Math.PI / 10_000)));
}
