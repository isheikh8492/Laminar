namespace Laminar.Sources.Simulation;

/// <summary>
/// SplitMix64: a fast, well-mixed hash of a number, the usual choice for deterministic noise.
/// The same input always gives the same output, so simulated data is identical on every run.
/// </summary>
internal static class SplitMix64
{
    public static ulong Hash(long value) => Hash((ulong)value);

    public static ulong Hash(ulong x)
    {
        x += 0x9E3779B97F4A7C15UL;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9UL;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBUL;
        return x ^ (x >> 31);
    }

    /// <summary>A number in [0, 1) from the top 53 bits of a hash: as fine as a double allows.</summary>
    public static double ToUnit(ulong hash) => (hash >> 11) * (1.0 / (1UL << 53));
}
