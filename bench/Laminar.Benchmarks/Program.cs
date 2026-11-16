// Laminar microbenchmarks. Run in Release:
//
//   dotnet run -c Release --project bench/Laminar.Benchmarks                     (pick from a menu)
//   dotnet run -c Release --project bench/Laminar.Benchmarks -- --filter *Scan*  (only scans)
//   dotnet run -c Release --project bench/Laminar.Benchmarks -- --filter * --job short
//
// "--job short" runs fewer iterations: quicker, a little less precise.

using BenchmarkDotNet.Running;

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
