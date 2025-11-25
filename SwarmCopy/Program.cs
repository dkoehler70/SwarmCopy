using System;
using System.Diagnostics;

namespace SwarmCopy
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                ArgumentParser.ShowHelp();
                return;
            }

            try
            {
                var copyArgs = ArgumentParser.Parse(args);

                if (string.IsNullOrEmpty(copyArgs.Input) || string.IsNullOrEmpty(copyArgs.Output))
                {
                    Console.WriteLine("Error: Both -i (input) and -o (output) arguments are required.");
                    Console.WriteLine();
                    ArgumentParser.ShowHelp();
                    return;
                }

                Console.WriteLine("SwarmCopy - Starting copy operation");
                Console.WriteLine($"Input:  {copyArgs.Input}");
                Console.WriteLine($"Output: {copyArgs.Output}");
                Console.WriteLine();

                var stopwatch = Stopwatch.StartNew();

                CopyOrchestrator.ExecuteCopy(copyArgs);

                stopwatch.Stop();

                Console.WriteLine();
                Console.WriteLine($"Copy completed successfully in {stopwatch.Elapsed.TotalSeconds:F2} seconds");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine();
                Console.WriteLine(ex.StackTrace);
                Environment.Exit(1);
            }
        }
    }
}
