using System;
using System.Diagnostics;
using System.Threading;

namespace SwarmCopy
{
    public class ProgressTracker : IDisposable
    {
        private long _rowsProcessed;
        private readonly Timer _timer;
        private readonly Stopwatch _stopwatch;
        private readonly string _operationName;
        private bool _isActive;

        public ProgressTracker(string operationName)
        {
            _operationName = operationName;
            _stopwatch = Stopwatch.StartNew();
            _isActive = true;

            // Timer fires every 60 seconds
            _timer = new Timer(ReportProgress, null, TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(60));
        }

        public void IncrementRows(long count = 1)
        {
            Interlocked.Add(ref _rowsProcessed, count);
        }

        public long RowsProcessed => Interlocked.Read(ref _rowsProcessed);

        private void ReportProgress(object state)
        {
            if (!_isActive) return;

            var rows = Interlocked.Read(ref _rowsProcessed);
            var elapsed = _stopwatch.Elapsed;
            var rowsPerSecond = rows / elapsed.TotalSeconds;

            Console.WriteLine($"  [{_operationName}] Progress: {rows:N0} rows in {elapsed.TotalSeconds:F1}s ({rowsPerSecond:F0} rows/sec)");
        }

        public void Dispose()
        {
            _isActive = false;
            _timer?.Dispose();
            _stopwatch?.Stop();

            // Final report
            var rows = Interlocked.Read(ref _rowsProcessed);
            var elapsed = _stopwatch.Elapsed;
            if (rows > 0)
            {
                var rowsPerSecond = rows / elapsed.TotalSeconds;
                Console.WriteLine($"  [{_operationName}] Completed: {rows:N0} rows in {elapsed.TotalSeconds:F1}s ({rowsPerSecond:F0} rows/sec)");
            }
        }
    }
}
