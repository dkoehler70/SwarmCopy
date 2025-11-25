using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SwarmCopy
{
    public class CopyOrchestrator
    {
        private const int THREAD_MULTIPLIER = 2; // Use 2x cores for I/O efficiency

        public static void ExecuteCopy(CopyArguments args)
        {
            // Determine copy mode
            if (args.IsInputDatabase && args.IsOutputDatabase)
            {
                // Database to Database
                CopyDatabaseToDatabase(args.Input, args.Output);
            }
            else if (args.IsInputDatabase && !args.IsOutputDatabase)
            {
                // Database to File
                CopyDatabaseToFile(args.Input, args.Output);
            }
            else if (!args.IsInputDatabase && args.IsOutputDatabase)
            {
                // File to Database
                CopyFileToDatabase(args.Input, args.Output);
            }
            else
            {
                // File to File
                CopyFileToFile(args.Input, args.Output);
            }
        }

        private static void CopyDatabaseToDatabase(string input, string output)
        {
            var inputConn = ConnectionInfo.Parse(input);
            var outputConn = ConnectionInfo.Parse(output);

            if (inputConn.DbTable == "*")
            {
                // Copy all tables
                var tables = DatabaseReader.GetAllTables(inputConn);
                Console.WriteLine($"Found {tables.Length} tables to copy");

                // Smart partition: divide threads (cores * multiplier) among tables for I/O efficiency
                var coreCount = Environment.ProcessorCount;
                var totalThreads = coreCount * THREAD_MULTIPLIER;
                var partitionsPerTable = Math.Max(1, totalThreads / tables.Length);
                Console.WriteLine($"Using {partitionsPerTable} partitions per table ({coreCount} cores × {THREAD_MULTIPLIER} = {totalThreads} threads / {tables.Length} tables)");

                Parallel.ForEach(tables, new ParallelOptions { MaxDegreeOfParallelism = coreCount }, table =>
                {
                    Console.WriteLine($"Copying table: {table}");
                    CopySingleTableToTable(inputConn, outputConn, table, table, partitionsPerTable);
                    Console.WriteLine($"Completed table: {table}");
                });
            }
            else
            {
                // Copy single table - use 2x cores for I/O efficiency
                var partitionCount = Environment.ProcessorCount * THREAD_MULTIPLIER;
                var outputTable = string.IsNullOrEmpty(outputConn.DbTable) ? inputConn.DbTable : outputConn.DbTable;
                Console.WriteLine($"Using {partitionCount} partitions for single table ({Environment.ProcessorCount} cores × {THREAD_MULTIPLIER})");
                CopySingleTableToTable(inputConn, outputConn, inputConn.DbTable, outputTable, partitionCount);
            }
        }

        private static void CopySingleTableToTable(ConnectionInfo inputConn, ConnectionInfo outputConn, string inputTable, string outputTable, int partitionCount)
        {
            // Get columns and their sizes from source table
            var columns = DatabaseReader.GetTableColumns(inputConn, inputTable);
            var sourceSizes = DatabaseWriter.GetColumnSizes(inputConn, inputTable);

            // Add buffer to column sizes
            var targetSizes = new Dictionary<string, int>();
            foreach (var column in columns)
            {
                if (sourceSizes.TryGetValue(column, out var size))
                {
                    // Add buffer of 10, but if already MAX, keep it MAX
                    if (size == int.MaxValue)
                    {
                        targetSizes[column] = int.MaxValue;
                    }
                    else
                    {
                        var sizeWithBuffer = size + 10;
                        // Switch to MAX if >= 512 chars
                        targetSizes[column] = sizeWithBuffer >= 512 ? int.MaxValue : sizeWithBuffer;
                    }
                }
                else
                {
                    // Default to reasonable size if not found
                    targetSizes[column] = 100;
                }
            }

            Console.WriteLine($"  Creating destination table with optimized column sizes...");

            // Create or ensure destination table exists with proper sizes
            DatabaseWriter.EnsureTableExistsWithSizes(outputConn, outputTable, targetSizes);

            // Multi-threaded copy using binary_checksum partitioning
            var tasks = new List<Task>();

            using (var progress = new ProgressTracker($"Table {inputTable} -> {outputTable}"))
            {
                for (int i = 0; i < partitionCount; i++)
                {
                    var partitionIndex = i;
                    var task = Task.Run(() =>
                    {
                        var rows = DatabaseReader.ReadTable(inputConn, inputTable, partitionIndex, partitionCount);
                        var rowList = new List<Dictionary<string, string>>();

                        foreach (var row in rows)
                        {
                            rowList.Add(row);

                            // Batch inserts
                            if (rowList.Count >= 10000)
                            {
                                DatabaseWriter.BulkInsertWithSizing(outputConn, outputTable, rowList, columns, targetSizes);
                                progress.IncrementRows(rowList.Count);
                                rowList.Clear();
                            }
                        }

                        if (rowList.Count > 0)
                        {
                            DatabaseWriter.BulkInsertWithSizing(outputConn, outputTable, rowList, columns, targetSizes);
                            progress.IncrementRows(rowList.Count);
                        }

                        Console.WriteLine($"  [{inputTable} -> {outputTable}] Partition {partitionIndex + 1}/{partitionCount} completed");
                    });

                    tasks.Add(task);
                }

                Task.WaitAll(tasks.ToArray());
            }
        }

        private static void CopyDatabaseToFile(string input, string output)
        {
            var inputConn = ConnectionInfo.Parse(input);

            if (!string.IsNullOrEmpty(inputConn.DbSql))
            {
                // Copy from SQL query to file
                Console.WriteLine($"Executing SQL query and exporting to file...");
                CopySqlQueryToFile(inputConn, inputConn.DbSql, output);
            }
            else if (inputConn.DbTable == "*")
            {
                // Copy all tables to separate files
                var tables = DatabaseReader.GetAllTables(inputConn);

                // Determine output directory
                // If output has extension (like "file.csv"), use its directory
                // Otherwise, treat output as a directory name
                var outputDir = Path.HasExtension(output)
                    ? (Path.GetDirectoryName(output) ?? ".")
                    : output;

                // Create directory if it doesn't exist
                if (!Directory.Exists(outputDir))
                {
                    Directory.CreateDirectory(outputDir);
                    Console.WriteLine($"Created directory: {outputDir}");
                }

                Console.WriteLine($"Found {tables.Length} tables to export");

                Parallel.ForEach(tables, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, table =>
                {
                    Console.WriteLine($"Exporting table: {table}");
                    var outputFile = Path.Combine(outputDir, $"{table}.csv");
                    CopySingleTableToFile(inputConn, table, outputFile);
                    Console.WriteLine($"Completed table: {table}");
                });
            }
            else
            {
                // Copy single table to file
                CopySingleTableToFile(inputConn, inputConn.DbTable, output);
            }
        }

        private static void CopySqlQueryToFile(ConnectionInfo inputConn, string sqlQuery, string outputFile)
        {
            var columns = DatabaseReader.GetColumnsFromSql(inputConn, sqlQuery);
            var rows = DatabaseReader.ReadFromSql(inputConn, sqlQuery);
            var delimiter = FileWriter.GetDelimiterFromExtension(outputFile);

            FileWriter.WriteFile(outputFile, rows, columns, delimiter);
        }

        private static void CopySingleTableToFile(ConnectionInfo inputConn, string tableName, string outputFile)
        {
            var columns = DatabaseReader.GetTableColumns(inputConn, tableName);
            var rows = DatabaseReader.ReadTable(inputConn, tableName);
            var delimiter = FileWriter.GetDelimiterFromExtension(outputFile);

            FileWriter.WriteFile(outputFile, rows, columns, delimiter);
        }

        private static void CopyFileToDatabase(string input, string output)
        {
            var outputConn = ConnectionInfo.Parse(output);
            var files = GetInputFiles(input);

            Console.WriteLine($"Found {files.Length} files to import");

            if (files.Length == 1)
            {
                // Single file
                var file = files[0];
                var tableName = string.IsNullOrEmpty(outputConn.DbTable) ? Path.GetFileNameWithoutExtension(file) : outputConn.DbTable;
                Console.WriteLine($"Importing: {file} -> {tableName}");
                CopySingleFileToTable(file, outputConn, tableName);
            }
            else
            {
                // Multiple files
                Parallel.ForEach(files, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, file =>
                {
                    var tableName = string.IsNullOrEmpty(outputConn.DbTable) ? Path.GetFileNameWithoutExtension(file) : outputConn.DbTable;
                    Console.WriteLine($"Importing: {file} -> {tableName}");
                    CopySingleFileToTable(file, outputConn, tableName);
                    Console.WriteLine($"Completed: {file}");
                });
            }
        }

        private static void CopySingleFileToTable(string inputFile, ConnectionInfo outputConn, string tableName)
        {
            // Sample first 100 rows to determine column sizes
            Console.WriteLine($"  Sampling file to determine column sizes...");
            var columnSizes = DelimitedFileReader.SampleColumnSizes(inputFile, sampleSize: 100, buffer: 10);

            Console.WriteLine($"  Creating/updating table with {columnSizes.Count} columns...");
            DatabaseWriter.EnsureTableExistsWithSizes(outputConn, tableName, columnSizes);

            // Now read and insert all rows
            var headers = columnSizes.Keys.ToArray();
            var rows = DelimitedFileReader.ReadFile(inputFile);

            Console.WriteLine($"  Bulk inserting data...");

            using (var progress = new ProgressTracker($"File {Path.GetFileName(inputFile)} -> {tableName}"))
            {
                var batch = new List<Dictionary<string, string>>();
                foreach (var row in rows)
                {
                    batch.Add(row);

                    if (batch.Count >= 10000)
                    {
                        DatabaseWriter.BulkInsertWithSizing(outputConn, tableName, batch, headers, columnSizes);
                        progress.IncrementRows(batch.Count);
                        batch.Clear();
                    }
                }

                if (batch.Count > 0)
                {
                    DatabaseWriter.BulkInsertWithSizing(outputConn, tableName, batch, headers, columnSizes);
                    progress.IncrementRows(batch.Count);
                }
            }
        }

        private static void CopyFileToFile(string input, string output)
        {
            var files = GetInputFiles(input);
            var isOutputDirectory = Directory.Exists(output) || (!File.Exists(output) && !Path.HasExtension(output));

            Console.WriteLine($"Found {files.Length} files to copy");

            if (files.Length == 1 && !isOutputDirectory)
            {
                // Single file to single file
                var file = files[0];
                Console.WriteLine($"Copying: {file} -> {output}");
                CopySingleFileToFile(file, output);
            }
            else if (isOutputDirectory)
            {
                // Multiple files to directory (one-to-one)
                if (!Directory.Exists(output))
                {
                    Directory.CreateDirectory(output);
                }

                Parallel.ForEach(files, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, file =>
                {
                    var fileName = Path.GetFileName(file);
                    var outputFile = Path.Combine(output, fileName);
                    Console.WriteLine($"Copying: {file} -> {outputFile}");
                    CopySingleFileToFile(file, outputFile);
                });
            }
            else
            {
                // Multiple files concatenated to single file
                Console.WriteLine($"Concatenating {files.Length} files -> {output}");
                ConcatenateFilesToSingleFile(files, output);
            }
        }

        private static void CopySingleFileToFile(string inputFile, string outputFile)
        {
            var headers = DelimitedFileReader.GetHeaders(inputFile);
            var rows = DelimitedFileReader.ReadFile(inputFile);
            var delimiter = FileWriter.GetDelimiterFromExtension(outputFile);

            FileWriter.WriteFile(outputFile, rows, headers, delimiter);
        }

        private static void ConcatenateFilesToSingleFile(string[] files, string outputFile)
        {
            // Get headers from first file
            var headers = DelimitedFileReader.GetHeaders(files[0]);
            var delimiter = FileWriter.GetDelimiterFromExtension(outputFile);

            // Collect all rows from all files
            var allRows = new List<Dictionary<string, string>>();

            foreach (var file in files)
            {
                var fileHeaders = DelimitedFileReader.GetHeaders(file);

                // Verify headers match
                if (!headers.SequenceEqual(fileHeaders))
                {
                    Console.WriteLine($"Warning: File {file} has different headers, skipping");
                    continue;
                }

                var rows = DelimitedFileReader.ReadFile(file);
                allRows.AddRange(rows);
            }

            FileWriter.WriteFile(outputFile, allRows, headers, delimiter);
        }

        private static string[] GetInputFiles(string input)
        {
            if (File.Exists(input))
            {
                return new[] { input };
            }

            if (Directory.Exists(input))
            {
                return Directory.GetFiles(input, "*.*", SearchOption.TopDirectoryOnly)
                    .Where(f => f.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) ||
                                f.EndsWith(".tsv", StringComparison.OrdinalIgnoreCase) ||
                                f.EndsWith(".psv", StringComparison.OrdinalIgnoreCase) ||
                                f.EndsWith(".txt", StringComparison.OrdinalIgnoreCase))
                    .ToArray();
            }

            // Handle wildcards
            var directory = Path.GetDirectoryName(input);
            if (string.IsNullOrEmpty(directory))
            {
                directory = Directory.GetCurrentDirectory();
            }

            var searchPattern = Path.GetFileName(input);
            return Directory.GetFiles(directory, searchPattern, SearchOption.TopDirectoryOnly);
        }
    }
}
