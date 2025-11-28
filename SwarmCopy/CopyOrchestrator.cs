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

            // Special case: When copying from SQL Server's default schema (dbo) to DuckDB,
            // use DuckDB's default schema (main) instead - "default to default"
            if (outputConn.IsDuckDb && outputConn.DbSchema == "dbo")
            {
                outputConn.DbSchema = "main";
                Console.WriteLine($"Converting schema from 'dbo' to 'main' for DuckDB");
            }

            if (inputConn.DbTable == "*")
            {
                // Copy all tables
                var tables = DatabaseReader.GetAllTables(inputConn);
                Console.WriteLine($"Found {tables.Length} tables to copy");

                if (outputConn.IsDuckDb)
                {
                    // Two-phase approach for DuckDB: CSV export then bulk load
                    CopyDatabaseToDuckDBViaCsv(inputConn, outputConn, tables);
                }
                else
                {
                    // Direct copy for SQL Server
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
            }
            else
            {
                // Copy single table with partitioning for I/O efficiency
                var partitionCount = Environment.ProcessorCount * THREAD_MULTIPLIER;
                var outputTable = string.IsNullOrEmpty(outputConn.DbTable) ? inputConn.DbTable : outputConn.DbTable;
                Console.WriteLine($"Using {partitionCount} partitions for single table ({Environment.ProcessorCount} cores × {THREAD_MULTIPLIER})");

                CopySingleTableToTable(inputConn, outputConn, inputConn.DbTable, outputTable, partitionCount);
            }
        }

        private static void CopyDatabaseToDuckDBViaCsv(ConnectionInfo inputConn, ConnectionInfo outputConn, string[] tables)
        {
            // Create temp directory for CSV files
            var tempDir = Path.Combine(Path.GetTempPath(), $"SwarmCopy_{Guid.NewGuid()}");
            Directory.CreateDirectory(tempDir);
            Console.WriteLine($"Using temp directory: {tempDir}");

            try
            {
                Console.WriteLine($"\nPipelined export and load of {tables.Length} tables...");

                // Track progress
                int exportedCount = 0;
                int loadedCount = 0;
                long totalRowsExported = 0;
                var exportTimer = new System.Timers.Timer(60000); // 60 seconds
                var loadTimer = new System.Timers.Timer(60000);
                string currentLoadingTable = null;
                var currentLoadStart = DateTime.Now;

                exportTimer.Elapsed += (s, e) =>
                {
                    Console.WriteLine($"  [Export Progress] {exportedCount}/{tables.Length} tables, {totalRowsExported:N0} rows exported");
                };
                exportTimer.Start();

                loadTimer.Elapsed += (s, e) =>
                {
                    if (currentLoadingTable != null)
                    {
                        var elapsed = DateTime.Now - currentLoadStart;
                        Console.WriteLine($"  [Load Progress] Still loading: {currentLoadingTable} - {elapsed.TotalSeconds:F0}s elapsed ({loadedCount}/{tables.Length} tables completed)");
                    }
                };
                loadTimer.Start();

                // Use BlockingCollection for producer-consumer pattern
                using (var completedTables = new System.Collections.Concurrent.BlockingCollection<string>())
                {
                    // Start DuckDB loader task (consumer)
                    var loadTask = Task.Run(() =>
                    {
                        try
                        {
                            using (var connection = new DuckDB.NET.Data.DuckDBConnection(outputConn.GetConnectionString()))
                            {
                                connection.Open();

                                // Configure DuckDB for performance
                                using (var configCmd = connection.CreateCommand())
                                {
                                    configCmd.CommandText = @"
                                        PRAGMA memory_limit='8GB';
                                        PRAGMA threads=24;
                                    ";
                                    configCmd.ExecuteNonQuery();
                                }

                                // Create schema if needed
                                if (!string.IsNullOrEmpty(outputConn.DbSchema))
                                {
                                    using (var cmd = connection.CreateCommand())
                                    {
                                        cmd.CommandText = $"CREATE SCHEMA IF NOT EXISTS {outputConn.DbSchema}";
                                        cmd.ExecuteNonQuery();
                                    }
                                }

                                // Load tables as they become available
                                foreach (var table in completedTables.GetConsumingEnumerable())
                                {
                                    try
                                    {
                                        currentLoadingTable = table;
                                        currentLoadStart = DateTime.Now;
                                        Console.WriteLine($"  Loading: {table}");
                                        var csvFile = Path.Combine(tempDir, $"{table}.csv");
                                        var qualifiedTableName = outputConn.GetQualifiedTableName(table);

                                        // Use DuckDB's fast read_csv() bulk load
                                        using (var cmd = connection.CreateCommand())
                                        {
                                            cmd.CommandText = $"CREATE OR REPLACE TABLE {qualifiedTableName} AS SELECT * FROM read_csv('{csvFile.Replace("\\", "\\\\")}', header=true, all_varchar=true)";
                                            cmd.ExecuteNonQuery();
                                        }

                                        Interlocked.Increment(ref loadedCount);
                                        currentLoadingTable = null;
                                        Console.WriteLine($"  Completed load: {table}");
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"  ERROR loading {table}: {ex.Message}");
                                        currentLoadingTable = null;
                                        throw;
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"  FATAL ERROR in load task: {ex.Message}");
                            Console.WriteLine($"  Stack: {ex.StackTrace}");
                            throw;
                        }
                    });

                    // Export tables in parallel (producers)
                    Parallel.ForEach(tables, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, table =>
                    {
                        Console.WriteLine($"  Exporting: {table}");
                        var csvFile = Path.Combine(tempDir, $"{table}.csv");
                        var rowCount = CopySingleTableToFile(inputConn, table, csvFile);
                        Interlocked.Add(ref totalRowsExported, rowCount);
                        Interlocked.Increment(ref exportedCount);
                        Console.WriteLine($"  Completed export: {table} ({rowCount:N0} rows)");

                        // Add to queue for loading
                        completedTables.Add(table);
                    });

                    // Signal that no more tables will be added
                    completedTables.CompleteAdding();

                    // Wait for all loads to complete
                    loadTask.Wait();
                }

                exportTimer.Stop();
                loadTimer.Stop();
                exportTimer.Dispose();
                loadTimer.Dispose();

                // Verify and display summary
                Console.WriteLine($"\n=== VERIFICATION SUMMARY ===");
                Console.WriteLine($"{"Table",-40} {"Exported",-15} {"Loaded",-15} {"Status",-10}");
                Console.WriteLine(new string('-', 85));

                using (var connection = new DuckDB.NET.Data.DuckDBConnection(outputConn.GetConnectionString()))
                {
                    connection.Open();

                    long totalExported = 0;
                    long totalLoaded = 0;
                    int mismatches = 0;

                    foreach (var table in tables)
                    {
                        // Get exported count from CSV
                        var csvFile = Path.Combine(tempDir, $"{table}.csv");
                        long exportedRows = 0;
                        if (File.Exists(csvFile))
                        {
                            exportedRows = File.ReadLines(csvFile).Count() - 1; // -1 for header
                        }

                        // Get loaded count from DuckDB
                        var qualifiedTableName = outputConn.GetQualifiedTableName(table);
                        long loadedRows = 0;
                        using (var cmd = connection.CreateCommand())
                        {
                            cmd.CommandText = $"SELECT COUNT(*) FROM {qualifiedTableName}";
                            loadedRows = Convert.ToInt64(cmd.ExecuteScalar());
                        }

                        totalExported += exportedRows;
                        totalLoaded += loadedRows;

                        var status = exportedRows == loadedRows ? "OK" : "MISMATCH";
                        if (exportedRows != loadedRows) mismatches++;

                        Console.WriteLine($"{table,-40} {exportedRows,-15:N0} {loadedRows,-15:N0} {status,-10}");
                    }

                    Console.WriteLine(new string('-', 85));
                    Console.WriteLine($"{"TOTAL",-40} {totalExported,-15:N0} {totalLoaded,-15:N0} {(mismatches == 0 ? "OK" : $"{mismatches} ERRORS"),-10}");
                    Console.WriteLine(new string('=', 85));
                }

                Console.WriteLine($"\nCompleted all {tables.Length} tables");
            }
            finally
            {
                // Clean up temp CSV files
                if (Directory.Exists(tempDir))
                {
                    Console.WriteLine($"Cleaning up temp directory: {tempDir}");
                    Directory.Delete(tempDir, true);
                }
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

        private static long CopySingleTableToFile(ConnectionInfo inputConn, string tableName, string outputFile, Action<long> progressCallback = null)
        {
            var columns = DatabaseReader.GetTableColumns(inputConn, tableName);
            var rows = DatabaseReader.ReadTable(inputConn, tableName);
            var delimiter = FileWriter.GetDelimiterFromExtension(outputFile);

            return FileWriter.WriteFile(outputFile, rows, columns, delimiter, progressCallback);
        }

        private static void CopyFileToDatabase(string input, string output)
        {
            var outputConn = ConnectionInfo.Parse(output);

            // Special case: When writing to DuckDB with schema 'dbo', use 'main' instead
            if (outputConn.IsDuckDb && outputConn.DbSchema == "dbo")
            {
                outputConn.DbSchema = "main";
                Console.WriteLine($"Converting schema from 'dbo' to 'main' for DuckDB");
            }

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
                // Multiple files - check if going to same table or different tables
                var singleTargetTable = !string.IsNullOrEmpty(outputConn.DbTable);

                if (singleTargetTable)
                {
                    // All files go to same table - create table once, then parallel load
                    var tableName = outputConn.DbTable;
                    Console.WriteLine($"Loading {files.Length} files to single table: {tableName}");

                    // Sample first file to create table structure
                    Console.WriteLine($"  Sampling first file to determine column sizes...");
                    var columnSizes = DelimitedFileReader.SampleColumnSizes(files[0], sampleSize: 100, buffer: 10);

                    // Create table once
                    Console.WriteLine($"  Creating table with {columnSizes.Count} columns...");
                    DatabaseWriter.EnsureTableExistsWithSizes(outputConn, tableName, columnSizes);

                    // Now load all files in parallel (skip table creation)
                    Parallel.ForEach(files, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, file =>
                    {
                        Console.WriteLine($"Importing: {file} -> {tableName}");
                        CopySingleFileToTable(file, outputConn, tableName, skipTableCreation: true);
                        Console.WriteLine($"Completed: {file}");
                    });
                }
                else
                {
                    // Each file goes to its own table (named after the file)
                    Parallel.ForEach(files, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, file =>
                    {
                        var tableName = Path.GetFileNameWithoutExtension(file);
                        Console.WriteLine($"Importing: {file} -> {tableName}");
                        CopySingleFileToTable(file, outputConn, tableName);
                        Console.WriteLine($"Completed: {file}");
                    });
                }
            }
        }

        private static void CopySingleFileToTable(string inputFile, ConnectionInfo outputConn, string tableName, bool skipTableCreation = false)
        {
            Dictionary<string, int> columnSizes;
            string[] headers;

            if (!skipTableCreation)
            {
                // Sample first 100 rows to determine column sizes
                Console.WriteLine($"  Sampling file to determine column sizes...");
                columnSizes = DelimitedFileReader.SampleColumnSizes(inputFile, sampleSize: 100, buffer: 10);

                Console.WriteLine($"  Creating/updating table with {columnSizes.Count} columns...");
                DatabaseWriter.EnsureTableExistsWithSizes(outputConn, tableName, columnSizes);

                headers = columnSizes.Keys.ToArray();
            }
            else
            {
                // Table already exists - just get column info from first row
                var firstRow = DelimitedFileReader.ReadFile(inputFile).First();
                headers = firstRow.Keys.ToArray();
                columnSizes = headers.ToDictionary(h => h, h => 0); // Sizes don't matter since table exists
            }

            // Now read and insert all rows
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
