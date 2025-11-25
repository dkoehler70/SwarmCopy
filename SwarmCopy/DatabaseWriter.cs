using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;

namespace SwarmCopy
{
    public class DatabaseWriter
    {
        private const int BUFFER_SIZE = 10;
        private const int MAX_THRESHOLD = 512; // Switch to VARCHAR(MAX) at this size
        private const int MAX_RETRIES = 5;
        private static readonly object ResizeLock = new object(); // Thread-safe column resizing

        public static void CreateTable(ConnectionInfo connInfo, string tableName, string[] columns)
        {
            using (var connection = new SqlConnection(connInfo.GetConnectionString()))
            {
                connection.Open();

                // Drop table if exists
                var dropQuery = $"IF OBJECT_ID('[{tableName}]', 'U') IS NOT NULL DROP TABLE [{tableName}]";
                using (var dropCommand = new SqlCommand(dropQuery, connection))
                {
                    dropCommand.ExecuteNonQuery();
                }

                // Create table with all columns as VARCHAR(MAX)
                var columnDefinitions = string.Join(", ", columns.Select(c => $"[{c}] VARCHAR(MAX) NULL"));
                var createQuery = $"CREATE TABLE [{tableName}] ({columnDefinitions})";

                using (var createCommand = new SqlCommand(createQuery, connection))
                {
                    createCommand.ExecuteNonQuery();
                }
            }
        }

        public static void CreateTableWithSizes(ConnectionInfo connInfo, string tableName, Dictionary<string, int> columnSizes)
        {
            using (var connection = new SqlConnection(connInfo.GetConnectionString()))
            {
                connection.Open();

                var qualifiedTableName = connInfo.GetQualifiedTableName(tableName);

                // Drop table if overwrite mode
                if (connInfo.IsOverwrite)
                {
                    var dropQuery = $"IF OBJECT_ID('{qualifiedTableName}', 'U') IS NOT NULL DROP TABLE {qualifiedTableName}";
                    using (var dropCommand = new SqlCommand(dropQuery, connection))
                    {
                        dropCommand.ExecuteNonQuery();
                    }
                }

                // Adjust column sizes to ensure row doesn't exceed 8060 bytes
                var adjustedSizes = AdjustColumnSizesForRowLimit(columnSizes);

                // Create table with specified column sizes
                // Use VARCHAR(MAX) for columns >= 512 chars or adjusted to MAX
                var columnDefinitions = string.Join(", ", adjustedSizes.Select(kvp =>
                {
                    var size = kvp.Value >= MAX_THRESHOLD || kvp.Value == int.MaxValue ? "MAX" : kvp.Value.ToString();
                    return $"[{kvp.Key}] VARCHAR({size}) NULL";
                }));
                var createQuery = $"CREATE TABLE {qualifiedTableName} ({columnDefinitions})";

                using (var createCommand = new SqlCommand(createQuery, connection))
                {
                    createCommand.ExecuteNonQuery();
                }
            }
        }

        private static Dictionary<string, int> AdjustColumnSizesForRowLimit(Dictionary<string, int> columnSizes)
        {
            const int MAX_ROW_SIZE = 8060; // SQL Server row size limit
            var adjusted = new Dictionary<string, int>(columnSizes);

            // Calculate total row size (VARCHAR uses 1 byte per character)
            // Only count columns that aren't already MAX
            var regularColumns = adjusted.Where(kvp => kvp.Value < MAX_THRESHOLD && kvp.Value != int.MaxValue).ToList();
            var totalRowSize = regularColumns.Sum(kvp => kvp.Value);

            if (totalRowSize > MAX_ROW_SIZE)
            {
                // Need to convert some columns to VARCHAR(MAX)
                // Start with the largest columns first
                var sortedColumns = regularColumns.OrderByDescending(kvp => kvp.Value).ToList();

                foreach (var kvp in sortedColumns)
                {
                    // Convert this column to MAX
                    adjusted[kvp.Key] = int.MaxValue;
                    totalRowSize -= kvp.Value;

                    if (totalRowSize <= MAX_ROW_SIZE)
                        break;
                }
            }

            return adjusted;
        }

        public static void ResizeColumn(ConnectionInfo connInfo, string tableName, string columnName, int newSize)
        {
            // Thread-safe: Lock to prevent concurrent ALTER TABLE operations
            lock (ResizeLock)
            {
                using (var connection = new SqlConnection(connInfo.GetConnectionString()))
                {
                    connection.Open();

                    // Get current size first to avoid unnecessary resizes
                    var currentSizes = GetColumnSizes(connInfo, tableName);
                    var oldSize = "NEW";
                    if (currentSizes.TryGetValue(columnName, out var currentSize))
                    {
                        if (currentSize >= newSize)
                        {
                            // Already big enough, skip resize
                            return;
                        }
                        oldSize = currentSize == int.MaxValue ? "MAX" : currentSize.ToString();
                    }

                    // Use VARCHAR(MAX) for columns >= 512 chars
                    var size = newSize >= MAX_THRESHOLD || newSize == int.MaxValue ? "MAX" : newSize.ToString();
                    var qualifiedTableName = connInfo.GetQualifiedTableName(tableName);
                    var alterQuery = $"ALTER TABLE {qualifiedTableName} ALTER COLUMN [{columnName}] VARCHAR({size}) NULL";

                    Console.WriteLine($"  Resizing column [{tableName}].[{columnName}]: {oldSize} -> {size}");

                    using (var command = new SqlCommand(alterQuery, connection))
                    {
                        command.CommandTimeout = 120; // 2 minute timeout for ALTER TABLE
                        command.ExecuteNonQuery();
                    }
                }
            }
        }

        public static Dictionary<string, int> GetColumnSizes(ConnectionInfo connInfo, string tableName)
        {
            var columnSizes = new Dictionary<string, int>();

            using (var connection = new SqlConnection(connInfo.GetConnectionString()))
            {
                connection.Open();

                var query = @"
                    SELECT COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = @TableSchema AND TABLE_NAME = @TableName
                    ORDER BY ORDINAL_POSITION";

                using (var command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@TableSchema", connInfo.DbSchema);
                    command.Parameters.AddWithValue("@TableName", tableName);

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var columnName = reader.GetString(0);
                            var maxLength = reader.IsDBNull(1) ? -1 : reader.GetInt32(1);
                            columnSizes[columnName] = maxLength == -1 ? int.MaxValue : maxLength;
                        }
                    }
                }
            }

            return columnSizes;
        }

        public static void BulkInsert(ConnectionInfo connInfo, string tableName, IEnumerable<Dictionary<string, string>> rows, string[] columns)
        {
            BulkInsertWithSizing(connInfo, tableName, rows, columns, null);
        }

        public static void BulkInsertWithSizing(ConnectionInfo connInfo, string tableName, IEnumerable<Dictionary<string, string>> rows, string[] columns, Dictionary<string, int> initialSizes)
        {
            using (var connection = new SqlConnection(connInfo.GetConnectionString()))
            {
                connection.Open();

                // Get current column sizes if not provided
                var currentSizes = initialSizes ?? GetColumnSizes(connInfo, tableName);

                var dataTable = new DataTable();
                foreach (var column in columns)
                {
                    dataTable.Columns.Add(column, typeof(string));
                }

                var batchRows = new List<Dictionary<string, string>>();

                foreach (var row in rows)
                {
                    batchRows.Add(row);

                    // Insert in batches for better performance
                    if (batchRows.Count >= 10000)
                    {
                        InsertBatchWithResize(connInfo, connection, tableName, batchRows, columns, dataTable, currentSizes);
                        batchRows.Clear();
                    }
                }

                // Insert remaining rows
                if (batchRows.Count > 0)
                {
                    InsertBatchWithResize(connInfo, connection, tableName, batchRows, columns, dataTable, currentSizes);
                }
            }
        }

        private static void InsertBatchWithResize(ConnectionInfo connInfo, SqlConnection connection, string tableName, List<Dictionary<string, string>> batchRows,
            string[] columns, DataTable dataTable, Dictionary<string, int> currentSizes)
        {
            // Retry loop with P=5 retries
            for (int retry = 0; retry < MAX_RETRIES; retry++)
            {
                try
                {
                    // Rebuild DataTable structure in case schema changed
                    // This is critical for multi-threaded scenarios where another thread may have resized columns
                    if (retry > 0)
                    {
                        dataTable = RebuildDataTable(columns);

                        // Refresh current sizes from database after potential schema changes
                        if (connInfo != null)
                        {
                            var latestSizes = GetColumnSizes(connInfo, tableName);
                            foreach (var kvp in latestSizes)
                            {
                                currentSizes[kvp.Key] = kvp.Value;
                            }
                        }
                    }

                    // Clear and populate DataTable
                    dataTable.Rows.Clear();
                    foreach (var row in batchRows)
                    {
                        var dataRow = dataTable.NewRow();
                        foreach (var column in columns)
                        {
                            dataRow[column] = row.ContainsKey(column) ? (object)row[column] : DBNull.Value;
                        }
                        dataTable.Rows.Add(dataRow);
                    }

                    // Try bulk insert
                    // Note: SqlBulkCopy needs schema qualification in a special format
                    var qualifiedName = $"[{connInfo.DbSchema}].[{tableName}]";
                    using (var bulkCopy = new SqlBulkCopy(connection))
                    {
                        bulkCopy.DestinationTableName = qualifiedName;
                        bulkCopy.BulkCopyTimeout = 0;
                        bulkCopy.BatchSize = 10000;

                        foreach (var column in columns)
                        {
                            bulkCopy.ColumnMappings.Add(column, column);
                        }

                        bulkCopy.WriteToServer(dataTable);
                    }

                    // Success - break out of retry loop
                    break;
                }
                catch (Exception ex)
                {
                    bool isTruncationError = ex.Message.Contains("String or binary data would be truncated") ||
                                            ex.Message.Contains("truncated");
                    bool isSchemaError = ex.Message.Contains("schema") ||
                                        ex.Message.Contains("ALTER TABLE") ||
                                        ex.Message.Contains("column") && ex.Message.Contains("does not match");
                    bool isLockError = ex.Message.Contains("timeout") ||
                                      ex.Message.Contains("deadlock") ||
                                      ex.Message.Contains("locked");

                    if (retry >= MAX_RETRIES - 1)
                    {
                        Console.WriteLine($"  ERROR: Failed after {MAX_RETRIES} retries: {ex.Message}");
                        throw; // Give up after max retries
                    }

                    // Log the error
                    Console.WriteLine($"  Retry {retry + 1}/{MAX_RETRIES}: {ex.GetType().Name} - {ex.Message.Substring(0, Math.Min(100, ex.Message.Length))}");

                    // Handle truncation errors by resizing columns
                    if (isTruncationError)
                    {
                        Console.WriteLine($"    Analyzing batch for oversized values...");

                        // Find columns that need resizing
                        var columnsToResize = new Dictionary<string, int>();
                        foreach (var row in batchRows)
                        {
                            foreach (var kvp in row)
                            {
                                var columnName = kvp.Key;
                                var valueLength = kvp.Value?.Length ?? 0;

                                if (currentSizes.TryGetValue(columnName, out var currentSize) &&
                                    currentSize != int.MaxValue &&
                                    valueLength > currentSize)
                                {
                                    var newSize = valueLength + BUFFER_SIZE;

                                    // Switch to MAX if >= 512 chars
                                    if (newSize >= MAX_THRESHOLD)
                                    {
                                        newSize = int.MaxValue;
                                    }

                                    if (!columnsToResize.ContainsKey(columnName) || columnsToResize[columnName] < newSize)
                                    {
                                        columnsToResize[columnName] = newSize;
                                    }
                                }
                            }
                        }

                        if (columnsToResize.Count > 0)
                        {
                            // Resize columns (thread-safe with lock inside ResizeColumn)
                            foreach (var kvp in columnsToResize)
                            {
                                var newSize = kvp.Value;
                                var displaySize = newSize == int.MaxValue ? "MAX" : newSize.ToString();
                                var currentDisplay = currentSizes[kvp.Key] == int.MaxValue ? "MAX" : currentSizes[kvp.Key].ToString();

                                Console.WriteLine($"    Resizing column [{kvp.Key}] from {currentDisplay} to {displaySize}");

                                ResizeColumn(connInfo, tableName, kvp.Key, newSize);
                                currentSizes[kvp.Key] = newSize;
                            }
                        }
                    }

                    // Add delay before retry (exponential backoff)
                    var delayMs = 100 * (int)Math.Pow(2, retry); // 100ms, 200ms, 400ms, 800ms, 1600ms
                    Thread.Sleep(delayMs);

                    // Retry will happen in next iteration
                }
            }
        }

        private static DataTable RebuildDataTable(string[] columns)
        {
            var dataTable = new DataTable();
            foreach (var column in columns)
            {
                dataTable.Columns.Add(column, typeof(string));
            }
            return dataTable;
        }

        public static void TruncateTable(ConnectionInfo connInfo, string tableName)
        {
            using (var connection = new SqlConnection(connInfo.GetConnectionString()))
            {
                connection.Open();

                var query = $"TRUNCATE TABLE [{tableName}]";
                using (var command = new SqlCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        public static void EnsureTableExists(ConnectionInfo connInfo, string tableName, string[] columns)
        {
            if (!DatabaseReader.TableExists(connInfo, tableName))
            {
                CreateTable(connInfo, tableName, columns);
            }
            else
            {
                // Verify columns match
                var existingColumns = DatabaseReader.GetTableColumns(connInfo, tableName);
                var missingColumns = columns.Except(existingColumns, StringComparer.OrdinalIgnoreCase).ToArray();

                if (missingColumns.Length > 0)
                {
                    // Add missing columns
                    using (var connection = new SqlConnection(connInfo.GetConnectionString()))
                    {
                        connection.Open();
                        foreach (var column in missingColumns)
                        {
                            var alterQuery = $"ALTER TABLE [{tableName}] ADD [{column}] VARCHAR(MAX) NULL";
                            using (var command = new SqlCommand(alterQuery, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                }
            }
        }

        public static void EnsureTableExistsWithSizes(ConnectionInfo connInfo, string tableName, Dictionary<string, int> columnSizes)
        {
            var tableExists = DatabaseReader.TableExists(connInfo, tableName);

            if (!tableExists)
            {
                // Table doesn't exist - create it
                CreateTableWithSizes(connInfo, tableName, columnSizes);
            }
            else if (connInfo.IsOverwrite)
            {
                // Table exists and we're in overwrite mode - drop and recreate
                CreateTableWithSizes(connInfo, tableName, columnSizes);
            }
            else
            {
                // Table exists and we're in append mode - verify columns match
                var existingColumns = DatabaseReader.GetTableColumns(connInfo, tableName);
                var missingColumns = columnSizes.Keys.Except(existingColumns, StringComparer.OrdinalIgnoreCase).ToArray();

                if (missingColumns.Length > 0)
                {
                    // Add missing columns
                    var qualifiedTableName = connInfo.GetQualifiedTableName(tableName);
                    using (var connection = new SqlConnection(connInfo.GetConnectionString()))
                    {
                        connection.Open();
                        foreach (var column in missingColumns)
                        {
                            // Use VARCHAR(MAX) for columns >= 512 chars
                            var size = columnSizes[column] >= MAX_THRESHOLD || columnSizes[column] == int.MaxValue ? "MAX" : columnSizes[column].ToString();
                            var alterQuery = $"ALTER TABLE {qualifiedTableName} ADD [{column}] VARCHAR({size}) NULL";
                            using (var command = new SqlCommand(alterQuery, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                }
            }
        }
    }
}
