# SwarmCopy Technical Architecture

## Overview

SwarmCopy is designed for maximum performance through multi-threading, bulk operations, and streaming data processing. The architecture follows a modular design with clear separation of concerns.

## Core Components

### 1. ConnectionInfo.cs
**Purpose**: Parse and manage database connection strings

**Key Features**:
- Parses custom connection string format: `db:dbname=x&dbhost=y&...`
- Supports both Windows Authentication and SQL Authentication
- Converts to ADO.NET connection string format
- Provides defaults (localhost:1433)

**Methods**:
```csharp
ConnectionInfo Parse(string connectionString)
string GetConnectionString()
```

### 2. ArgumentParser.cs
**Purpose**: Parse command-line arguments and display help

**Key Features**:
- Simple argument parsing for `-i` and `-o` flags
- Identifies database vs file inputs/outputs
- Comprehensive help display with examples

**Methods**:
```csharp
CopyArguments Parse(string[] args)
void ShowHelp()
```

### 3. DelimitedFileReader.cs
**Purpose**: Read delimited files with auto-detection and robust CSV parsing

**Key Features**:
- Auto-detects delimiter by counting occurrences in first line
- Uses CsvHelper library for RFC4180 compliance
- Handles embedded newlines, commas, quotes
- Streams data (yield return) to minimize memory
- Returns data as `Dictionary<string, string>` for flexibility

**Methods**:
```csharp
char DetectDelimiter(string filePath)
IEnumerable<Dictionary<string, string>> ReadFile(string filePath, char? delimiter)
string[] GetHeaders(string filePath, char? delimiter)
```

**Performance Notes**:
- Uses `yield return` for streaming
- Minimal memory footprint even for large files
- Bad data and missing fields are gracefully handled

### 4. FileWriter.cs
**Purpose**: Write delimited files with proper formatting

**Key Features**:
- Uses CsvHelper for proper quoting and escaping
- Determines delimiter from file extension
- Handles all CSV edge cases correctly

**Methods**:
```csharp
void WriteFile(string filePath, IEnumerable<Dictionary<string, string>> rows, string[] headers, char delimiter)
char GetDelimiterFromExtension(string filePath)
```

### 5. DatabaseReader.cs
**Purpose**: Read data from SQL Server tables

**Key Features**:
- Supports partitioned reads using `BINARY_CHECKSUM` modulo
- Streams results using `yield return`
- All data converted to strings
- NULL → empty string conversion
- No query timeout for large tables
- Schema inspection capabilities

**Methods**:
```csharp
IEnumerable<Dictionary<string, string>> ReadTable(ConnectionInfo, string tableName, int? partitionIndex, int? partitionCount)
string[] GetTableColumns(ConnectionInfo, string tableName)
string[] GetAllTables(ConnectionInfo)
bool TableExists(ConnectionInfo, string tableName)
```

**Partitioning Strategy**:
```sql
WHERE ABS(BINARY_CHECKSUM(*)) % 10 = 0  -- Partition 0
WHERE ABS(BINARY_CHECKSUM(*)) % 10 = 1  -- Partition 1
...
WHERE ABS(BINARY_CHECKSUM(*)) % 10 = 9  -- Partition 9
```

This distributes rows across threads evenly without requiring indexed columns.

### 6. DatabaseWriter.cs
**Purpose**: Write data to SQL Server using bulk operations

**Key Features**:
- Uses `SqlBulkCopy` for maximum performance
- Creates tables dynamically with `NVARCHAR(MAX)` columns
- Adds missing columns to existing tables
- Batches inserts (10,000 rows per batch)
- No timeout for large operations

**Methods**:
```csharp
void CreateTable(ConnectionInfo, string tableName, string[] columns)
void BulkInsert(ConnectionInfo, string tableName, IEnumerable<Dictionary<string, string>> rows, string[] columns)
void TruncateTable(ConnectionInfo, string tableName)
void EnsureTableExists(ConnectionInfo, string tableName, string[] columns)
```

**Performance Notes**:
- Bulk insert is 100-1000x faster than individual inserts
- Batch size of 10,000 balances memory and network
- DataTable is reused and cleared for memory efficiency

### 7. CopyOrchestrator.cs
**Purpose**: Coordinate all copy operations with multi-threading

**Key Features**:
- Determines copy mode based on input/output types
- Manages threading strategy
- Handles wildcards and directory operations
- Implements all copy scenarios

**Copy Modes**:
1. Database → Database
2. Database → File
3. File → Database
4. File → File

**Threading Strategies**:

**File-to-File (Multiple)**:
```csharp
Parallel.ForEach(files, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, file => {
    // Process each file
});
```

**Table-to-Table**:
```csharp
const int DEFAULT_PARTITION_COUNT = 10;
for (int i = 0; i < partitionCount; i++) {
    Task.Run(() => {
        var rows = DatabaseReader.ReadTable(inputConn, inputTable, partitionIndex, partitionCount);
        DatabaseWriter.BulkInsert(outputConn, outputTable, rows, columns);
    });
}
```

**Database-to-Database (All Tables)**:
```csharp
Parallel.ForEach(tables, table => {
    CopySingleTableToTable(inputConn, outputConn, table, table);
    // Each table copy uses 10 threads internally
});
```

### 8. Program.cs
**Purpose**: Main entry point and error handling

**Key Features**:
- Validates arguments
- Times operations
- Catches and displays errors
- Returns proper exit codes

## Data Flow

### File to Database

```
CSV File
  ↓
DelimitedFileReader.ReadFile() [Streams rows]
  ↓
Dictionary<string, string>[] (batches of 10,000)
  ↓
DatabaseWriter.BulkInsert() [SqlBulkCopy]
  ↓
SQL Server Table
```

### Database to File

```
SQL Server Table
  ↓
DatabaseReader.ReadTable() [Streams rows]
  ↓
Dictionary<string, string>[]
  ↓
FileWriter.WriteFile() [CsvHelper]
  ↓
CSV File
```

### Table to Table (Multi-threaded)

```
Source Table
  ├─ Thread 1: WHERE CHECKSUM % 10 = 0 → Batch Insert → Dest Table
  ├─ Thread 2: WHERE CHECKSUM % 10 = 1 → Batch Insert → Dest Table
  ├─ Thread 3: WHERE CHECKSUM % 10 = 2 → Batch Insert → Dest Table
  ├─ ...
  └─ Thread 10: WHERE CHECKSUM % 10 = 9 → Batch Insert → Dest Table
```

## Performance Characteristics

### Memory Usage
- **Streaming**: Data is processed in streams, not loaded entirely into memory
- **Batching**: 10,000 row batches for bulk insert
- **Thread overhead**: ~1MB per thread
- **Total**: Can process GB-scale files with <100MB RAM

### Threading
- **File operations**: Up to CPU core count
- **Table operations**: 10 threads per table
- **All-table migrations**: CPU core count tables, each with 10 threads

### Database Performance
- **SqlBulkCopy**: ~50,000-100,000 rows/second (depends on network/server)
- **Binary Checksum Partitioning**: Near-linear speedup with thread count
- **No indexes**: Assumes tables created without indexes for max speed

### Bottlenecks
1. **Network**: For remote databases, network is often the bottleneck
2. **Disk I/O**: For file operations, disk speed matters
3. **SQL Server**: CPU and memory on SQL Server side

## Extension Points

### Adding New Delimiters
Edit `DelimitedFileReader.DetectDelimiter()`:
```csharp
var semicolonCount = firstLine.Count(c => c == ';');
if (semicolonCount > commaCount && semicolonCount > tabCount && semicolonCount > pipeCount)
    return ';';
```

### Adding New Database Types
1. Create `DatabaseReader_PostgreSQL.cs` and `DatabaseWriter_PostgreSQL.cs`
2. Update `ConnectionInfo` to detect database type
3. Update `CopyOrchestrator` to route to correct reader/writer

### Changing Partition Count
Edit `CopyOrchestrator.cs`:
```csharp
private const int DEFAULT_PARTITION_COUNT = 20; // Change from 10 to 20
```

### Custom Batch Size
Edit `DatabaseWriter.cs`:
```csharp
bulkCopy.BatchSize = 50000; // Change from 10000
```

### Adding Compression
Add to `FileWriter`:
```csharp
using (var fileStream = new FileStream(filePath, FileMode.Create))
using (var gzipStream = new GZipStream(fileStream, CompressionMode.Compress))
using (var writer = new StreamWriter(gzipStream))
using (var csv = new CsvWriter(writer, config))
{
    // ... existing write code
}
```

## Dependencies

### NuGet Packages
- **System.Data.SqlClient** (4.8.6): SQL Server connectivity
- **CsvHelper** (27.2.1): Robust CSV parsing

### Framework
- **.NET Core 3.1**: Long-term support, widely compatible

## Design Decisions

### Why All Strings?
- **Simplicity**: No type mapping complexity
- **Flexibility**: Works with any data type
- **Performance**: String conversion is fast
- **Reliability**: No data loss from type mismatches

### Why NVARCHAR(MAX)?
- **Unicode support**: Works with international data
- **No truncation**: MAX handles any length
- **Flexible**: Can store any data
- **Simple**: No column size analysis needed

### Why Binary Checksum?
- **No index required**: Works on any table
- **Deterministic**: Same row always goes to same partition
- **Even distribution**: Generally uniform distribution
- **Fast**: Very efficient hash function

### Why 10 Partitions?
- **Balance**: Good balance of parallelism vs overhead
- **CPU friendly**: Most servers have 8-16 cores
- **Memory**: Reasonable memory usage
- **Tunable**: Easy to change if needed

### Why CsvHelper?
- **RFC4180 compliant**: Handles all CSV edge cases
- **Battle-tested**: Widely used library
- **Performant**: Optimized for speed
- **Maintained**: Active development

## Testing Recommendations

### Unit Tests
- Test delimiter detection with various files
- Test connection string parsing
- Test wildcard expansion
- Test column mismatch handling

### Integration Tests
- Test file to file copy
- Test file to database (with test DB)
- Test database to file
- Test partitioned reads

### Performance Tests
- 1M row file import
- 10M row table copy
- 100 small files
- All-table migration

### Stress Tests
- Very wide tables (1000 columns)
- Very long strings
- Unicode data
- Malformed CSV files

## Monitoring and Logging

### Current Logging
```csharp
Console.WriteLine($"Found {tables.Length} tables to copy");
Console.WriteLine($"Copying table: {table}");
Console.WriteLine($"Completed table: {table}");
Console.WriteLine($"Partition {partitionIndex + 1}/{partitionCount} completed");
```

### Recommended Additions
- Row counts processed
- Throughput metrics (rows/second)
- Error counts and details
- Memory usage tracking
- Thread utilization

### Example Enhancement
```csharp
var rowCount = 0;
var stopwatch = Stopwatch.StartNew();
foreach (var row in rows) {
    rowList.Add(row);
    rowCount++;

    if (rowCount % 100000 == 0) {
        var rate = rowCount / stopwatch.Elapsed.TotalSeconds;
        Console.WriteLine($"  {rowCount:N0} rows processed ({rate:N0} rows/sec)");
    }
}
```

## Security Considerations

### Connection Strings
- Passwords in command-line arguments visible in process list
- Consider environment variables or config files for production
- Use Windows Authentication when possible

### SQL Injection
- Current implementation uses parameterized queries for schema operations
- Table names are bracketed: `[TableName]`
- Consider additional validation for table names from user input

### File System Access
- No sandboxing of file paths
- Can read/write anywhere user has permissions
- Consider restricting to specific directories in production

## Future Enhancements

### Potential Features
1. **Resume capability**: Save progress and resume interrupted copies
2. **Transformation support**: Apply functions during copy
3. **Filtering**: WHERE clauses for database queries
4. **Compression**: GZip support for files
5. **Validation**: Row count verification
6. **Progress bar**: Visual progress indicator
7. **Logging to file**: Detailed operation logs
8. **Configuration file**: Store common connections
9. **Scheduling**: Built-in task scheduler
10. **Change tracking**: Only copy changed rows
