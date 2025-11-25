# SwarmCopy

SwarmCopy is a high-performance C# console application for copying data between delimited files and SQL Server databases. Built for speed, it leverages multi-threading and bulk operations to handle large-scale data transfers efficiently.

## Features

- **Multi-format Support**: CSV, TSV (tab-delimited), PSV (pipe-delimited) files
- **Auto-detection**: Automatically detects file delimiters
- **Robust CSV Parsing**: Handles line breaks, embedded commas, quotes, and other edge cases
- **Multi-threaded Operations**: Parallel processing for maximum performance
- **Database Operations**: SQL Server support with bulk insert and parallel partitioning
- **Flexible Copy Modes**:
  - File to Database Table
  - Database Table to File
  - File to File
  - Database Table to Table
  - Wildcard file operations
  - All-table database migrations

## Requirements

- .NET Core 3.1 Runtime
- SQL Server (for database operations)

## Installation

Build the project:
```bash
cd SwarmCopy
dotnet build -c Release
```

The executable will be in: `bin/Release/netcoreapp3.1/SwarmCopy.dll`

## Usage

```bash
SwarmCopy -i <input> -o <output>
```

### Arguments

- `-i <input>` - Input source (file path, directory, or database connection)
- `-o <output>` - Output destination (file path, directory, or database connection)

### Database Connection Format

```
db:dbname=<name>&dbhost=<host>&dbport=<port>&dbusername=<user>&dbpassword=<pass>&dbtable=<table>
```

**Parameters:**
- `dbname` - Database name (required)
- `dbhost` - Host name (default: localhost)
- `dbport` - Port number (default: 1433)
- `dbusername` - Username (omit for Windows authentication)
- `dbpassword` - Password (omit for Windows authentication)
- `dbtable` - Table name (use `*` for all tables)

## Examples

### File to Database
Import a CSV file into a database table:
```bash
SwarmCopy -i data.csv -o "db:dbname=mydb&dbtable=customers"
```

### Database to File
Export a database table to CSV:
```bash
SwarmCopy -i "db:dbname=mydb&dbtable=customers" -o output.csv
```

### Multiple Files to Database
Import all CSV files in current directory:
```bash
SwarmCopy -i "*.csv" -o "db:dbname=mydb"
```
Each file becomes a table named after the filename (without extension).

### File to File
Copy and convert between formats:
```bash
SwarmCopy -i data.csv -o data.tsv
```

### Table to Table (High-Speed Multi-threaded)
Copy a table from one database to another using 10 parallel threads:
```bash
SwarmCopy -i "db:dbname=sourcedb&dbtable=bigtable" -o "db:dbname=destdb&dbtable=bigtable"
```

The table-to-table copy uses `BINARY_CHECKSUM` modulo partitioning to split the work across 10 threads for maximum performance.

### All Tables Migration
Copy all tables from one database to another (even across different servers):
```bash
SwarmCopy -i "db:dbname=sourcedb&dbhost=server1&dbtable=*" -o "db:dbname=destdb&dbhost=server2"
```

### Windows Authentication
Omit username and password to use Windows authentication:
```bash
SwarmCopy -i "db:dbname=mydb&dbtable=customers" -o output.csv
```

### Remote SQL Server
Specify host and port:
```bash
SwarmCopy -i "db:dbname=mydb&dbhost=sqlserver.example.com&dbport=1433&dbusername=sa&dbpassword=pass&dbtable=orders" -o orders.csv
```

## Performance Features

### Multi-threading
- **File operations**: Each file is processed in its own thread
- **Table-to-table**: Splits source table into 10 partitions using `BINARY_CHECKSUM` modulo
- **Parallel degree**: Automatically uses all available CPU cores

### Bulk Operations
- **Bulk Insert**: Uses `SqlBulkCopy` with 10,000 row batches
- **No timeout**: Handles large operations without timeout errors
- **Streaming**: Reads and writes data in streaming fashion to minimize memory usage

### Data Handling
- All database columns treated as strings (`NVARCHAR(MAX)`)
- NULL values converted to empty strings
- Tables automatically created if they don't exist
- Missing columns automatically added to existing tables

## File Handling

### Delimiter Detection
Automatically detects delimiters by counting occurrences in the first line:
- Comma (`,`) for CSV
- Tab (`\t`) for TSV
- Pipe (`|`) for PSV

### CSV Robustness
Uses RFC4180 standard for CSV parsing:
- Handles embedded line breaks within quoted fields
- Handles embedded commas within quoted fields
- Handles escaped quotes
- Trims whitespace

### Wildcard Support
Use standard Windows wildcards:
- `*.csv` - All CSV files
- `data*.txt` - All text files starting with "data"
- `report?.csv` - Single character wildcard

## Architecture

### Core Components

- **ConnectionInfo**: Parses and manages database connection strings
- **ArgumentParser**: Handles command-line arguments
- **DelimitedFileReader**: Reads and auto-detects delimited files using CsvHelper
- **FileWriter**: Writes delimited files with proper formatting
- **DatabaseReader**: Reads from SQL Server with partitioning support
- **DatabaseWriter**: Writes to SQL Server using bulk insert
- **CopyOrchestrator**: Coordinates multi-threaded copy operations

### Threading Strategy

**File to Database** (Multiple Files):
```
File1 -> Thread1 -> Table1
File2 -> Thread2 -> Table2
File3 -> Thread3 -> Table3
```

**Table to Table** (Single Large Table):
```
Source Table
├─ Partition 0 (CHECKSUM % 10 = 0) -> Thread1 -> Dest Table
├─ Partition 1 (CHECKSUM % 10 = 1) -> Thread2 -> Dest Table
├─ ...
└─ Partition 9 (CHECKSUM % 10 = 9) -> Thread10 -> Dest Table
```

**Database to Database** (All Tables):
```
Table1 -> Thread1 -> (10 partition threads) -> Dest Table1
Table2 -> Thread2 -> (10 partition threads) -> Dest Table2
Table3 -> Thread3 -> (10 partition threads) -> Dest Table3
```

## Limitations

- Targets .NET Core 3.1 (out of support, but widely compatible)
- SQL Server only (not MySQL, PostgreSQL, etc.)
- All data treated as strings
- Assumes files have headers
- Windows file paths only

## License

This project is provided as-is for educational and commercial use.

## Contributing

This is a standalone utility. Feel free to fork and modify for your needs.
