# SwarmCopy Integration Tests

This project contains 21 comprehensive integration tests covering all major SwarmCopy scenarios.

## Test Scenarios

1. **Test01_SingleCsvToSqlServer** - Single CSV file to SQL Server table
2. **Test02_MultipleCsvToSqlServerTables** - Multiple CSV files to SQL Server (each file = one table)
3. **Test03_SingleCsvToDuckDB** - Single CSV file to DuckDB table
4. **Test04_MultipleCsvToDuckDBTables** - Multiple CSV files to DuckDB (each file = one table)
5. **Test05_SqlServerTableToFile** - SQL Server table to CSV file
6. **Test06_DuckDBTableToFile** - DuckDB table to CSV file
7. **Test07_SqlServerAllTablesToDuckDB** - SQL Server all tables (*) to DuckDB
8. **Test08_SqlServerAllTablesToFiles** - SQL Server all tables (*) to CSV files
9. **Test09_DuckDBAllTablesToSqlServer** - DuckDB all tables (*) to SQL Server
10. **Test10_DuckDBAllTablesToFiles** - DuckDB all tables (*) to CSV files
11. **Test11_SqlServerQueryToFile** - SQL Server SQL query to CSV file
12. **Test12_DuckDBQueryToFile** - DuckDB SQL query to CSV file
13. **Test13_SqlServerQueryToDuckDB** - SQL Server SQL query to DuckDB table
14. **Test14_MultipleCsvToSqlServerSingleTable** - Multiple CSV files concatenated to single SQL Server table
15. **Test15_MultipleCsvToDuckDBSingleTable** - Multiple CSV files concatenated to single DuckDB table
16. **Test16_DuckDBOverwriteMode** - DuckDB overwrite mode (replaces existing data)
17. **Test17_DuckDBAppendMode** - DuckDB append mode (adds to existing data)
18. **Test18_DuckDBCreateMode** - DuckDB create mode (fails if table exists)
19. **Test19_SqlServerOverwriteMode** - SQL Server overwrite mode (replaces existing data)
20. **Test20_SqlServerAppendMode** - SQL Server append mode (adds to existing data)
21. **Test21_SqlServerCreateMode** - SQL Server create mode (fails if table exists)

## Running the Tests

### All Tests
```bash
dotnet test
```

### Specific Test
```bash
dotnet test --filter "Test03_SingleCsvToDuckDB"
```

### DuckDB Tests Only (no SQL Server required)
```bash
dotnet test --filter "DuckDB"
```

## SQL Server Setup

For SQL Server tests to run, you need:

1. SQL Server installed and running locally
2. Update the connection string in `SwarmCopyIntegrationTests.cs`:
```csharp
_sqlServerConnString = "db:dbname=SwarmCopyTest&dbhost=localhost&dbusername=sa&dbpassword=YourPassword123&dbschema=dbo";
```

If SQL Server is not available, those tests will be skipped automatically.

## Test Data

Test data files are in the `TestData` directory:
- test1.csv - 3 rows of sample data
- test2.csv - 3 rows of sample data
- products.csv - 3 product records
- orders.csv - 3 order records

## Clean Up

Tests automatically clean up:
- Temporary files in the output directory
- Test database files created during execution

SQL Server test tables may remain in the database and need manual cleanup if desired.
