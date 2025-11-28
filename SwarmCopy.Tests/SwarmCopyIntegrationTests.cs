using System;
using System.IO;
using System.Linq;
using Xunit;

namespace SwarmCopy.Tests
{
    public class SwarmCopyIntegrationTests : IDisposable
    {
        private readonly string _testDataDir;
        private readonly string _outputDir;
        private readonly string _sqlServerConnString;
        private readonly bool _sqlServerAvailable;

        public SwarmCopyIntegrationTests()
        {
            _testDataDir = Path.Combine(Directory.GetCurrentDirectory(), "TestData");
            _outputDir = Path.Combine(Path.GetTempPath(), $"SwarmCopyTests_{Guid.NewGuid()}");
            Directory.CreateDirectory(_outputDir);

            // SQL Server connection - update with your test server details
            _sqlServerConnString = "db:dbname=SwarmCopyTest&dbhost=localhost&dbusername=sa&dbpassword=YourPassword123&dbschema=dbo";

            // Check if SQL Server is available
            _sqlServerAvailable = CheckSqlServerAvailable();
        }

        public void Dispose()
        {
            // Clean up output directory
            if (Directory.Exists(_outputDir))
            {
                Directory.Delete(_outputDir, true);
            }
        }

        private bool CheckSqlServerAvailable()
        {
            try
            {
                var connInfo = ConnectionInfo.Parse(_sqlServerConnString);
                using (var conn = new System.Data.SqlClient.SqlConnection(connInfo.GetConnectionString()))
                {
                    conn.Open();
                    return true;
                }
            }
            catch
            {
                return false;
            }
        }

        private void RunSwarmCopy(string input, string output)
        {
            var args = new CopyArguments
            {
                Input = input,
                Output = output
            };
            CopyOrchestrator.ExecuteCopy(args);
        }

        // Test 1: Single CSV file to SQL Server
        [Fact]
        public void Test01_SingleCsvToSqlServer()
        {
            if (!_sqlServerAvailable)
            {
                Assert.True(true, "SQL Server not available - test skipped");
                return;
            }

            var csvFile = Path.Combine(_testDataDir, "test1.csv");
            var output = $"{_sqlServerConnString}&dbtable=test1_import";

            RunSwarmCopy(csvFile, output);

            // Verify table exists and has correct row count
            var connInfo = ConnectionInfo.Parse(output);
            Assert.True(DatabaseReader.TableExists(connInfo, "test1_import"));
            var rows = DatabaseReader.ReadTable(connInfo, "test1_import").ToList();
            Assert.Equal(3, rows.Count);
        }

        // Test 2: Multiple CSV files to SQL Server tables
        [Fact]
        public void Test02_MultipleCsvToSqlServerTables()
        {
            if (!_sqlServerAvailable)
            {
                Assert.True(true, "SQL Server not available - test skipped");
                return;
            }

            var csvPattern = Path.Combine(_testDataDir, "*.csv");
            var output = _sqlServerConnString;

            RunSwarmCopy(csvPattern, output);

            // Verify tables exist
            var connInfo = ConnectionInfo.Parse(output);
            Assert.True(DatabaseReader.TableExists(connInfo, "test1"));
            Assert.True(DatabaseReader.TableExists(connInfo, "test2"));
            Assert.True(DatabaseReader.TableExists(connInfo, "products"));
            Assert.True(DatabaseReader.TableExists(connInfo, "orders"));
        }

        // Test 3: Single CSV file to DuckDB
        [Fact]
        public void Test03_SingleCsvToDuckDB()
        {
            var csvFile = Path.Combine(_testDataDir, "test1.csv");
            var duckDbFile = Path.Combine(_outputDir, "test3.db");
            var output = $"duck:dbname={duckDbFile}&dbschema=main&dbtable=test1_import";

            RunSwarmCopy(csvFile, output);

            // Verify table exists and has correct row count
            var connInfo = ConnectionInfo.Parse(output);
            Assert.True(DatabaseReader.TableExists(connInfo, "test1_import"));
            var rows = DatabaseReader.ReadTable(connInfo, "test1_import").ToList();
            Assert.Equal(3, rows.Count);
        }

        // Test 4: Multiple CSV files to DuckDB tables
        [Fact]
        public void Test04_MultipleCsvToDuckDBTables()
        {
            var csvPattern = Path.Combine(_testDataDir, "*.csv");
            var duckDbFile = Path.Combine(_outputDir, "test4.db");
            var output = $"duck:dbname={duckDbFile}&dbschema=main";

            RunSwarmCopy(csvPattern, output);

            // Verify tables exist
            var connInfo = ConnectionInfo.Parse(output);
            Assert.True(DatabaseReader.TableExists(connInfo, "test1"));
            Assert.True(DatabaseReader.TableExists(connInfo, "test2"));
            Assert.True(DatabaseReader.TableExists(connInfo, "products"));
            Assert.True(DatabaseReader.TableExists(connInfo, "orders"));
        }

        // Test 5: SQL Server table to file
        [Fact]
        public void Test05_SqlServerTableToFile()
        {
            if (!_sqlServerAvailable)
            {
                Assert.True(true, "SQL Server not available - test skipped");
                return;
            }

            // First, create a test table
            var setupConn = $"{_sqlServerConnString}&dbtable=test5_source";
            var csvFile = Path.Combine(_testDataDir, "test1.csv");
            RunSwarmCopy(csvFile, setupConn);

            // Now export it
            var outputFile = Path.Combine(_outputDir, "test5_output.csv");
            RunSwarmCopy(setupConn, outputFile);

            // Verify file exists and has data
            Assert.True(File.Exists(outputFile));
            var lines = File.ReadAllLines(outputFile);
            Assert.Equal(4, lines.Length); // header + 3 data rows
        }

        // Test 6: DuckDB table to file
        [Fact]
        public void Test06_DuckDBTableToFile()
        {
            // First, create a test table in DuckDB
            var duckDbFile = Path.Combine(_outputDir, "test6.db");
            var setupConn = $"duck:dbname={duckDbFile}&dbschema=main&dbtable=test6_source";
            var csvFile = Path.Combine(_testDataDir, "test1.csv");
            RunSwarmCopy(csvFile, setupConn);

            // Now export it
            var outputFile = Path.Combine(_outputDir, "test6_output.csv");
            RunSwarmCopy(setupConn, outputFile);

            // Verify file exists and has data
            Assert.True(File.Exists(outputFile));
            var lines = File.ReadAllLines(outputFile);
            Assert.Equal(4, lines.Length); // header + 3 data rows
        }

        // Test 7: SQL Server table=* to DuckDB
        [Fact]
        public void Test07_SqlServerAllTablesToDuckDB()
        {
            if (!_sqlServerAvailable)
            {
                Assert.True(true, "SQL Server not available - test skipped");
                return;
            }

            // Setup: Create multiple tables in SQL Server
            var csvPattern = Path.Combine(_testDataDir, "test*.csv");
            var setupConn = _sqlServerConnString;
            RunSwarmCopy(csvPattern, setupConn);

            // Export all tables to DuckDB
            var sourceConn = $"{_sqlServerConnString}&dbtable=*";
            var duckDbFile = Path.Combine(_outputDir, "test7.db");
            var destConn = $"duck:dbname={duckDbFile}&dbschema=main";

            RunSwarmCopy(sourceConn, destConn);

            // Verify tables in DuckDB
            var connInfo = ConnectionInfo.Parse(destConn);
            Assert.True(DatabaseReader.TableExists(connInfo, "test1"));
            Assert.True(DatabaseReader.TableExists(connInfo, "test2"));
        }

        // Test 8: SQL Server table=* to files
        [Fact]
        public void Test08_SqlServerAllTablesToFiles()
        {
            if (!_sqlServerAvailable)
            {
                Assert.True(true, "SQL Server not available - test skipped");
                return;
            }

            // Setup: Create multiple tables in SQL Server
            var csvPattern = Path.Combine(_testDataDir, "test*.csv");
            var setupConn = _sqlServerConnString;
            RunSwarmCopy(csvPattern, setupConn);

            // Export all tables to files
            var sourceConn = $"{_sqlServerConnString}&dbtable=*";
            var outputSubDir = Path.Combine(_outputDir, "test8_files");

            RunSwarmCopy(sourceConn, outputSubDir);

            // Verify files exist
            Assert.True(File.Exists(Path.Combine(outputSubDir, "test1.csv")));
            Assert.True(File.Exists(Path.Combine(outputSubDir, "test2.csv")));
        }

        // Test 9: DuckDB table=* to SQL Server
        [Fact]
        public void Test09_DuckDBAllTablesToSqlServer()
        {
            if (!_sqlServerAvailable)
            {
                Assert.True(true, "SQL Server not available - test skipped");
                return;
            }

            // Setup: Create multiple tables in DuckDB
            var duckDbFile = Path.Combine(_outputDir, "test9.db");
            var csvPattern = Path.Combine(_testDataDir, "test*.csv");
            var setupConn = $"duck:dbname={duckDbFile}&dbschema=main";
            RunSwarmCopy(csvPattern, setupConn);

            // Export all tables to SQL Server
            var sourceConn = $"duck:dbname={duckDbFile}&dbschema=main&dbtable=*";
            var destConn = _sqlServerConnString;

            RunSwarmCopy(sourceConn, destConn);

            // Verify tables in SQL Server
            var connInfo = ConnectionInfo.Parse(destConn);
            Assert.True(DatabaseReader.TableExists(connInfo, "test1"));
            Assert.True(DatabaseReader.TableExists(connInfo, "test2"));
        }

        // Test 10: DuckDB table=* to files
        [Fact]
        public void Test10_DuckDBAllTablesToFiles()
        {
            // Setup: Create multiple tables in DuckDB
            var duckDbFile = Path.Combine(_outputDir, "test10.db");
            var csvPattern = Path.Combine(_testDataDir, "test*.csv");
            var setupConn = $"duck:dbname={duckDbFile}&dbschema=main";
            RunSwarmCopy(csvPattern, setupConn);

            // Export all tables to files
            var sourceConn = $"duck:dbname={duckDbFile}&dbschema=main&dbtable=*";
            var outputSubDir = Path.Combine(_outputDir, "test10_files");

            RunSwarmCopy(sourceConn, outputSubDir);

            // Verify files exist
            Assert.True(File.Exists(Path.Combine(outputSubDir, "test1.csv")));
            Assert.True(File.Exists(Path.Combine(outputSubDir, "test2.csv")));
        }

        // Test 11: SQL Server SQL query to file
        [Fact]
        public void Test11_SqlServerQueryToFile()
        {
            if (!_sqlServerAvailable)
            {
                Assert.True(true, "SQL Server not available - test skipped");
                return;
            }

            // Setup: Create a test table
            var setupConn = $"{_sqlServerConnString}&dbtable=test11_source";
            var csvFile = Path.Combine(_testDataDir, "test1.csv");
            RunSwarmCopy(csvFile, setupConn);

            // Export using SQL query
            var sourceConn = $"{_sqlServerConnString}&dbsql=SELECT * FROM test11_source WHERE id > 1";
            var outputFile = Path.Combine(_outputDir, "test11_output.csv");

            RunSwarmCopy(sourceConn, outputFile);

            // Verify file has filtered data (should have 2 rows, not 3)
            Assert.True(File.Exists(outputFile));
            var lines = File.ReadAllLines(outputFile);
            Assert.Equal(3, lines.Length); // header + 2 filtered rows
        }

        // Test 12: DuckDB SQL query to file
        [Fact]
        public void Test12_DuckDBQueryToFile()
        {
            // Setup: Create a test table in DuckDB
            var duckDbFile = Path.Combine(_outputDir, "test12.db");
            var setupConn = $"duck:dbname={duckDbFile}&dbschema=main&dbtable=test12_source";
            var csvFile = Path.Combine(_testDataDir, "test1.csv");
            RunSwarmCopy(csvFile, setupConn);

            // Export using SQL query (note: columns are VARCHAR, so use string comparison)
            var sourceConn = $"duck:dbname={duckDbFile}&dbschema=main&dbsql=SELECT * FROM test12_source WHERE id > '1'";
            var outputFile = Path.Combine(_outputDir, "test12_output.csv");

            RunSwarmCopy(sourceConn, outputFile);

            // Verify file has filtered data
            Assert.True(File.Exists(outputFile));
            var lines = File.ReadAllLines(outputFile);
            Assert.Equal(3, lines.Length); // header + 2 filtered rows
        }

        // Test 13: SQL Server SQL query to DuckDB
        [Fact]
        public void Test13_SqlServerQueryToDuckDB()
        {
            if (!_sqlServerAvailable)
            {
                Assert.True(true, "SQL Server not available - test skipped");
                return;
            }

            // Setup: Create a test table
            var setupConn = $"{_sqlServerConnString}&dbtable=test13_source";
            var csvFile = Path.Combine(_testDataDir, "test1.csv");
            RunSwarmCopy(csvFile, setupConn);

            // Export using SQL query to DuckDB
            var sourceConn = $"{_sqlServerConnString}&dbsql=SELECT * FROM test13_source WHERE id > 1";
            var duckDbFile = Path.Combine(_outputDir, "test13.db");
            var destConn = $"duck:dbname={duckDbFile}&dbschema=main&dbtable=test13_result";

            RunSwarmCopy(sourceConn, destConn);

            // Verify DuckDB table has filtered data
            var connInfo = ConnectionInfo.Parse(destConn);
            var rows = DatabaseReader.ReadTable(connInfo, "test13_result").ToList();
            Assert.Equal(2, rows.Count); // Filtered to 2 rows
        }

        // Test 14: Multiple CSV files to SQL Server single table
        [Fact]
        public void Test14_MultipleCsvToSqlServerSingleTable()
        {
            if (!_sqlServerAvailable)
            {
                Assert.True(true, "SQL Server not available - test skipped");
                return;
            }

            var csvPattern = Path.Combine(_testDataDir, "test*.csv");
            var output = $"{_sqlServerConnString}&dbtable=test14_combined";

            RunSwarmCopy(csvPattern, output);

            // Verify table exists and has combined data (3 + 3 = 6 rows)
            var connInfo = ConnectionInfo.Parse(output);
            Assert.True(DatabaseReader.TableExists(connInfo, "test14_combined"));
            var rows = DatabaseReader.ReadTable(connInfo, "test14_combined").ToList();
            Assert.Equal(6, rows.Count);
        }

        // Test 15: Multiple CSV files to DuckDB single table
        [Fact]
        public void Test15_MultipleCsvToDuckDBSingleTable()
        {
            var csvPattern = Path.Combine(_testDataDir, "test*.csv");
            var duckDbFile = Path.Combine(_outputDir, "test15.db");
            var output = $"duck:dbname={duckDbFile}&dbschema=main&dbtable=test15_combined";

            RunSwarmCopy(csvPattern, output);

            // Verify table exists and has combined data (3 + 3 = 6 rows)
            var connInfo = ConnectionInfo.Parse(output);
            Assert.True(DatabaseReader.TableExists(connInfo, "test15_combined"));
            var rows = DatabaseReader.ReadTable(connInfo, "test15_combined").ToList();
            Assert.Equal(6, rows.Count);
        }
    }
}
