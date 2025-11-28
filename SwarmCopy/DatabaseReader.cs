using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using DuckDB.NET.Data;

namespace SwarmCopy
{
    public class DatabaseReader
    {
        private static IDbConnection CreateConnection(ConnectionInfo connInfo)
        {
            if (connInfo.IsDuckDb)
            {
                return new DuckDBConnection(connInfo.GetConnectionString());
            }
            else
            {
                return new SqlConnection(connInfo.GetConnectionString());
            }
        }
        public static IEnumerable<Dictionary<string, string>> ReadTable(ConnectionInfo connInfo, string tableName, int? partitionIndex = null, int? partitionCount = null)
        {
            using (var connection = CreateConnection(connInfo))
            {
                connection.Open();

                var qualifiedTableName = connInfo.GetQualifiedTableName(tableName);
                string query;

                if (partitionIndex.HasValue && partitionCount.HasValue)
                {
                    if (connInfo.IsDuckDb)
                    {
                        // DuckDB: use row_number for partitioning
                        query = $"SELECT * FROM (SELECT *, row_number() OVER () as __rn FROM {qualifiedTableName}) WHERE (__rn - 1) % {partitionCount.Value} = {partitionIndex.Value}";
                    }
                    else
                    {
                        // SQL Server: use BINARY_CHECKSUM
                        var whereClause = $" WHERE ABS(BINARY_CHECKSUM(*)) % {partitionCount.Value} = {partitionIndex.Value}";
                        query = $"SELECT * FROM {qualifiedTableName}{whereClause}";
                    }
                }
                else
                {
                    query = $"SELECT * FROM {qualifiedTableName}";
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = query;
                    command.CommandTimeout = 0; // No timeout for large operations

                    using (var reader = command.ExecuteReader())
                    {
                        var fieldCount = reader.FieldCount;
                        var fieldNames = new string[fieldCount];

                        for (int i = 0; i < fieldCount; i++)
                        {
                            fieldNames[i] = reader.GetName(i);
                        }

                        while (reader.Read())
                        {
                            var row = new Dictionary<string, string>();
                            for (int i = 0; i < fieldCount; i++)
                            {
                                var value = reader.IsDBNull(i) ? string.Empty : reader.GetValue(i).ToString();
                                // Replace "NULL" string with empty string
                                if (value == "NULL")
                                {
                                    value = string.Empty;
                                }
                                row[fieldNames[i]] = value;
                            }
                            yield return row;
                        }
                    }
                }
            }
        }

        public static IEnumerable<Dictionary<string, string>> ReadFromSql(ConnectionInfo connInfo, string sqlQuery)
        {
            using (var connection = CreateConnection(connInfo))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlQuery;
                    command.CommandTimeout = 0; // No timeout for large operations

                    using (var reader = command.ExecuteReader())
                    {
                        var fieldCount = reader.FieldCount;
                        var fieldNames = new string[fieldCount];

                        for (int i = 0; i < fieldCount; i++)
                        {
                            fieldNames[i] = reader.GetName(i);
                        }

                        while (reader.Read())
                        {
                            var row = new Dictionary<string, string>();
                            for (int i = 0; i < fieldCount; i++)
                            {
                                var value = reader.IsDBNull(i) ? string.Empty : reader.GetValue(i).ToString();
                                // Replace "NULL" string with empty string
                                if (value == "NULL")
                                {
                                    value = string.Empty;
                                }
                                row[fieldNames[i]] = value;
                            }
                            yield return row;
                        }
                    }
                }
            }
        }

        public static string[] GetColumnsFromSql(ConnectionInfo connInfo, string sqlQuery)
        {
            using (var connection = CreateConnection(connInfo))
            {
                connection.Open();

                // Execute query with TOP 0 or LIMIT 0 to get column names without data
                var topQuery = sqlQuery.Trim();
                if (!topQuery.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                {
                    throw new ArgumentException("SQL query must start with SELECT");
                }

                // Wrap query to get just schema
                string schemaQuery;
                if (connInfo.IsDuckDb)
                {
                    schemaQuery = $"SELECT * FROM ({sqlQuery}) AS QuerySchema LIMIT 0";
                }
                else
                {
                    schemaQuery = $"SELECT TOP 0 * FROM ({sqlQuery}) AS QuerySchema";
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = schemaQuery;
                    command.CommandTimeout = 30;

                    using (var reader = command.ExecuteReader())
                    {
                        var columns = new List<string>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            columns.Add(reader.GetName(i));
                        }
                        return columns.ToArray();
                    }
                }
            }
        }

        public static string[] GetTableColumns(ConnectionInfo connInfo, string tableName)
        {
            using (var connection = CreateConnection(connInfo))
            {
                connection.Open();

                string query;
                if (connInfo.IsDuckDb)
                {
                    // DuckDB: use PRAGMA or simple SELECT with LIMIT 0
                    var qualifiedTableName = connInfo.GetQualifiedTableName(tableName);
                    query = $"SELECT * FROM {qualifiedTableName} LIMIT 0";

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = query;

                        using (var reader = command.ExecuteReader())
                        {
                            var columns = new List<string>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                columns.Add(reader.GetName(i));
                            }
                            return columns.ToArray();
                        }
                    }
                }
                else
                {
                    // SQL Server: use INFORMATION_SCHEMA
                    query = @"
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = @TableSchema AND TABLE_NAME = @TableName
                        ORDER BY ORDINAL_POSITION";

                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = query;
                        var sqlCommand = command as SqlCommand;
                        sqlCommand.Parameters.AddWithValue("@TableSchema", connInfo.DbSchema);
                        sqlCommand.Parameters.AddWithValue("@TableName", tableName);

                        var columns = new List<string>();
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                columns.Add(reader.GetString(0));
                            }
                        }

                        return columns.ToArray();
                    }
                }
            }
        }

        public static string[] GetAllTables(ConnectionInfo connInfo)
        {
            using (var connection = CreateConnection(connInfo))
            {
                connection.Open();

                string query;
                if (connInfo.IsDuckDb)
                {
                    // DuckDB: use information_schema.tables (it supports this)
                    query = @"
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_type = 'BASE TABLE' AND table_schema = ?
                        ORDER BY table_name";
                }
                else
                {
                    // SQL Server: use INFORMATION_SCHEMA
                    query = @"
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = @TableSchema
                        ORDER BY TABLE_NAME";
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = query;

                    if (connInfo.IsDuckDb)
                    {
                        var param = command.CreateParameter();
                        param.Value = connInfo.DbSchema;
                        command.Parameters.Add(param);
                    }
                    else
                    {
                        var sqlCommand = command as SqlCommand;
                        sqlCommand.Parameters.AddWithValue("@TableSchema", connInfo.DbSchema);
                    }

                    var tables = new List<string>();
                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            tables.Add(reader.GetString(0));
                        }
                    }

                    return tables.ToArray();
                }
            }
        }

        public static bool TableExists(ConnectionInfo connInfo, string tableName)
        {
            using (var connection = CreateConnection(connInfo))
            {
                connection.Open();

                string query;
                if (connInfo.IsDuckDb)
                {
                    query = @"
                        SELECT COUNT(*)
                        FROM information_schema.tables
                        WHERE table_schema = ? AND table_name = ?";
                }
                else
                {
                    query = @"
                        SELECT COUNT(*)
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = @TableSchema AND TABLE_NAME = @TableName";
                }

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = query;

                    if (connInfo.IsDuckDb)
                    {
                        var param1 = command.CreateParameter();
                        param1.Value = connInfo.DbSchema;
                        command.Parameters.Add(param1);

                        var param2 = command.CreateParameter();
                        param2.Value = tableName;
                        command.Parameters.Add(param2);
                    }
                    else
                    {
                        var sqlCommand = command as SqlCommand;
                        sqlCommand.Parameters.AddWithValue("@TableSchema", connInfo.DbSchema);
                        sqlCommand.Parameters.AddWithValue("@TableName", tableName);
                    }

                    var count = Convert.ToInt64(command.ExecuteScalar());
                    return count > 0;
                }
            }
        }
    }
}
