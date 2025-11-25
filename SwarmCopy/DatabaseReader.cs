using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace SwarmCopy
{
    public class DatabaseReader
    {
        public static IEnumerable<Dictionary<string, string>> ReadTable(ConnectionInfo connInfo, string tableName, int? partitionIndex = null, int? partitionCount = null)
        {
            using (var connection = new SqlConnection(connInfo.GetConnectionString()))
            {
                connection.Open();

                var whereClause = string.Empty;
                if (partitionIndex.HasValue && partitionCount.HasValue)
                {
                    whereClause = $" WHERE ABS(BINARY_CHECKSUM(*)) % {partitionCount.Value} = {partitionIndex.Value}";
                }

                var qualifiedTableName = connInfo.GetQualifiedTableName(tableName);
                var query = $"SELECT * FROM {qualifiedTableName}{whereClause}";

                using (var command = new SqlCommand(query, connection))
                {
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
            using (var connection = new SqlConnection(connInfo.GetConnectionString()))
            {
                connection.Open();

                using (var command = new SqlCommand(sqlQuery, connection))
                {
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
            using (var connection = new SqlConnection(connInfo.GetConnectionString()))
            {
                connection.Open();

                // Execute query with TOP 0 to get column names without data
                var topQuery = sqlQuery.Trim();
                if (!topQuery.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                {
                    throw new ArgumentException("SQL query must start with SELECT");
                }

                // Wrap query to get just schema
                var schemaQuery = $"SELECT TOP 0 * FROM ({sqlQuery}) AS QuerySchema";

                using (var command = new SqlCommand(schemaQuery, connection))
                {
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
            using (var connection = new SqlConnection(connInfo.GetConnectionString()))
            {
                connection.Open();

                var query = @"
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = @TableSchema AND TABLE_NAME = @TableName
                    ORDER BY ORDINAL_POSITION";

                using (var command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@TableSchema", connInfo.DbSchema);
                    command.Parameters.AddWithValue("@TableName", tableName);

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

        public static string[] GetAllTables(ConnectionInfo connInfo)
        {
            using (var connection = new SqlConnection(connInfo.GetConnectionString()))
            {
                connection.Open();

                var query = @"
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = @TableSchema
                    ORDER BY TABLE_NAME";

                using (var command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@TableSchema", connInfo.DbSchema);

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
            using (var connection = new SqlConnection(connInfo.GetConnectionString()))
            {
                connection.Open();

                var query = @"
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = @TableSchema AND TABLE_NAME = @TableName";

                using (var command = new SqlCommand(query, connection))
                {
                    command.Parameters.AddWithValue("@TableSchema", connInfo.DbSchema);
                    command.Parameters.AddWithValue("@TableName", tableName);
                    var count = (int)command.ExecuteScalar();
                    return count > 0;
                }
            }
        }
    }
}
