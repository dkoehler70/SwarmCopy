using System;
using System.Collections.Generic;
using System.Linq;

namespace SwarmCopy
{
    public class ConnectionInfo
    {
        public string DbName { get; set; }
        public string DbHost { get; set; } = "localhost";
        public int DbPort { get; set; } = 1433;
        public string DbUsername { get; set; }
        public string DbPassword { get; set; }
        public string DbTable { get; set; }
        public string DbSchema { get; set; } = "dbo";
        public string DbAction { get; set; } = "overwrite";
        public string DbSql { get; set; }

        public bool UseWindowsAuth => string.IsNullOrEmpty(DbUsername) && string.IsNullOrEmpty(DbPassword);
        public bool IsOverwrite => string.IsNullOrEmpty(DbAction) || DbAction.Equals("overwrite", StringComparison.OrdinalIgnoreCase);
        public bool IsAppend => DbAction != null && DbAction.Equals("append", StringComparison.OrdinalIgnoreCase);

        public string GetQualifiedTableName(string tableName)
        {
            return $"[{DbSchema}].[{tableName}]";
        }

        public string GetConnectionString()
        {
            if (UseWindowsAuth)
            {
                return $"Server={DbHost},{DbPort};Database={DbName};Integrated Security=true;";
            }
            else
            {
                return $"Server={DbHost},{DbPort};Database={DbName};User Id={DbUsername};Password={DbPassword};";
            }
        }

        public static ConnectionInfo Parse(string connectionString)
        {
            if (!connectionString.StartsWith("db:", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("Database connection string must start with 'db:'");
            }

            var paramString = connectionString.Substring(3);
            var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var part in paramString.Split('&'))
            {
                var keyValue = part.Split(new[] { '=' }, 2);
                if (keyValue.Length == 2)
                {
                    parameters[keyValue[0].Trim()] = keyValue[1].Trim();
                }
            }

            var connInfo = new ConnectionInfo();

            if (parameters.TryGetValue("dbname", out var dbName))
                connInfo.DbName = dbName;

            if (parameters.TryGetValue("dbhost", out var dbHost))
                connInfo.DbHost = dbHost;

            if (parameters.TryGetValue("dbport", out var dbPort) && int.TryParse(dbPort, out var port))
                connInfo.DbPort = port;

            if (parameters.TryGetValue("dbusername", out var dbUsername))
                connInfo.DbUsername = dbUsername;

            if (parameters.TryGetValue("dbpassword", out var dbPassword))
                connInfo.DbPassword = dbPassword;

            if (parameters.TryGetValue("dbtable", out var dbTable))
                connInfo.DbTable = dbTable;

            if (parameters.TryGetValue("dbschema", out var dbSchema))
                connInfo.DbSchema = dbSchema;

            if (parameters.TryGetValue("dbaction", out var dbAction))
                connInfo.DbAction = dbAction;

            if (parameters.TryGetValue("dbsql", out var dbSql))
                connInfo.DbSql = dbSql;

            // Validation
            if (string.IsNullOrEmpty(connInfo.DbName))
            {
                throw new ArgumentException("dbname is required in database connection string");
            }

            // Cannot specify both dbtable and dbsql
            if (!string.IsNullOrEmpty(connInfo.DbTable) && !string.IsNullOrEmpty(connInfo.DbSql))
            {
                throw new ArgumentException("Cannot specify both dbtable and dbsql. Use one or the other.");
            }

            // Validate dbaction
            if (!string.IsNullOrEmpty(connInfo.DbAction) &&
                !connInfo.DbAction.Equals("overwrite", StringComparison.OrdinalIgnoreCase) &&
                !connInfo.DbAction.Equals("append", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException("dbaction must be either 'overwrite' or 'append'");
            }

            return connInfo;
        }
    }
}
