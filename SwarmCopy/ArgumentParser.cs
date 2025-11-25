using System;
using System.Collections.Generic;

namespace SwarmCopy
{
    public class CopyArguments
    {
        public string Input { get; set; }
        public string Output { get; set; }
        public bool IsInputDatabase => Input != null && Input.StartsWith("db:", StringComparison.OrdinalIgnoreCase);
        public bool IsOutputDatabase => Output != null && Output.StartsWith("db:", StringComparison.OrdinalIgnoreCase);
    }

    public class ArgumentParser
    {
        public static CopyArguments Parse(string[] args)
        {
            var result = new CopyArguments();

            for (int i = 0; i < args.Length; i++)
            {
                if (args[i] == "-i" && i + 1 < args.Length)
                {
                    result.Input = args[++i];
                }
                else if (args[i] == "-o" && i + 1 < args.Length)
                {
                    result.Output = args[++i];
                }
            }

            return result;
        }

        public static void ShowHelp()
        {
            Console.WriteLine("SwarmCopy - High-Performance Data Copy Utility");
            Console.WriteLine();
            Console.WriteLine("Usage: SwarmCopy -i <input> -o <output>");
            Console.WriteLine();
            Console.WriteLine("Arguments:");
            Console.WriteLine("  -i <input>    Input source (file path, directory, or database connection)");
            Console.WriteLine("  -o <output>   Output destination (file path, directory, or database connection)");
            Console.WriteLine();
            Console.WriteLine("File Format:");
            Console.WriteLine("  - Use Windows file paths (supports wildcards like *.csv)");
            Console.WriteLine("  - Auto-detects delimiters (comma, tab, pipe)");
            Console.WriteLine("  - Assumes files have headers");
            Console.WriteLine();
            Console.WriteLine("Database Format:");
            Console.WriteLine("  db:dbname=<name>&dbhost=<host>&dbport=<port>&dbusername=<user>&dbpassword=<pass>&dbtable=<table>");
            Console.WriteLine();
            Console.WriteLine("  Parameters:");
            Console.WriteLine("    dbname      - Database name (required)");
            Console.WriteLine("    dbhost      - Host name (default: localhost)");
            Console.WriteLine("    dbport      - Port number (default: 1433)");
            Console.WriteLine("    dbusername  - Username (omit for Windows authentication)");
            Console.WriteLine("    dbpassword  - Password (omit for Windows authentication)");
            Console.WriteLine("    dbtable     - Table name (use * for all tables)");
            Console.WriteLine("    dbaction    - Action to perform");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("  File to Database:");
            Console.WriteLine("    SwarmCopy -i data.csv -o \"db:dbname=mydb&dbtable=mytable\"");
            Console.WriteLine();
            Console.WriteLine("  Database to File:");
            Console.WriteLine("    SwarmCopy -i \"db:dbname=mydb&dbtable=mytable\" -o output.csv");
            Console.WriteLine();
            Console.WriteLine("  Multiple Files to Database:");
            Console.WriteLine("    SwarmCopy -i \"*.csv\" -o \"db:dbname=mydb\"");
            Console.WriteLine();
            Console.WriteLine("  All Tables Database to Database:");
            Console.WriteLine("    SwarmCopy -i \"db:dbname=sourcedb&dbtable=*\" -o \"db:dbname=destdb&dbhost=server2\"");
            Console.WriteLine();
            Console.WriteLine("  Table to Table (multi-threaded):");
            Console.WriteLine("    SwarmCopy -i \"db:dbname=db1&dbtable=table1\" -o \"db:dbname=db2&dbtable=table2\"");
        }
    }
}
