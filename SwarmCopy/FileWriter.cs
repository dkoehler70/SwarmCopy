using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using CsvHelper;
using CsvHelper.Configuration;

namespace SwarmCopy
{
    public class FileWriter
    {
        public static void WriteFile(string filePath, IEnumerable<Dictionary<string, string>> rows, string[] headers, char delimiter = ',')
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = delimiter.ToString(),
                HasHeaderRecord = true
            };

            using (var writer = new StreamWriter(filePath))
            using (var csv = new CsvWriter(writer, config))
            {
                // Write headers
                foreach (var header in headers)
                {
                    csv.WriteField(header);
                }
                csv.NextRecord();

                // Write rows
                foreach (var row in rows)
                {
                    foreach (var header in headers)
                    {
                        csv.WriteField(row.ContainsKey(header) ? row[header] : string.Empty);
                    }
                    csv.NextRecord();
                }
            }
        }

        public static char GetDelimiterFromExtension(string filePath)
        {
            var extension = Path.GetExtension(filePath).ToLower();
            switch (extension)
            {
                case ".tsv":
                case ".tab":
                    return '\t';
                case ".psv":
                    return '|';
                case ".csv":
                default:
                    return ',';
            }
        }
    }
}
