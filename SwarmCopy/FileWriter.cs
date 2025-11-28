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
        public static long WriteFile(string filePath, IEnumerable<Dictionary<string, string>> rows, string[] headers, char delimiter = ',', Action<long> progressCallback = null, int progressInterval = 10000)
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = delimiter.ToString(),
                HasHeaderRecord = true
            };

            long rowCount = 0;

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
                    rowCount++;

                    // Call progress callback every N rows
                    if (progressCallback != null && rowCount % progressInterval == 0)
                    {
                        progressCallback(rowCount);
                    }
                }
            }

            // Final callback with total count
            if (progressCallback != null && rowCount % progressInterval != 0)
            {
                progressCallback(rowCount);
            }

            return rowCount;
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
