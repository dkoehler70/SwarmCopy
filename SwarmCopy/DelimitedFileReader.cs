using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using CsvHelper;
using CsvHelper.Configuration;

namespace SwarmCopy
{
    public class DelimitedFileReader
    {
        public static char DetectDelimiter(string filePath)
        {
            using (var reader = new StreamReader(filePath))
            {
                var firstLine = reader.ReadLine();
                if (string.IsNullOrEmpty(firstLine))
                    return ',';

                // Count occurrences of each delimiter
                var commaCount = firstLine.Count(c => c == ',');
                var tabCount = firstLine.Count(c => c == '\t');
                var pipeCount = firstLine.Count(c => c == '|');

                // Return the most common delimiter
                if (tabCount > commaCount && tabCount > pipeCount)
                    return '\t';
                if (pipeCount > commaCount && pipeCount > tabCount)
                    return '|';
                return ',';
            }
        }

        public static IEnumerable<Dictionary<string, string>> ReadFile(string filePath, char? delimiter = null)
        {
            if (!delimiter.HasValue)
            {
                delimiter = DetectDelimiter(filePath);
            }

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = delimiter.Value.ToString(),
                HasHeaderRecord = true,
                BadDataFound = null, // Ignore bad data
                MissingFieldFound = null, // Ignore missing fields
                TrimOptions = TrimOptions.Trim,
                Mode = CsvMode.RFC4180 // Handles quotes and embedded delimiters properly
            };

            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, config))
            {
                csv.Read();
                csv.ReadHeader();
                var headers = csv.HeaderRecord;

                while (csv.Read())
                {
                    var row = new Dictionary<string, string>();
                    foreach (var header in headers)
                    {
                        var value = csv.GetField(header);
                        row[header] = value ?? string.Empty;
                    }
                    yield return row;
                }
            }
        }

        public static string[] GetHeaders(string filePath, char? delimiter = null)
        {
            if (!delimiter.HasValue)
            {
                delimiter = DetectDelimiter(filePath);
            }

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = delimiter.Value.ToString(),
                HasHeaderRecord = true
            };

            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, config))
            {
                csv.Read();
                csv.ReadHeader();
                return csv.HeaderRecord;
            }
        }

        public static Dictionary<string, int> SampleColumnSizes(string filePath, int sampleSize = 100, int buffer = 10, char? delimiter = null)
        {
            if (!delimiter.HasValue)
            {
                delimiter = DetectDelimiter(filePath);
            }

            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = delimiter.Value.ToString(),
                HasHeaderRecord = true,
                BadDataFound = null,
                MissingFieldFound = null,
                TrimOptions = TrimOptions.Trim,
                Mode = CsvMode.RFC4180
            };

            var maxSizes = new Dictionary<string, int>();

            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, config))
            {
                csv.Read();
                csv.ReadHeader();
                var headers = csv.HeaderRecord;

                // Initialize max sizes
                foreach (var header in headers)
                {
                    maxSizes[header] = 0;
                }

                // Sample first K rows
                int rowCount = 0;
                while (csv.Read() && rowCount < sampleSize)
                {
                    foreach (var header in headers)
                    {
                        var value = csv.GetField(header) ?? string.Empty;
                        maxSizes[header] = Math.Max(maxSizes[header], value.Length);
                    }
                    rowCount++;
                }
            }

            // Add buffer to each column size and ensure minimum of 1
            // Switch to NVARCHAR(MAX) for columns >= 512 chars
            var result = new Dictionary<string, int>();
            foreach (var kvp in maxSizes)
            {
                var sizeWithBuffer = Math.Max(1, kvp.Value + buffer);

                // If size >= 512, use int.MaxValue to represent NVARCHAR(MAX)
                if (sizeWithBuffer >= 512)
                {
                    result[kvp.Key] = int.MaxValue;
                }
                else
                {
                    result[kvp.Key] = sizeWithBuffer;
                }
            }

            return result;
        }
    }
}
