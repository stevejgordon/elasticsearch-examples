using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Nest;

namespace ElasticsearchExamples
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            const string indexName = "stock-demo-v1";
            const string aliasName = "stock-demo";

            var client = new ElasticClient();

            var existsResponse = await client.Indices.ExistsAsync(indexName);

            if (!existsResponse.Exists)
            {
                var newIndexResponse = await client.Indices.CreateAsync(indexName, i =>
                    i.Map(m => m
                        .AutoMap<StockData>()
                        .Properties<StockData>(p => p.Keyword(k => k.Name(f => f.Symbol)))));

                var bulkAll = client.BulkAll(ReadStockData(), r => r
                    .Index(indexName)
                    .BackOffRetries(2)
                    .BackOffTime("30s")
                    .MaxDegreeOfParallelism(4)
                    .Size(1000));

                bulkAll.Wait(TimeSpan.FromMinutes(10), r => Console.WriteLine("Data indexed"));

                var aliasResponse = await client.Indices.PutAliasAsync(indexName, aliasName);
            }
        }

        public static IEnumerable<StockData> ReadStockData()
        {
            // Update this to the correct path of the CSV file
            var file = new StreamReader("c:\\stock-data\\all_stocks_5yr.csv");

            string line;
            while ((line = file.ReadLine()) is not null) yield return new StockData(line);
        }
    }

    public class StockData
    {
        private static readonly Dictionary<string, string> CompanyLookup = new()
        {
            {"AAL", "American Airlines Group Inc"},
            {"MSFT", "Microsoft Corporation"},
            {"AME", "AMETEK, Inc."},
            {"M", "Macy's Inc"}
        };

        public StockData(string dataLine)
        {
            var columns = dataLine.Split(',', StringSplitOptions.TrimEntries);

            if (DateTime.TryParse(columns[0], out var date))
                Date = date;

            if (double.TryParse(columns[1], out var open))
                Open = open;

            if (double.TryParse(columns[2], out var high))
                High = high;

            if (double.TryParse(columns[3], out var low))
                Low = low;

            if (double.TryParse(columns[4], out var close))
                Close = close;

            if (uint.TryParse(columns[5], out var volume))
                Volume = volume;

            Symbol = columns[6];

            if (CompanyLookup.TryGetValue(Symbol, out var name))
                Name = name;
        }

        public DateTime Date { get; init; }
        public double Open { get; init; }
        public double Close { get; init; }
        public double High { get; init; }
        public double Low { get; init; }
        public uint Volume { get; init; }
        public string Symbol { get; init; }
        public string Name { get; init; }
    }
}