using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Nest;

namespace ElasticsearchExamples
{
    internal class Program
    {
        const string IndexName = "stock-demo-v1";
        const string AliasName = "stock-demo";

        public static IElasticClient Client = new ElasticClient(new ConnectionSettings().DefaultIndex(IndexName));

        private static async Task Main(string[] args)
        {
            var existsResponse = await Client.Indices.ExistsAsync(IndexName);

            if (!existsResponse.Exists)
            {
                var newIndexResponse = await Client.Indices.CreateAsync(IndexName, i => i
                    .Map(m => m
                        .AutoMap<StockData>()
                        .Properties<StockData>(p => p.Keyword(k => k.Name(f => f.Symbol))))
                    .Settings(s => s.NumberOfShards(1).NumberOfReplicas(0)));
                if (!newIndexResponse.IsValid || newIndexResponse.Acknowledged is false) throw new Exception("Oh no!!");

                var bulkAll = Client.BulkAll(ReadStockData(), r => r
                    .Index(IndexName)
                    .BackOffRetries(2)
                    .BackOffTime("30s")
                    .MaxDegreeOfParallelism(4)
                    .Size(1000));

                bulkAll.Wait(TimeSpan.FromMinutes(10), r => Console.WriteLine("Data indexed"));

                var aliasResponse = await Client.Indices.PutAliasAsync(IndexName, AliasName);
                if (!aliasResponse.IsValid) throw new Exception("Oh no!!");
            }

            var countResponse = await Client.CountAsync<StockData>(c => c.Index(AliasName));

            if (countResponse.IsValid)
                Console.WriteLine($"The count of documents is {countResponse.Count}");

            // var searchResponse = await Client.SearchAsync<StockData>(s => s.Index(AliasName).MatchAll());

            var searchResponse = await Client.SearchAsync<StockData>(s => s.Index(AliasName)
                .Aggregations(a => a
                    .Terms("symbols", t => t
                        .Field(f => f.Symbol)
                        .Size(1000)))
                .Size(0));

            var request = new SearchRequest<StockData>
            {
                Aggregations = new TermsAggregation("symbols")
                {
                    Field = Infer.Field<StockData>(f => f.Symbol),
                    Size = 1000
                },
                Size = 0
            };

            if (!searchResponse.IsValid) throw new Exception("Oh no");

            var symbols = searchResponse.Aggregations.Terms("symbols")
                .Buckets.Select(s => s.Key).ToList();

            foreach(var symbol in symbols)
            {
                //Console.WriteLine(symbol);
            }

            var symbolResponse = await Client.SearchAsync<StockData>(s => s.Index(AliasName)
                .Query(q => q
                    .Bool(b => b
                        .Filter(f => f
                            .Term(t => t.Field(fld => fld.Symbol).Value("MSFT")))))
                .Size(20)
                .Sort(srt => srt.Descending(d => d.Date)));

            if (!symbolResponse.IsValid) throw new Exception("Oh no");

            foreach(var data in symbolResponse.Documents)
            {
                //Console.WriteLine($"{data.Date}   {data.High} {data.Low}");
            }

            var fullTextSearchResponse = await Client.SearchAsync<StockData>(s => s.Index(AliasName)
                .Query(q => q.Match(m => m.Field(f => f.Name).Query("microsoft")))
                .Size(20)
                .Sort(srt => srt.Descending(d => d.Date)));

            foreach (var data in fullTextSearchResponse.Documents)
            {
                //Console.WriteLine($"{data.Name} {data.Date}   {data.High} {data.Low}");
            }

            var aggExampleResponse = await Client.SearchAsync<StockData>(s => s
                .Index(AliasName)
                .Size(0)
                .Query(q => q
                    .Bool(b => b
                        .Filter(f => f
                            .Term(t => t.Field(fld => fld.Symbol).Value("MSFT")))))
                .Aggregations(a => a
                    .DateHistogram("by-month", dh => dh
                        .CalendarInterval(DateInterval.Month)
                        .Field(fld => fld.Date)
                        .Order(HistogramOrder.KeyDescending)
                            .Aggregations(agg => agg.Sum("trade-volumes", sum => sum.Field(fld => fld.Volume))))));

            var monthly = aggExampleResponse.Aggregations.DateHistogram("by-month").Buckets;

            foreach(var bucket in monthly)
            {
                var volume = bucket.Sum("trade-volumes").Value;
                //Console.WriteLine($"{bucket.Date} : {volume}");
            }

            var scrollAllObservable = Client.ScrollAll<StockData>("10s", Environment.ProcessorCount, scroll => scroll
                .Search(s => s.Index(AliasName).MatchAll())
                .MaxDegreeOfParallelism(Environment.ProcessorCount));

            scrollAllObservable.Wait(TimeSpan.FromMinutes(5), s =>
            {
                foreach (var doc in s.SearchResponse.Documents)
                {
                    Console.WriteLine(doc.Symbol);
                }
            });
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