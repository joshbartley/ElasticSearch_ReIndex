using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSearchLiveReIndex
{
    class Program
    {
        static void Main(string[] args)
        {
            const string INDEX_V1 = "datav1";
            const string INDEX_V2 = "datav2";

            ElasticClient client = new ElasticClient(new Uri("http://127.0.0.1:9200"));

            client.DeleteIndex(Indices.All, i => i.Index(INDEX_V1));

            var request = new IndexExistsRequest(INDEX_V1);
            if (!client.IndexExists(request).Exists)
            { //make sure to use all lower case
                client.CreateIndex(new IndexName() { Name = INDEX_V1 },
                    x => x.Settings(y => y.NumberOfShards(1).NumberOfReplicas(0))
                    .Mappings(m=> m.Map<Company>(c=> c.Properties(prop=> prop.String(s=> s.Name(p=> p.Name).NotAnalyzed()))))); //for the test no need to add shards or replicas
            }

            request = new IndexExistsRequest(INDEX_V2);
            if (!client.IndexExists(request).Exists)
            { //make sure to use all lower case
                var response = client.CreateIndex(new IndexName() { Name = INDEX_V2 },
                    x => x.Settings(y => y.NumberOfShards(1).NumberOfReplicas(0).Analysis(analysis => analysis
                                .Tokenizers(tokenizers => tokenizers
                                    .Pattern("nuget-id-tokenizer",pt=> pt.Pattern(@"\W+"))
                                )
                                .TokenFilters(tokenfilters => tokenfilters
                                    .WordDelimiter("nuget-id-words", wdtf=> wdtf.SplitOnCaseChange(true).PreserveOriginal(true)
                                    .SplitOnNumerics(true).GenerateNumberParts(true).GenerateWordParts(true)
                                ))
                                .Analyzers(analyzers => analyzers
                                    .Custom("nuget-id-analyzer", ca1=> ca1.Tokenizer("nuget-id-tokenizer").Filters(new[] { "nuget-id-words", "lowercase" }
                                    ))
                                    .Custom("nuget-id-keyword", ca2=> ca2.Tokenizer("keyword").Filters(new[] { "lowercase" }))
                                )))
                    .Mappings(m => m.Map<Company>(c => c.Properties(prop => prop.String(s => s.Name(p => p.Name).Analyzer("nuget-id-keyword")) )))); //for the test no need to add shards or replicas
            }


            var aliasRequest = new PutAliasRequest(Indices.Index(new IndexName() { Name = INDEX_V1 }),   new Name("data_rw"));
            client.PutAlias(aliasRequest);

            aliasRequest = new PutAliasRequest(Indices.Index(new IndexName() { Name = INDEX_V1 }), new Name("data_r"));
            client.PutAlias(aliasRequest);


            WriteInitialObjects(client);


            aliasRequest = new PutAliasRequest(Indices.Index(new IndexName() { Name = INDEX_V1 })
                .And(new IndexName() { Name = INDEX_V2 }), new Name("data_rw"));

            client.PutAlias(aliasRequest);


            WriteSecondaryObjects(client);

            //var o = new ReindexObserver<object>((resp) => Console.WriteLine("Here"),
            //    e => Console.WriteLine("Error occured {0}", e.ToString()),
            //    () => Console.WriteLine("Done!"));//x=> x. :  reindex_Error, onNext: reindex_Next, completed: reindex_Completed);

            //var reindexRequest = new ReindexRequest(INDEX_V1, INDEX_V2, Types.All);
            //reindexRequest.CreateIndexRequest

            //var reindex = client.Reindex<object>(reindexRequest);            
            //reindex.Subscribe(o);
            Reindex(client, "data_r", INDEX_V1, INDEX_V2);

            Console.WriteLine("Wait for Done! message"); //it is a console app, i cheat
            Console.ReadLine();


        }

        private static void WriteInitialObjects(ElasticClient client)
        {
            var indexes = client.GetAlias(x => x.Name("data_rw"));

            foreach (var index in indexes.Indices)
            {
                client.Index(new Company() { Name = "Acme Corp" }, idx => idx.Index(index.Key));
                client.Index(new Company() { Name = "Corp Acme" }, idx => idx.Index(index.Key));
            }
        }

        private static void WriteSecondaryObjects(ElasticClient client)
        {
            var indexes = client.GetAlias(x => x.Name("data_rw"));

            foreach (var index in indexes.Indices)
            {
                client.Index(new Company() { Name = "Mega Acme Corp" }, idx => idx.Index(index.Key));
                client.Index(new Company() { Name = "Global World Domination Corp Acme LLC" }, idx => idx.Index(index.Key));
            }
        }

        public static IResponse ThrowOnError(IResponse response, string actionDescription = null)  
        {
            //http://stackoverflow.com/a/34867857/32963
            if (!response.IsValid)
            {
                throw new Exception(actionDescription == null ? string.Empty : "Failed to " + actionDescription + ": " + response.ServerError.Error);
            }

            return response;
        }
        public static void Reindex(ElasticClient client, string aliasName, string currentIndexName, string nextIndexName)
        {
            Console.WriteLine("Reindexing documents to new index...");
            var searchResult = client.Search<object>(s => s.Index(currentIndexName).AllTypes().From(0).Size(100).Query(q => q.MatchAll()).SearchType(Elasticsearch.Net.SearchType.Scan).Scroll("2m"));
            if (searchResult.Total <= 0)
            {
                Console.WriteLine("Existing index has no documents, nothing to reindex.");
            }
            else
            {
                var page = 0;
                IBulkResponse bulkResponse = null;
                do
                {
                    var result = searchResult;
                    searchResult = client.Scroll<object>(new Time("2m"), result.ScrollId);
                    if (searchResult.Documents != null && searchResult.Documents.Any())
                    {
                        ThrowOnError(searchResult,"reindex scroll " + page);
                        bulkResponse = (IBulkResponse)ThrowOnError(client.Bulk(b =>
                        {
                            foreach (var hit in searchResult.Hits)
                            {
                                b.Index<object>(bi => bi.Document(hit.Source).Type(hit.Type).Index(nextIndexName).Id(hit.Id));
                            }

                            return b;
                        }),"reindex page " + page);
                        Console.WriteLine("Reindexing progress: " + (page + 1) * 100);
                    }

                    ++page;
                }
                while (searchResult.IsValid && bulkResponse != null && bulkResponse.IsValid && searchResult.Documents != null && searchResult.Documents.Any());
                Console.WriteLine("Reindexing complete!");
            }

            Console.WriteLine("Updating alias to point to new index...");
            client.Alias(a => a
                .Add(aa => aa.Alias(aliasName).Index(nextIndexName))
                .Remove(aa => aa.Alias(aliasName).Index(currentIndexName)));

            // TODO: Don't forget to delete the old index if you want
        }
    }
}
