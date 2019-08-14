using ElasticGraphImporter.Models.Elastic;
using Nest;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace ElasticGraphImporter.Models
{
    class ElasticDataInserter
    {
        private readonly Uri ElasticUri = new Uri($"http://localhost:9200");
        private readonly ElasticClient _client;
        private readonly Stopwatch Watch = new Stopwatch();
        public Task Task { get; private set; }

        public ElasticDataInserter()
        {
            var settings = new ConnectionSettings(ElasticUri);
            _client = new ElasticClient(settings);
        }

        public void CreateConnectionsIndex(string connectionsTableName)
        {
            if (_client.Indices.Exists(connectionsTableName).Exists)
            {
                Watch.Restart();
                _client.Indices.Delete(connectionsTableName).Validate();
                Console.WriteLine(connectionsTableName + " deleted in " + Watch.ElapsedMilliseconds + " ms.");
            }
            Watch.Restart();
            _client.Indices.Create(connectionsTableName, c => c.Map<Edge>(
                m => m.Properties(p => p
                    .Keyword(k => k
                        .Name(edge => edge.SourceId)
                    )
                    .Keyword(k => k
                        .Name(edge => edge.TargetId)
                    )
                    .Number(k => k
                        .Name(edge => edge.Weight)
                    )
                )
            )).Validate();
            Console.WriteLine(connectionsTableName + " created in " + Watch.ElapsedMilliseconds + " ms.");
        }

        public void CreateNodeSetIndex(string nodesTableName)
        {
            if (_client.Indices.Exists(nodesTableName).Exists)
            {
                Watch.Restart();
                _client.Indices.Delete(nodesTableName).Validate();
                Console.WriteLine(nodesTableName + " deleted in " + Watch.ElapsedMilliseconds + " ms.");
            }

            Watch.Restart();
            _client.Indices.Create(nodesTableName).Validate();
            Console.WriteLine(nodesTableName + " created in " + Watch.ElapsedMilliseconds + " ms.");
        }

        public void NodesBulkInsert(string nodesTableName, List<Node> list)
        {
            var nodesBulkDescriptor = new BulkDescriptor(nodesTableName);

            Watch.Restart();
            list.ForEach(
                node => nodesBulkDescriptor.Index<Dictionary<string, object>>(
                    i => i.Document(new Dictionary<string, object> { ["name"] = node.Name }).Id(node.Id)
                )
            );

            _client.Bulk(nodesBulkDescriptor).Validate();
            Console.WriteLine("Bulk node insert in " + Watch.ElapsedMilliseconds + " ms. count: " + list.Count);
        }

        public void EdgesBulkInsert(string connectionsTableName, List<Edge> list)
        {
            Watch.Restart();
            _client.Bulk(b => b.Index(connectionsTableName).IndexMany(list)).Validate();
            Console.WriteLine("Bulk edge insert in " + Watch.ElapsedMilliseconds + " ms. count: " + list.Count);
        }
    }
}
