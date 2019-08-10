using System;
using System.Collections.Generic;
using System.IO;
using BigDataPathFinding.Models;
using Nest;

namespace ElasticGraphImporter
{
    internal static class Program
    {
        private static ElasticClient _client;
        private const string DirectoryPath = "..\\..\\files";
        private static readonly Dictionary<string, Guid> Ids = new Dictionary<string, Guid>();
        private static readonly List<Node> NodesList = new List<Node>();
        private static readonly List<Edge> EdgesList = new List<Edge>();

        private static void Main()
        {
            var settings = new ConnectionSettings(new Uri($"http://localhost:9200"));
            _client = new ElasticClient(settings);

            while (true)
            {
                Console.WriteLine("What is the graph file name?");
                var graphName = Console.ReadLine()?.Trim();

                ImportGraph(DirectoryPath + "/" + graphName, graphName?.Split('.')[0].ToLower());
            }
        }

        private static void ImportGraph(string filePath, string graphName)
        {
            Ids.Clear();
            NodesList.Clear();
            EdgesList.Clear();
            var nodesTableName = graphName + "_node_set";
            var connectionsTableName = graphName + "_connections";

            if (_client.Indices.Exists(nodesTableName).Exists)
            {
                _client.Indices.Delete(nodesTableName);
                Console.WriteLine(nodesTableName + " deleted.");
            }

            if (_client.Indices.Exists(connectionsTableName).Exists)
            {
                _client.Indices.Delete(connectionsTableName);
                Console.WriteLine(connectionsTableName + " deleted.");
            }

            _client.Indices.Create(nodesTableName);
            Console.WriteLine(nodesTableName + " created.");
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
            ));
            Console.WriteLine(connectionsTableName + " created.");

            var lines = File.ReadAllLines(filePath);
            foreach (var line in lines) ReadEdge(line);

            var nodesBulkDescriptor = new BulkDescriptor(nodesTableName);
            NodesList.ForEach(
                node => nodesBulkDescriptor.Index<Dictionary<string, object>>(
                    i => i.Document(new Dictionary<string, object> {["name"] = node.Name}).Id(node.Id)
                )
            );
            _client.Bulk(nodesBulkDescriptor);

            _client.Bulk(b => b.Index(connectionsTableName).IndexMany(EdgesList));
            Console.WriteLine(graphName + " Imported.");
        }


        private static void ReadEdge(string edge)
        {
            var groups = edge.Split(',');
            var source = groups[0];
            var target = groups[1];
            var weight = double.Parse(groups[2]);
            AddEdge(source, target, weight);
        }

        private static void AddEdge(string sourceName, string targetName, double weight)
        {
            if (!Ids.ContainsKey(sourceName)) AddNode(sourceName);
            if (!Ids.ContainsKey(targetName)) AddNode(targetName);
            EdgesList.Add(new Edge(Ids[sourceName], Ids[targetName], weight));
        }

        private static void AddNode(string name)
        {
            var id = Guid.NewGuid();
            var node = new Node(id, name);
            Ids.Add(name, id);
            NodesList.Add(node);
        }
    }
}