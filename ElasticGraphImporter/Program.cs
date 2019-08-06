using System;
using System.Collections.Generic;
using System.IO;
using BigDataPathFinding.Models;
using Nest;

namespace ElasticGraphImporter
{
    internal class Program
    {
        private static ElasticClient client;

        private static readonly Dictionary<string, Guid> Ids = new Dictionary<string, Guid>();
        private static readonly List<Node> NodesList = new List<Node>();
        private static readonly List<Edge> EdgesList = new List<Edge>();

        private static void Main(string[] args)
        {
            var settings = new ConnectionSettings(new Uri("http://localhost:9200"));
            client = new ElasticClient(settings);

            var filePaths = Directory.GetFiles("..\\..\\files");
            while (true)
            {
                Console.WriteLine("What is  the graph name?");
                var graphName = Console.ReadLine()?.Trim(' ').ToLower();
                foreach (var filePath in filePaths)
                {
                    var fileName = Path.GetFileName(filePath);
                    if (!fileName.ToLower().StartsWith(graphName + ".")) continue;
                    ImportGraph(filePath, graphName);
                    break;
                }
            }
        }

        private static void ImportGraph(string filePath, string graphName)
        {
            Ids.Clear();
            NodesList.Clear();
            EdgesList.Clear();
            var nodesTableName = graphName + "_node_set";
            var connectionsTableName = graphName + "_connections";
            if (client.Indices.Exists(nodesTableName).Exists)
            {
                client.DeleteByQuery<Node>(del => del
                    .Index(nodesTableName)
                    .Query(q => q.MatchAll())
                );
            }
            else
            {
                /*var createIndexResponse = client.CreateIndex("index-name", c => c
                    .Settings(s => s
                        .NumberOfShards(1)
                        .NumberOfReplicas(0)
                    )
                );*/
            }

            if (client.Indices.Exists(connectionsTableName).Exists)
            {
                client.DeleteByQuery<Edge>(del => del
                    .Index(connectionsTableName)
                    .Query(q => q.MatchAll())
                );
            }
            else
            {
                /*var createIndexResponse = client.CreateIndex("index-name", c => c
                    .Settings(s => s
                        .NumberOfShards(1)
                        .NumberOfReplicas(0)
                    )
                );*/
            }

            var lines = File.ReadAllLines(filePath);
            foreach (var line in lines) ReadEdge(line);
            var result1 = client.Bulk(b => b.Index(nodesTableName).IndexMany(NodesList));
            var result2 = client.Bulk(b => b.Index(connectionsTableName).IndexMany(EdgesList));
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