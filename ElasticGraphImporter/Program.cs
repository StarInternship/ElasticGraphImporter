using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using BigDataPathFinding.Models;
using Nest;

namespace ElasticGraphImporter
{
    internal static class Program
    {
        private static ElasticClient _client;
        private const string DirectoryPath = "../../files/";
        private const int BulkInsertChunkSize = 300000;
        private const int LinesPerRead = 3000000;
        private static readonly Stopwatch sw = new Stopwatch();
        private static readonly Uri ElasticUri = new Uri($"http://localhost:9200");
        private static Dictionary<string, Guid> Ids = new Dictionary<string, Guid>();
        private static List<Node> NodesList = new List<Node>();
        private static List<Edge> EdgesList = new List<Edge>();
        private static int numberOfReadLines;

        private static void Main()
        {
            var settings = new ConnectionSettings(ElasticUri);
            _client = new ElasticClient(settings);

            while (true)
            {
                Console.WriteLine("What is the graph file name?");
                var graphName = Console.ReadLine()?.Trim(' ');
                numberOfReadLines = 0;

                ImportGraph(DirectoryPath + graphName, graphName?.Split('.')[0].ToLower());
            }
        }

        private static void ImportGraph(string filePath, string graphName)
        {
            Ids = new Dictionary<string, Guid>();
            NodesList = new List<Node>();
            EdgesList = new List<Edge>();
            var nodesTableName = graphName + "_node_set";
            var connectionsTableName = graphName + "_connections";

            CreateNodeSetIndex(nodesTableName);
            CreateConnectionsIndex(connectionsTableName);

            foreach (var lines in ReadCursored(filePath))
            {
                NodesList = new List<Node>();
                EdgesList = new List<Edge>();

                foreach (var line in lines)
                {
                    ReadEdge(line);
                }

                foreach (var list in NodesList.ChunkBy(BulkInsertChunkSize))
                {
                    NodesBulkInsert(nodesTableName, list);
                }

                foreach (var list in EdgesList.ChunkBy(BulkInsertChunkSize))
                {
                    EdgesBulkInsert(connectionsTableName, list);
                }
            }

            Console.WriteLine(graphName + " Imported.");
        }

        private static void EdgesBulkInsert(string connectionsTableName, List<Edge> list)
        {
            sw.Restart();
            _client.Bulk(b => b.Index(connectionsTableName).IndexMany(list));
            Console.WriteLine("Bulk edge insert in " + sw.ElapsedMilliseconds + " ms. count: " + list.Count);
        }

        private static void NodesBulkInsert(string nodesTableName, List<Node> list)
        {
            var nodesBulkDescriptor = new BulkDescriptor(nodesTableName);

            sw.Restart();
            list.ForEach(
                node => nodesBulkDescriptor.Index<Dictionary<string, object>>(
                    i => i.Document(new Dictionary<string, object> { ["name"] = node.Name }).Id(node.Id)
                )
            );

            _client.Bulk(nodesBulkDescriptor);
            Console.WriteLine("Bulk node insert in " + sw.ElapsedMilliseconds + " ms. count: " + list.Count);
        }

        private static void CreateConnectionsIndex(string connectionsTableName)
        {
            if (_client.Indices.Exists(connectionsTableName).Exists)
            {
                sw.Restart();
                _client.Indices.Delete(connectionsTableName);
                Console.WriteLine(connectionsTableName + " deleted in " + sw.ElapsedMilliseconds + " ms.");
            }
            sw.Restart();
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
            Console.WriteLine(connectionsTableName + " created in " + sw.ElapsedMilliseconds + " ms.");
        }

        private static void CreateNodeSetIndex(string nodesTableName)
        {
            if (_client.Indices.Exists(nodesTableName).Exists)
            {
                sw.Restart();
                _client.Indices.Delete(nodesTableName);
                Console.WriteLine(nodesTableName + " deleted in " + sw.ElapsedMilliseconds + " ms.");
            }

            sw.Restart();
            _client.Indices.Create(nodesTableName);
            Console.WriteLine(nodesTableName + " created in " + sw.ElapsedMilliseconds + " ms.");
        }

        private static IEnumerable<IEnumerable<string>> ReadCursored(string filePath)
        {
            using (var input = File.OpenRead(filePath))
            {
                while (input.Position < input.Length)
                {
                    int remainingLines = LinesPerRead;
                    var currentList = new List<string>();
                    sw.Restart();

                    while (input.Position < input.Length && remainingLines-- > 0)
                    {
                        currentList.Add(input.ReadLine());
                    }
                    numberOfReadLines += currentList.Count;

                    Console.WriteLine(currentList.Count + " lines read in " + sw.ElapsedMilliseconds + " ms.");
                    Console.WriteLine("all lines read: " + numberOfReadLines);

                    yield return currentList;
                }
            }
        }

        private static void ReadEdge(string edge)
        {
            var groups = Regex.Split(edge, ",");
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