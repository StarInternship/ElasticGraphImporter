using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using ElasticGraphImporter.Models;

namespace ElasticGraphImporter
{
    internal static class Program
    {
        private const string DirectoryPath = "../../files/";
        private const int BulkInsertChunkSize = 300000;
        private static ElasticDataInserter DataInserter;
        private static GraphData GraphData;
        private static Task InserterTask = null;

        private static void Main()
        {
            DataInserter = new ElasticDataInserter();

            while (true)
            {
                Console.WriteLine("What is the graph file name?");
                var graphName = Console.ReadLine()?.Trim();

                StartImporting(DirectoryPath + graphName, graphName?.Split('.')[0].ToLower());
            }
        }

        private static void StartImporting(string filePath, string graphName)
        {
            GraphData = new GraphData(graphName);

            DataInserter.CreateNodeSetIndex(GraphData.NodeSetIndex);
            DataInserter.CreateConnectionsIndex(GraphData.ConnectionsIndex);

            ReadGraph(filePath);

            Console.WriteLine(graphName + " Imported.");
        }

        private static void ReadGraph(string filePath)
        {
            foreach (var lines in new CursorFileReader(filePath).Read())
            {
                GraphData.NodesList = new List<Node>();
                GraphData.EdgesList = new List<Edge>();

                foreach (var line in lines)
                {
                    GraphData.ReadEdge(line);
                }

                if (InserterTask != null)
                {
                    InserterTask.Wait();
                }
                BeginBulkInsert();
            }
            InserterTask.Wait();
        }

        private static void BeginBulkInsert()
        {
            var nodesList = GraphData.NodesList;
            var edgesList = GraphData.EdgesList;
            InserterTask = new Task(() =>
            {
                foreach (var list in nodesList.ChunkBy(BulkInsertChunkSize))
                {
                    DataInserter.NodesBulkInsert(GraphData.NodeSetIndex, list);
                }
                foreach (var list in edgesList.ChunkBy(BulkInsertChunkSize))
                {
                    DataInserter.EdgesBulkInsert(GraphData.ConnectionsIndex, list);
                }
            });
            InserterTask.Start();
        }
    }
}