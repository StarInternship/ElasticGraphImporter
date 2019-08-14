using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace ElasticGraphImporter.Models
{
    class GraphData
    {
        private readonly Dictionary<string, Guid> Ids = new Dictionary<string, Guid>();
        public string NodeSetIndex { get; }
        public string ConnectionsIndex { get; }

        public GraphData(string graphName)
        {
            NodeSetIndex = graphName + "_node_set";
            ConnectionsIndex = graphName + "_connections";
        }

        public List<Node> NodesList { get; set; } = new List<Node>();
        public List<Edge> EdgesList { get; set; } = new List<Edge>();

        public void ReadEdge(string edge)
        {
            var groups = Regex.Split(edge, ",|-");
            var source = groups[0];
            var target = groups[1];
            var weight = double.Parse(groups[2]);
            AddEdge(source, target, weight);
        }

        private void AddEdge(string sourceName, string targetName, double weight)
        {
            if (!Ids.ContainsKey(sourceName)) AddNode(sourceName);
            if (!Ids.ContainsKey(targetName)) AddNode(targetName);
            EdgesList.Add(new Edge(Ids[sourceName], Ids[targetName], weight));
        }

        private void AddNode(string name)
        {
            var id = Guid.NewGuid();
            var node = new Node(id, name);
            Ids.Add(name, id);
            NodesList.Add(node);
        }
    }
}
