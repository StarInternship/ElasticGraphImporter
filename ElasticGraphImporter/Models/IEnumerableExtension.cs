using System.Collections.Generic;
using System.Linq;

namespace ElasticGraphImporter.Models
{
    public static class ListExtension
    {
        public static IEnumerable<List<T>> ChunkBy<T>(this IEnumerable<T> source, int chunkSize)
        {
            return source
                .Select((x, i) => new { Index = i, Value = x })
                .GroupBy(x => x.Index / chunkSize)
                .Select(x => x.Select(v => v.Value).ToList());
        }
    }
}
