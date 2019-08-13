using System.IO;
using System.Text;

namespace ElasticGraphImporter
{
    public static class FileStreamExtension
    {
        public static string ReadLine(this FileStream fs)
        {
            var currentLine = new StringBuilder();
            int b;
            while (fs.Position < fs.Length && 10 != (b = fs.ReadByte()))
            {
                currentLine.Append((char)b);
            }
            return currentLine.ToString().Trim();
        }
    }
}
