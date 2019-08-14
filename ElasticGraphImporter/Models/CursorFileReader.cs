using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace ElasticGraphImporter.Models
{
    public class CursorFileReader
    {
        private const int LinesPerRead = 3000000;
        private readonly FileStream Input;
        private readonly Stopwatch Watch = new Stopwatch();

        public CursorFileReader(string filePath) => Input = File.OpenRead(filePath);

        public int NumberOfReadLines { get; private set; }

        public IEnumerable<IEnumerable<string>> Read()
        {
            using (Input)
            {
                while (Input.Position < Input.Length)
                {
                    int remainingLines = LinesPerRead;
                    var currentList = new List<string>();
                    Watch.Restart();

                    while (Input.Position < Input.Length && remainingLines-- > 0)
                    {
                        currentList.Add(Input.ReadLine());
                    }
                    NumberOfReadLines += currentList.Count;

                    Console.WriteLine(currentList.Count + " lines read in " + Watch.ElapsedMilliseconds + " ms.");
                    Console.WriteLine("all lines read: " + NumberOfReadLines);

                    yield return currentList;
                }
            }
        }
    }
}
