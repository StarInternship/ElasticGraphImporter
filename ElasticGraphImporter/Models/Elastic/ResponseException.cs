using Nest;
using System;

namespace ElasticGraphImporter.Models.Elastic
{
    public class ResponseException : Exception
    {
        public ResponseException(IResponse response) : base(response.DebugInformation)
        {
        }
    }
}
