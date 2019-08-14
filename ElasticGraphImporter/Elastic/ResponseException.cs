using Nest;
using System;

namespace BigDataPathFinding.Models.Elastic
{
    public class ResponseException : Exception
    {
        public ResponseException(IResponse response) : base(response.DebugInformation)
        {
        }
    }
}
