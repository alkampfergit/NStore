using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Text;

namespace NStore.Persistence.CosmosDb
{
    public class CosmosDbPersistenceOptions
    {
        public string EndpointUrl { get; internal set; }
        public string PrimaryKey { get; internal set; }
        public string DatabaseName { get; internal set; } = "Nstore";
        public string ContainerName { get; internal set; } = "Commits";
        public bool UseLocalSequence { get; internal set; } = true;

        public CosmosDbPersistenceOptions SetConnection(string endpointUrl, string primaryKey) 
        {
            EndpointUrl = endpointUrl;
            PrimaryKey = primaryKey;
            return this;
        }
    }
}
