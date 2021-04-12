using NStore.Core.Logging;

namespace NStore.Persistence.CosmosDb
{
    public class CosmosDbPersistenceOptions
    {
        public INStoreLoggerFactory LoggerFactory { get; private set; } = NStoreNullLoggerFactory.Instance;

        public string EndpointUrl { get; private set; }
        public string PrimaryKey { get; private set; }
        public string DatabaseName { get; private set; } = "Nstore";
        public string ContainerName { get; private set; } = "Commits";
        public bool UseLocalSequence { get; private set; } = true;

        public bool DropOnInit { get; private set; }

        public CosmosDbPersistenceOptions SetConnection(string endpointUrl, string primaryKey)
        {
            EndpointUrl = endpointUrl;
            PrimaryKey = primaryKey;
            return this;
        }

        public CosmosDbPersistenceOptions SetLoggingFacctory(INStoreLoggerFactory nStoreLoggerFactory)
        {
            LoggerFactory = nStoreLoggerFactory;
            return this;
        }

        public CosmosDbPersistenceOptions SetDropOnInit() 
        {
            DropOnInit = true;
            return this;
        }
    }
}
