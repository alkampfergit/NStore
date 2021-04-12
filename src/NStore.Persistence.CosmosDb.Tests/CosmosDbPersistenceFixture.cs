﻿using NStore.Core.Persistence;
using NStore.Persistence.CosmosDb;
using System.Threading;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

// ReSharper disable CheckNamespace
namespace NStore.Persistence.Tests
{
    public partial class BasePersistenceTest
    {
        protected string _mongoConnectionString;
        private CosmosDbPersistenceOptions _options;
        private ICosmosDbPersistence _cosmosDbPersistence;
        private const string TestSuitePrefix = "CosmosDB";

        protected internal IPersistence Create(bool dropOnInit)
        {
            _options = GetCosmosPersistenceOptions();
            if (dropOnInit)
            {
                _options.SetDropOnInit();
            }
            _cosmosDbPersistence = CreatePersistence(_options);
            return _cosmosDbPersistence;
        }

        protected virtual internal CosmosDbPersistenceOptions GetCosmosPersistenceOptions()
        {
            var options = new CosmosDbPersistenceOptions();
            return options.SetConnection(
                "https://nstore.documents.azure.com", //this is the address of the database.
                "P60tqZOTN3TINvYV0A2q0jFRcOaetQQedNxLPi9IibEl1382TKyduN3tqquzpLT9hqyckY724pu4ZoJisM66zg==" //put here your key, but pay attention not to commit the key.
            );
        }

        protected virtual ICosmosDbPersistence CreatePersistence(CosmosDbPersistenceOptions options)
        {
            var persistence = new CosmosDbPersistence(options);
            persistence.InitAsync(new CancellationTokenSource().Token).Wait();
            return persistence;
        }

        protected void Clear(IPersistence persistence, bool drop)
        {
            // nothing to do
        }
    }
}