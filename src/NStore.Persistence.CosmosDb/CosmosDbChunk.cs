using Newtonsoft.Json;
using System;

namespace NStore.Persistence.CosmosDb
{
    public class CosmosDbChunk : ICosmosDbChunk
    {
        public CosmosDbChunk(long position, string partitionId, long index, object payload, string operationId)
        {
            Position = position;
            Id = position.ToString();
            PartitionId = partitionId;
            Index = index;
            Payload = payload;
            OperationId = operationId;
        }

        public String CosmosDbPartition { get; set; } = "1";

        /// <summary>
        /// Id property in Cosmos must be a string
        /// </summary>
        [JsonProperty(PropertyName = "id")]
        public string Id { get; private set; }

        public long Position { get; private set; }

        public string PartitionId { get; private set; }
        public long Index { get; private set; }
        public object Payload { get; private set; }
        public string OperationId { get; private set; }

    }
}
