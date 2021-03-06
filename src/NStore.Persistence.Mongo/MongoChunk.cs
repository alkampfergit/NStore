﻿using MongoDB.Bson.Serialization.Attributes;

namespace NStore.Persistence.Mongo
{
    public class MongoChunk : IMongoChunk
    {
        [BsonId]
        public long Position { get; private set; }

        public string PartitionId { get; private set; }
        public long Index { get; private set; }
        public object Payload { get; private set; }
        public string OperationId { get; private set; }

        private bool IndexEqualToId;

        public void ReplacePayload(object payload)
        {
            this.Payload = payload;
        }

        public void RewriteIndex(long newIndex)
        {
            this.Index = newIndex;
        }

        public void RewritePosition(long newPosition)
        {
            this.Position = newPosition;
            if (IndexEqualToId)
            {
                RewriteIndex(newPosition);
            }
        }

        public void RewriteOperationId(string id)
        {
            this.OperationId = id;
        }

        public virtual void Init(
            long id,
            string partitionId,
            long index,
            object payload,
            string operationId)
        {
            Position = id;
            PartitionId = partitionId;
            if (index < 0)
            {
                Index = id;
                IndexEqualToId = true;
            }
            else
            {
                Index = index;
            }
            Payload = payload;
            OperationId = operationId;
        }
    }
}