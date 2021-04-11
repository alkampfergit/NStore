using Microsoft.Azure.Cosmos;
using NStore.Core.Persistence;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Persistence.CosmosDb
{
    public class CosmosDbPersistence : ICosmosDbPersistence
    {
        private readonly CosmosDbPersistenceOptions _options;

        public CosmosDbPersistence(CosmosDbPersistenceOptions options)
        {
            _options = options;
        }

        #region Custom properties

        // The Cosmos client instance
        private CosmosClient _cosmosClient;

        // The database we will create
        private Database _database;

        // The container we will create.
        private Container _container;

        private long _sequence = 0;

        #endregion

        #region Custom initialization code

        public async Task InitAsync(CancellationToken cancellationToken)
        {
            _cosmosClient = new CosmosClient(_options.EndpointUrl, _options.PrimaryKey);
            _database = await _cosmosClient.CreateDatabaseIfNotExistsAsync(_options.DatabaseName, cancellationToken: cancellationToken);

            var containerBuilder = _database
                .DefineContainer(_options.ContainerName, "/cosmosDbPartition")
                .WithUniqueKey()
                    .Path("/partitionId")
                    .Path("/index")
                .Attach()
                .WithUniqueKey()
                    .Path("/partitionId")
                    .Path("/operationId")
                .Attach();

            _container = await containerBuilder.CreateIfNotExistsAsync();
        }

        #endregion

        #region IPersistence interface

        public bool SupportsFillers => true;

        public async Task<IChunk> AppendAsync(string partitionId, long index, object payload, string operationId, CancellationToken cancellationToken)
        {
            long id = await GetNextId(1, cancellationToken).ConfigureAwait(false);
            var chunk = new ComsosDbChunk(
                id,
                partitionId,
                index,
                payload,
                operationId ?? Guid.NewGuid().ToString());

            return await InternalPersistAsync(chunk, cancellationToken).ConfigureAwait(false);
        }

        public Task DeleteAsync(string partitionId, long fromLowerIndexInclusive, long toUpperIndexInclusive, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task ReadAllAsync(long fromPositionInclusive, ISubscription subscription, int limit, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task ReadAllByOperationIdAsync(string operationId, ISubscription subscription, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task ReadBackwardAsync(string partitionId, long fromUpperIndexInclusive, ISubscription subscription, long toLowerIndexInclusive, int limit, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<IChunk> ReadByOperationIdAsync(string partitionId, string operationId, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public async Task ReadForwardAsync(
                 string partitionId,
                 long fromLowerIndexInclusive,
                 ISubscription subscription,
                 long toUpperIndexInclusive,
                 int limit,
                 CancellationToken cancellationToken
             )
        {
            var filter = Builders<TChunk>.Filter.And(
                Builders<TChunk>.Filter.Eq(x => x.PartitionId, partitionId),
                Builders<TChunk>.Filter.Gte(x => x.Index, fromLowerIndexInclusive),
                Builders<TChunk>.Filter.Lte(x => x.Index, toUpperIndexInclusive)
            );

            var sort = Builders<TChunk>.Sort.Ascending(x => x.Index);
            var options = new FindOptions<TChunk>() { Sort = sort };
            if (limit != int.MaxValue)
            {
                options.Limit = limit;
            }

            await PushToSubscriber(
                fromLowerIndexInclusive,
                subscription,
                options,
                filter,
                false, cancellationToken).ConfigureAwait(false);
        }

        private async Task PushToSubscriber(
            long start,
            ISubscription subscription,
            FindOptions<TChunk> options,
            FilterDefinition<TChunk> filter,
            bool broadcastPosition,
            CancellationToken cancellationToken)
        {
            long positionOrIndex = 0;
            await subscription.OnStartAsync(start).ConfigureAwait(false);

            try
            {
                using (var cursor = await _chunks.FindAsync(filter, options, cancellationToken).ConfigureAwait(false))
                {
                    while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var batch = cursor.Current;
                        foreach (var b in batch)
                        {
                            positionOrIndex = broadcastPosition ? b.Position : b.Index;
                            b.ReplacePayload(_mongoPayloadSerializer.Deserialize(b.Payload));
                            if (!await subscription.OnNextAsync(b).ConfigureAwait(false))
                            {
                                await subscription.StoppedAsync(positionOrIndex).ConfigureAwait(false);
                                return;
                            }
                        }
                    }
                }

                await subscription.CompletedAsync(positionOrIndex).ConfigureAwait(false);
            }
            catch (TaskCanceledException ex)
            {
                _logger.LogWarning($"PushToSubscriber: {ex.Message}.\n{ex.StackTrace}");
                await subscription.StoppedAsync(positionOrIndex).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.LogError($"Error During PushToSubscriber: {e.Message}.\n{e.StackTrace}");
                await subscription.OnErrorAsync(positionOrIndex, e).ConfigureAwait(false);
            }
        }

        public Task<long> ReadLastPositionAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<IChunk> ReadSingleBackwardAsync(string partitionId, long fromUpperIndexInclusive, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Internal helper methods

        private async Task<long> GetNextId(int size, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (_options.UseLocalSequence)
            {
                return Interlocked.Add(ref _sequence, size);
            }

            throw new NotImplementedException("Still not implemented");
        }

        private async Task<ComsosDbChunk> InternalPersistAsync(
            ComsosDbChunk chunk,
            CancellationToken cancellationToken = default(CancellationToken)
        )
        {
            while (true)
            {
                try
                {
                    await this._container.CreateItemAsync(chunk, null).ConfigureAwait(true);
                    return chunk;
                }
                catch (Exception ex) 
                {
                    throw;
                }
//                catch (MongoWriteException ex)
//                {
//                    //Need to understand what kind of exception we had, some of them could lead to a retry
//                    if (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
//                    {
//                        //Index violation, we do have a chunk that broke an unique index, we need to know if this is 
//                        //at partitionId level (concurrency) or at position level (UseLocalSequence == false and multiple process/appdomain are appending to the stream).
//                        if (ex.Message.Contains(PartitionIndexIdx))
//                        {
//                            await PersistAsEmptyAsync(chunk, cancellationToken).ConfigureAwait(false);
//                            _logger.LogInformation($"DuplicateStreamIndexException: {ex.Message}.\n{ex.ToString()}");
//                            throw new DuplicateStreamIndexException(chunk.PartitionId, chunk.Index);
//                        }

//                        if (ex.Message.Contains(PartitionOperationIdx))
//                        {
//                            if (cancellationToken.IsCancellationRequested)
//                            {
//                                cancellationToken.ThrowIfCancellationRequested();
//                            }

//                            //since we should ignore the chunk (already exist a chunk with that operation Id, we fill with a blank).
//                            await PersistAsEmptyAsync(chunk, cancellationToken).ConfigureAwait(false);
//                            return null;
//                        }

//                        if (ex.Message.Contains("_id_"))
//                        {
//                            if (cancellationToken.IsCancellationRequested)
//                            {
//                                cancellationToken.ThrowIfCancellationRequested();
//                            }

//                            //some other process steals the Position, we need to warn the user, because too many of this error could suggest to enable UseLocalSequence
//                            _logger.LogWarning($@"Error writing chunk #{chunk.Position} - Some other process already wrote position {chunk.Position}. 
//Operation will be retried. 
//If you see too many of this kind of errors, consider enabling UseLocalSequence.
//{ex.Message} - {ex.GetType().FullName} ");
//                            await ReloadSequence(cancellationToken).ConfigureAwait(false);
//                            chunk.RewritePosition(await GetNextId(1, cancellationToken).ConfigureAwait(false));
//                            continue;
//                        }
//                    }

//                    _logger.LogError($"Error During InternalPersistAsync: {ex.Message}.\n{ex.ToString()}");
//                    throw;
//                }
            }

            return chunk;
        }

        #endregion
    }
}
