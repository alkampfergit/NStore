using System;
using System.Threading;
using System.Threading.Tasks;

namespace NStore.Raw
{
    public class ProfileDecorator : IRawStore
    {
        private readonly IRawStore _store;

        public TaskProfilingInfo PersistCounter { get; }
        public TaskProfilingInfo DeleteCounter { get; }
        public TaskProfilingInfo StoreScanCounter { get; }
        public TaskProfilingInfo PartitionScanCounter { get; }

        public ProfileDecorator(IRawStore store)
        {
            _store = store;
            PersistCounter = new TaskProfilingInfo("Persist");
            PartitionScanCounter = new TaskProfilingInfo("Partition scan", "chunks read");
            DeleteCounter = new TaskProfilingInfo("Delete");
            StoreScanCounter = new TaskProfilingInfo("Store Scan", "chunks read");
        }

        public async Task ReadPartitionForward(string partitionId, long fromIndexInclusive,
            IPartitionConsumer partitionConsumer, long toIndexInclusive = Int64.MaxValue, int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var counter = new LambdaPartitionConsumer((l, o) =>
            {
                PartitionScanCounter.IncCounter1();
                return partitionConsumer.Consume(l, o);
            });

            await PartitionScanCounter.CaptureAsync(() =>
                _store.ReadPartitionForward(
                    partitionId,
                    fromIndexInclusive,
                    counter,
                    toIndexInclusive,
                    limit,
                    cancellationToken
                ));
        }

        public async Task ReadPartitionBackward(string partitionId, long fromIndexInclusive,
            IPartitionConsumer partitionConsumer, long toIndexInclusive = Int64.MaxValue, int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var counter = new LambdaPartitionConsumer((l, o) =>
            {
                PartitionScanCounter.IncCounter1();
                return partitionConsumer.Consume(l, o);
            });

            await PartitionScanCounter.CaptureAsync(() =>
                _store.ReadPartitionBackward(
                    partitionId,
                    fromIndexInclusive,
                    counter,
                    toIndexInclusive,
                    limit,
                    cancellationToken
                ));
        }

        public async Task ScanStoreAsync(long sequenceStart, ScanDirection direction, IStoreConsumer consumer,
            int limit = Int32.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var storeObserver = new LambdaStoreConsumer((si, s, l, o) =>
            {
                StoreScanCounter.IncCounter1();
                return consumer.Consume(si, s, l, o);
            });

            await StoreScanCounter.CaptureAsync(() =>
                _store.ScanStoreAsync(sequenceStart, direction, storeObserver, limit, cancellationToken)
            );
        }

        public async Task PersistAsync(string partitionId, long index, object payload, string operationId = null,
            CancellationToken cancellationToken = new CancellationToken())
        {
            await PersistCounter.CaptureAsync(() =>
                _store.PersistAsync(partitionId, index, payload, operationId, cancellationToken)
            );
        }

        public async Task DeleteAsync(string partitionId, long fromIndex = 0, long toIndex = Int64.MaxValue,
            CancellationToken cancellationToken = new CancellationToken())
        {
            await DeleteCounter.CaptureAsync(() =>
                _store.DeleteAsync(partitionId, fromIndex, toIndex, cancellationToken)
            );
        }
    }
}