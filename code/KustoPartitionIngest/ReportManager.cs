
namespace KustoPartitionIngest
{
    internal class ReportManager
    {
        private BlobListManager blobListManager;
        private QueueManager queueManager1;
        private QueueManager? queueManager2;

        public ReportManager(
            BlobListManager blobListManager,
            QueueManager queueManager1,
            QueueManager? queueManager2)
        {
            this.blobListManager = blobListManager;
            this.queueManager1 = queueManager1;
            this.queueManager2 = queueManager2;
        }

        public void Complete()
        {
            throw new NotImplementedException();
        }

        public Task RunAsync()
        {
            throw new NotImplementedException();
        }
    }
}