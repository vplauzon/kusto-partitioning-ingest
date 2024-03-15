
namespace KustoPartitionIngest
{
    internal interface IQueueManager
    {
        event EventHandler? BlobUriQueued;

        Task RunAsync();
        
        void QueueUri(Uri blobUri);

        void Complete();
    }
}