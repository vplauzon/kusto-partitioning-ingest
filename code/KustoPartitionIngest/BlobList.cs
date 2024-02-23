
namespace KustoPartitionIngest
{
    internal class BlobList
    {
        private readonly string _storageUrl;

        public BlobList(string storageUrl)
        {
            _storageUrl = storageUrl;
        }

        public Task ListBlobsAsync()
        {
            throw new NotImplementedException();
        }
    }
}