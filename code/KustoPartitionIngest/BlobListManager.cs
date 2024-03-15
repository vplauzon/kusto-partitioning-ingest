using Azure;
using Azure.Storage.Blobs;

namespace KustoPartitionIngest
{
    internal class BlobListManager
    {
        private readonly BlobContainerClient _blobContainer;
        private readonly string _sasToken;
        private readonly string _prefix;

        public event EventHandler<Uri>? UriDiscovered;

        #region Constructors
        public BlobListManager(string storageUrl)
        {
            (var sasToken, var containerUri, var prefix) = ExtractContainerAndPrefix(storageUrl);
            var sasTokenCredential = new AzureSasCredential(sasToken);

            _blobContainer = new BlobContainerClient(containerUri, sasTokenCredential);
            _sasToken = sasToken.ToString()!;
            _prefix = prefix;
        }

        private static (string sasToken, Uri containerUri, string prefix) ExtractContainerAndPrefix(
            string storageUrl)
        {
            if (!Uri.TryCreate(storageUrl, UriKind.Absolute, out var storageUri))
            {
                throw new ArgumentException("Isn't a conform URL", nameof(storageUrl));
            }
            else
            {
                var parts = storageUri.AbsolutePath.Split('/');

                if (parts.Length < 1)
                {
                    throw new ArgumentException("No container in URL", nameof(storageUrl));
                }
                var containerName = parts[1];
                var containerUri =
                    new Uri($"{storageUri.Scheme}://{storageUri.Host}/{containerName}");
                var prefix = string.Join('/', parts.Skip(2));

                return (storageUri.Query, containerUri, prefix);
            }
        }
        #endregion

        public async Task ListBlobsAsync()
        {
            var pageable = _blobContainer.GetBlobsAsync(prefix: _prefix);

            await foreach (var item in pageable)
            {
                if (item.Properties.ContentLength > 0)
                {
                    var blobName = item.Name;
                    var blobClient = _blobContainer.GetBlobClient(blobName);
                    var blobUri = new Uri($"{blobClient.Uri}{_sasToken}");

                    RaiseUri(blobUri);
                }
            }
        }

        private void RaiseUri(Uri blobUri)
        {
            if (UriDiscovered != null)
            {
                UriDiscovered(this, blobUri);
            }
        }
    }
}