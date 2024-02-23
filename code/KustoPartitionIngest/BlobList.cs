﻿using Azure.Core;
using Azure.Storage.Blobs;

namespace KustoPartitionIngest
{
    internal class BlobList
    {
        private readonly BlobContainerClient _blobContainer;
        private readonly string _prefix;

        public event EventHandler<Uri> UriDiscovered;

        #region Constructors
        public BlobList(TokenCredential credentials, string storageUrl)
        {
            (var containerUri, var prefix) = ExtractContainerAndPrefix(storageUrl);

            _blobContainer = new BlobContainerClient(containerUri, credentials);
            _prefix = prefix;
        }

        private static (Uri containerUri, string prefix) ExtractContainerAndPrefix(string storageUrl)
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

                return (containerUri, prefix);
            }
        }
        #endregion

        public async Task ListBlobsAsync()
        {
            var pageable = _blobContainer.GetBlobsAsync(prefix: _prefix);

            await foreach (var item in pageable)
            {
                var blobName = item.Name;
                var blobClient = _blobContainer.GetBlobClient(blobName);
                var blobUri = blobClient.Uri;

                RaiseUri(blobUri);
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