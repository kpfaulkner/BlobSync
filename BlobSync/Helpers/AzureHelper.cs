//-----------------------------------------------------------------------
// <copyright >
//    Copyright 2013 Ken Faulkner
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
//-----------------------------------------------------------------------

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobSync.Helpers
{
    class AzureHelper
    {
        static CloudBlobClient BlobClient { get; set; }

        const string AzureDetection = "windows.net";
        const string DevAzureDetection = "127.0.0.1";
        const string AzureBaseUrl = "blob.core.windows.net";

        static AzureHelper()
        {
            BlobClient = null;
        }

        private static bool IsDevUrl(string url)
        {
            return ConfigHelper.IsDev == "1";
        }

        // blobUrl can contain multiple levels of / due to virtual directories 
        // may be referenced.
        public static string GetContainerFromUrl(string blobUrl, bool assumeNoBlob = false)
        {
            var url = new Uri(blobUrl);
            string container = "";  // there may be no container.

            if (IsDevUrl(blobUrl))
            {
                container = url.Segments[2];
            }
            else
            {
                if (url.Segments.Length > 1)
                {
                    container = url.Segments[1];
                }
            }

            container = container.TrimEnd('/');
            return container;
        }

        public static CloudBlobClient GetCloudBlobClient()
        {
            if (BlobClient == null)
            {
                if (IsDevUrl())
                {

                    CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;
                    BlobClient = storageAccount.CreateCloudBlobClient();

                }
                else
                {
                    var accountName = ConfigHelper.AzureAccountName;
                    string accountKey = ConfigHelper.AzureAccountKey;

                    var credentials = new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(accountName, accountKey);
                    CloudStorageAccount azureStorageAccount = new CloudStorageAccount(credentials, true);
                    BlobClient = azureStorageAccount.CreateCloudBlobClient();

                    // retry policy.
                    // could do with a little work.
                    IRetryPolicy linearRetryPolicy = new LinearRetry(TimeSpan.FromSeconds(ConfigHelper.RetryAttemptDelayInSeconds), ConfigHelper.MaxRetryAttempts);
                    BlobClient.RetryPolicy = linearRetryPolicy;

                }

            }

            return BlobClient;
        }

        public static string GetAccountNameFromUrl(string blobUrl)
        {
            var account = "";

            if (!string.IsNullOrEmpty(blobUrl))
            {
                Uri url = new Uri(blobUrl);
                var blobName = "";
                account = url.Host.Split('.')[0];
            }

            return account;
        }


        internal static string GenerateUrl(string containerName, string blobName)
        {
            var url = "https://" + ConfigHelper.AzureAccountName + "." + AzureBaseUrl + "/" + containerName + "/" + blobName;

            return url;
        }
    }
}
