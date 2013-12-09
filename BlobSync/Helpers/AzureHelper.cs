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

        private static bool IsDevUrl()
        {
            return ConfigHelper.IsDev == "1";
        }

        public static bool DoesBlobExist(string container, string blobName)
        {
            var exists = false;
            try
            {
                var client = GetCloudBlobClient();

                var url = GenerateUrl(container, blobName);

                var blob = client.GetBlobReferenceFromServer(new Uri(url));

                if (blob != null)
                    exists = true;
            }
            catch (Exception)
            {
                
            }

            return exists;
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

        internal static string GetBlobNameFromFilePath(string localFilePath)
        {
            var url = new Uri(localFilePath);
            var blobName = url.Segments[url.Segments.Length - 1];
            return blobName;
        }

        internal static bool DoesBlobSignatureExist(string container, string blobName)
        {
            var exists = false;
            try
            {
                var client = GetCloudBlobClient();

                var url = GenerateUrl(container, blobName);

                // sig file will be same of blob name except with extension ".sig"
                url += ".sig";

                var blob = client.GetBlobReferenceFromServer(new Uri(url));

                if (blob != null)
                    exists = true;
            }
            catch (Exception)
            {

            }

            return exists;
        }
    }
}
