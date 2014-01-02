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

using System.Runtime.Remoting.Messaging;
using BlobSync.Datatypes;
using BlobSync.Helpers;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobSync
{
    public class AzureOps : ICloudOps
    {
        
        // updates blob if possible.
        // if blob doesn't already exist OR does not have a signature file 
        // then we just upload as usual.
        public void UploadFile(string containerName, string blobName, string localFilePath)
        {
            // 1) Does remote blob exist?
            // 2) if so, download existing signature for blob.
            if (AzureHelper.DoesBlobExist(containerName, blobName) && AzureHelper.DoesBlobSignatureExist(containerName, blobName))
            {
                // 3) If blob exists and have signature, then let the magic begin.
                // 3.1) Download existing blob signature from Azure.
                // 3.2) Search through local file for matches in existing blob signature.
                // 3.3) Upload differences to Azure
                // 3.4) Upload new signature.s

                var blobSig = DownloadSignatureForBlob(containerName, blobName);
                var searchResults = CommonOps.SearchLocalFileForSignatures(localFilePath, blobSig);
                UploadDelta(localFilePath, searchResults, containerName, blobName);
                var sig = CommonOps.CreateSignatureForLocalFile(localFilePath);
                UploadSignatureForBlob(blobName, containerName, sig);

            }
            else
            {
                // 4) If blob or signature does NOT exist, just upload as normal. No tricky stuff to do here.
                // 4.1) Generate signature and upload it.
               
                // refactor into separate method.
                var f = File.Open(localFilePath, FileMode.Open);
                var fileLength = f.Length;
                f.Close();

                var remainingBytes = new RemainingBytes()
                {
                    BeginOffset = 0,
                    EndOffset = fileLength - 1
                };

                var allUploadedBlocks = UploadBytes(remainingBytes, localFilePath, containerName, blobName);
                var res = (from b in allUploadedBlocks orderby b.Offset ascending select b.BlockId);

                var client = AzureHelper.GetCloudBlobClient();
                var container = client.GetContainerReference(containerName);
                var blob = container.GetBlockBlobReference(blobName);

                blob.PutBlockList(res.ToArray());
                
                var sig = CommonOps.CreateSignatureForLocalFile(localFilePath);
                UploadSignatureForBlob(blobName, containerName, sig);

            }
        }

        // Uploads differences between existing blob and updated local file.
        // Have local file to reference, the search results (indicating which parts need to be uploaded)
        // container and blob name.
        private void UploadDelta(string localFilePath, SignatureSearchResult searchResults, string containerName, string blobName)
        {
            var allUploadedBlocks = new List<UploadedBlock>();

            // loop through each section of the search results.
            // create blob from each RemainingBytes instances.
            // reuse the blocks already in use.
            foreach (var remainingBytes in searchResults.ByteRangesToUpload)
            {
                var uploadedBlockList = UploadBytes(remainingBytes, localFilePath, containerName, blobName);
                allUploadedBlocks.AddRange( uploadedBlockList);
            }

            // once we're here we should have uploaded ALL new data to Azure Blob Storage.
            // We then need to send the "construct" blob message.
            // loop through existing blocks and get offset + blockId's.
            foreach (var sig in searchResults.SignaturesToReuse)
            {
                var blockId = Convert.ToBase64String(sig.MD5Signature);
                allUploadedBlocks.Add(new UploadedBlock() {BlockId = blockId, Offset = sig.Offset});
            }

            // needs to be sorted by offset so the final blob constructed is in correct order.
            var res = (from b in allUploadedBlocks orderby b.Offset ascending select b.BlockId);

            var client = AzureHelper.GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            blob.PutBlockList(res.ToArray());

        }

        private List<UploadedBlock> UploadBytes(RemainingBytes remainingBytes, string localFilePath, string containerName, string blobName)
        {
            var uploadedBlockList = new List<UploadedBlock>();

            try
            {
                var client = AzureHelper.GetCloudBlobClient();
                var container = client.GetContainerReference(containerName);
                container.CreateIfNotExists();
                var blob = container.GetBlockBlobReference(blobName);

                var blockCount =
                    Math.Round((double) (remainingBytes.EndOffset - remainingBytes.BeginOffset + 1)/
                               (double) ConfigHelper.SignatureSize, MidpointRounding.AwayFromZero);

                using (var stream = new FileStream(localFilePath, FileMode.Open))
                {
                    for (var offset = remainingBytes.BeginOffset; offset <= remainingBytes.EndOffset;)
                    {
                        var sizeToRead = offset + ConfigHelper.SignatureSize <= remainingBytes.EndOffset
                            ? ConfigHelper.SignatureSize
                            : remainingBytes.EndOffset - offset + 1;

                        // seek to the offset we need. Dont forget remaining bytes may be bigger than the signature size
                        // we want to deal with.
                        stream.Seek(offset, SeekOrigin.Begin);
                        var bytesToRead = new byte[sizeToRead];
                        stream.Read(bytesToRead, 0, (int) sizeToRead);

                        var sig = CommonOps.GenerateBlockSig(bytesToRead, offset, (int) sizeToRead, 0);
                        var blockId = Convert.ToBase64String(sig.MD5Signature);
                        
                        // yes, putting into memory stream is probably a waste here.
                        using (var ms = new MemoryStream(bytesToRead))
                        {
                            var options = new BlobRequestOptions() {ServerTimeout = new TimeSpan(0, 90, 0)};
                            blob.PutBlock(blockId, ms,null,null, options);
                           
                        }

                        // store the block id that is associated with this byte range.
                        uploadedBlockList.Add(new UploadedBlock()
                        {
                            BlockId = blockId,
                            Offset = offset
                        });

                        offset += sizeToRead;

                    }
                }
                
            }
            catch (ArgumentException ex)
            {
                // probably bad container.
                Console.WriteLine("Argument Exception " + ex.ToString());
            }
            finally
            {

            }

            return uploadedBlockList;
        }


        public SizeBasedCompleteSignature DownloadSignatureForBlob(string container, string blobName)
        {
            var blobSigName = AzureHelper.GetSignatureBlobName(container, blobName);

            SizeBasedCompleteSignature sig;

            using (var stream = new MemoryStream())
            {
                DownloadBlob(container, blobSigName, stream);
                sig = SerializationHelper.ReadSizeBasedBinarySignature(stream);
            }

            return sig;
        }

        public void UploadSignatureForBlob(string blobName, string containerName, SizeBasedCompleteSignature sig)
        {
            var client = AzureHelper.GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);

            // upload sig.
            var sigBlobName = AzureHelper.SetSignatureName(containerName, blobName);

            var sigBlob = container.GetBlockBlobReference(sigBlobName);

            using (Stream s = new MemoryStream())
            {
                SerializationHelper.WriteBinarySizedBasedSignature(sig, s);
                s.Seek(0, SeekOrigin.Begin);
                sigBlob.UploadFromStream(s);
            }
        }

        public void UploadBlockBlob(string localFilePath, string containerName, string blobName)
        {
            Stream stream = null;

            try
            {
                var client = AzureHelper.GetCloudBlobClient();

                
                var container = client.GetContainerReference(containerName);
                container.CreateIfNotExists();

                stream = new FileStream(localFilePath, FileMode.Open);
                
                // assuming block blobs for now.
                WriteBlockBlob(stream, blobName, container);
                

            }
            catch (ArgumentException ex)
            {
                // probably bad container.
                Console.WriteLine("Argument Exception " + ex.ToString());
            }
            finally
            {
                if (stream != null)
                {
                    stream.Close();
                }

            }

        }




        // can make this concurrent... soonish. :)
        private void WriteBlockBlob(Stream stream, string blobName, CloudBlobContainer container, int parallelFactor = 1, int chunkSizeInMB = 2)
        {
            var blobRef = container.GetBlockBlobReference(blobName);
            blobRef.DeleteIfExists();

            // use "parallel" option even if parallelfactor == 1.
            // This is because I've found that blobRef.UploadFromStream to be unreliable.
            // Unsure if its a timeout issue or some other cause. (huge stacktrace/exception thrown from within
            // the client lib code.
            if (parallelFactor > 0)
            {
                ParallelWriteBlockBlob(stream, blobRef, parallelFactor, chunkSizeInMB);
            }
            else
            {
                blobRef.UploadFromStream(stream);
            }
        }

        // NOTE: need to check if we need to modify  blob.ServiceClient.ParallelOperationThreadCount
        private void ParallelWriteBlockBlob(Stream stream, CloudBlockBlob blob, int parallelFactor, int chunkSizeInMB)
        {
            int chunkSize = chunkSizeInMB * 1024 * 1024;
            var length = stream.Length;
            var numberOfBlocks = (length / chunkSize) + 1;
            var blockIdList = new string[numberOfBlocks];
            var chunkSizeList = new int[numberOfBlocks];
            var taskList = new List<Task>();

            var count = numberOfBlocks - 1;

            // read the data...  spawn a task to launch... then wait for all.
            while (count >= 0)
            {
                while (count >= 0 && taskList.Count < parallelFactor)
                {
                    var index = (numberOfBlocks - count - 1);

                    var chunkSizeToUpload = (int)Math.Min(chunkSize, length - (index * chunkSize));
                    chunkSizeList[index] = chunkSizeToUpload;
                    var dataBuffer = new byte[chunkSizeToUpload];
                    stream.Seek(index * chunkSize, SeekOrigin.Begin);
                    stream.Read(dataBuffer, 0, chunkSizeToUpload);

                    var t = Task.Factory.StartNew(() =>
                    {
                        var tempCount = index;
                        var uploadSize = chunkSizeList[tempCount];

                        var newBuffer = new byte[uploadSize];
                        Array.Copy(dataBuffer, newBuffer, dataBuffer.Length);

                        var blockId = Convert.ToBase64String(Guid.NewGuid().ToByteArray());

                        using (var memStream = new MemoryStream(newBuffer, 0, uploadSize))
                        {
                            blob.PutBlock(blockId, memStream, null);
                        }
                        blockIdList[tempCount] = blockId;

                    });

                    taskList.Add(t);
                    count--;


                }

                var waitedIndex = Task.WaitAny(taskList.ToArray());
                taskList.RemoveAt(waitedIndex);
            }


            Task.WaitAll(taskList.ToArray());

            blob.PutBlockList(blockIdList);
        }


        
        // download blob to stream
        public void DownloadBlob(string containerName, string blobName, Stream stream)
        {
            var client = AzureHelper.GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var url = AzureHelper.GenerateUrl(containerName, blobName);
            var blobRef = client.GetBlobReferenceFromServer(new Uri(url));

            ReadBlockBlob(blobRef, stream);
            
        }
        

        // download blob to particular path.
        public void DownloadBlob(string containerName, string blobName, string localFilePath)
        {
            var client = AzureHelper.GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var url = AzureHelper.GenerateUrl(containerName, blobName);
            var blobRef = client.GetBlobReferenceFromServer(new Uri(url));

            ReadBlockBlob(blobRef, localFilePath);
        }

        private void ReadBlockBlob(ICloudBlob blobRef, Stream stream)
        {
            var blockBlob = blobRef as CloudBlockBlob;

            // no parallel yet.
            blockBlob.DownloadToStream(stream);
        }

        private void ReadBlockBlob(ICloudBlob blobRef, string fileName)
        {            
      
            // get stream to store.
            using (var stream = CommonHelper.GetStream(fileName))
            {
                ReadBlockBlob( blobRef, stream);
            }

        }

    }
}
