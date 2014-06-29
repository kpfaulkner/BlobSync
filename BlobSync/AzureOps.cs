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

using System.ComponentModel;
using System.Runtime.Remoting.Messaging;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using BlobSync.Datatypes;
using BlobSync.Helpers;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;

namespace BlobSync
{
    public class AzureOps : ICloudOps
    {
        
        // updates blob if possible.
        // if blob doesn't already exist OR does not have a signature file 
        // then we just upload as usual.
        public long UploadFile(string containerName, string blobName, string localFilePath)
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
                var bytesUploaded = UploadDelta(localFilePath, searchResults, containerName, blobName);
                var sig = CommonOps.CreateSignatureForLocalFile(localFilePath);
                UploadSignatureForBlob(blobName, containerName, sig);

                return bytesUploaded;
            }
            else
            {
                // 4) If blob or signature does NOT exist, just upload as normal. No tricky stuff to do here.
                // 4.1) Generate signature and upload it.

                var fileLength = CommonOps.GetFileSize(localFilePath);

                var remainingBytes = new RemainingBytes()
                {
                    BeginOffset = 0,
                    EndOffset = fileLength - 1
                };

                // upload all bytes of new file. UploadBytes method will break into appropriate sized blocks.
                var allUploadedBlocks = UploadBytes(remainingBytes, localFilePath, containerName, blobName);
                var res = (from b in allUploadedBlocks orderby b.Offset ascending select b.BlockId);
                PutBlockList(res.ToArray(), containerName, blobName);
                
                var sig = CommonOps.CreateSignatureForLocalFile(localFilePath);
                UploadSignatureForBlob(blobName, containerName, sig);

                return fileLength;
            }
        }


        // updates blob if possible.
        // if blob doesn't already exist OR does not have a signature file 
        // then we just upload as usual.
        public long CalculateDeltaSizeFromLocalSig(string localSigPath, string localFilePath)
        {

            using (var fs = new FileStream(localSigPath, FileMode.Open))
            {
                var sig = SerializationHelper.ReadSizeBasedBinarySignature(fs);
                var searchResults = CommonOps.SearchLocalFileForSignatures(localFilePath, sig);

                long total = 0;
                foreach (var remainingBytes in searchResults.ByteRangesToUpload)
                {
                    total += (remainingBytes.EndOffset - remainingBytes.BeginOffset);

                }

                return total;
            }
        }

        // updates blob if possible.
        // if blob doesn't already exist OR does not have a signature file 
        // then we just upload as usual.
        public long CalculateDeltaSize(string containerName, string blobName, string localFilePath)
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

                long total = 0;
                foreach (var remainingBytes in searchResults.ByteRangesToUpload)
                {
                    total += (remainingBytes.EndOffset - remainingBytes.BeginOffset);
                    
                }

                return total;

            }
            else
            {
                var fileLength = CommonOps.GetFileSize(localFilePath);

                return fileLength - 1;
            }
        }

        private void PutBlockList(string[] blockIdArray, string containerName, string blobName)
        {
            var client = AzureHelper.GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            var blobIdList = blob.DownloadBlockList(BlockListingFilter.Committed);
            var overlap = (from b in blobIdList where blockIdArray.Contains(b.Name) select b).ToList();
            blob.PutBlockList(blockIdArray);
        }

        // Uploads differences between existing blob and updated local file.
        // Have local file to reference, the search results (indicating which parts need to be uploaded)
        // container and blob name.
        private long UploadDelta(string localFilePath, SignatureSearchResult searchResults, string containerName, string blobName)
        {
            var allUploadedBlocks = new List<UploadedBlock>();

            long bytesUploaded = 0;

            // loop through each section of the search results.
            // create blob from each RemainingBytes instances.
            // reuse the blocks already in use.
            foreach (var remainingBytes in searchResults.ByteRangesToUpload)
            {
                var uploadedBlockList = UploadBytes(remainingBytes, localFilePath, containerName, blobName);
                allUploadedBlocks.AddRange( uploadedBlockList);

                bytesUploaded += (remainingBytes.EndOffset - remainingBytes.BeginOffset);
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
            PutBlockList(res.ToArray(), containerName, blobName);

            return bytesUploaded;
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
        public long DownloadBlob(string containerName, string blobName, Stream stream)
        {
            var client = AzureHelper.GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var url = AzureHelper.GenerateUrl(containerName, blobName);
            var blobRef = client.GetBlobReferenceFromServer(new Uri(url));

            ReadBlockBlob(blobRef, stream);

            return stream.Length;

        }


        public long DownloadBlob(string containerName, string blobName, string localFilePath)
        {
            long bytesDownloaded = 0;

            if (CommonOps.DoesFileExist(localFilePath))
            {
                // local file exists.
                // 1) generate sig for local file.
                // 2) download sig for blob.

                var blobSig = DownloadSignatureForBlob(containerName, blobName);
                var localSig = CommonOps.CreateSignatureForLocalFile(localFilePath);
                var searchResults = CommonOps.SearchLocalFileForSignatures(localFilePath, blobSig);

                // we now have a list of which blocks are already in the local file (searchResults.SignaturesToReuse)
                // We need to then determine the byteranges which are NOT covered by these blocks
                // and download those.
                // Then we need to get the blocks that already exist in the local file, read those then write them to the new file.
                var byteRangesToDownload = GenerateByteRangesOfBlobToDownload(searchResults.SignaturesToReuse,blobSig,
                    containerName, blobName);

                RegenerateBlob(containerName, blobName, byteRangesToDownload, localFilePath, searchResults.SignaturesToReuse, blobSig);
                
                foreach(var byteRange in byteRangesToDownload)
                {
                    bytesDownloaded += byteRange.EndOffset - byteRange.BeginOffset;
                }

            }
            else
            {
                // download fresh copy.
                // get stream to store.
                using (var stream = CommonHelper.GetStream(localFilePath))
                {
                    bytesDownloaded = DownloadBlob(containerName, blobName, stream);
                }
            }

            return bytesDownloaded;
        }

        // regenerate blob locally.
        // we need to either download byte ranges from Azure.
        // OR
        // need to copy from local file.
        private void RegenerateBlob(string containerName, string blobName, List<RemainingBytes> byteRangesToDownload, string localFilePath, List<BlockSignature> reusableBlockSignatures, SizeBasedCompleteSignature blobSig )
        {
            // removing size from the equation.
            var allBlobSigs =
                blobSig.Signatures.Values.SelectMany(x => x.SignatureList).OrderBy(a => a.Offset).ToList();

            // LUT to see if block is to be reused or not.
            var reusableBlockDict = CommonOps.GenerateBlockDict(reusableBlockSignatures.ToArray());

            var offset = 0L;

            using (var localStream = new FileStream( localFilePath, FileMode.Open))
            using (var newStream = new FileStream( localFilePath+".new", FileMode.Create))
            {
                // go through all sigs in offset order....  determine if can reuse or need to download.
                foreach (var sig in allBlobSigs)
                {
                    var haveMatch = false;
                    if (reusableBlockDict.ContainsKey(sig.RollingSig))
                    {
                        // have a match... so will reuse local file.
                        var localSig = reusableBlockDict[sig.RollingSig];

                        var matchingLocalSigs =
                                localSig.Where(s => s.MD5Signature.SequenceEqual(sig.MD5Signature))
                                    .Select(n => n)
                                    .ToList();

                        if (matchingLocalSigs.Any())
                        {
                            // have a match.
                            var matchingLocalSig = matchingLocalSigs[0];
                            
                            // huge amount of wasted allocations...  maybe move this.
                            var buffer = new byte[matchingLocalSig.Size];

                            localStream.Seek(matchingLocalSig.Offset, SeekOrigin.Begin);
                            localStream.Read(buffer, 0, (int) matchingLocalSig.Size);

                            newStream.Seek(sig.Offset, SeekOrigin.Begin);
                            newStream.Write( buffer, 0, (int) matchingLocalSig.Size);

                            haveMatch = true;
                            offset += matchingLocalSig.Size;
                        }

                    }

                    if (!haveMatch)
                    {
                        // check if we have byte ranges starting at offset.
                        var byteRange =
                            (from b in byteRangesToDownload where b.BeginOffset == offset select b).FirstOrDefault();
                        if (byteRange != null)
                        {
                            // download bytes.
                            var blobBytes = DownloadBytes(containerName, blobName, byteRange.BeginOffset,
                                byteRange.EndOffset);

                            newStream.Seek(sig.Offset, SeekOrigin.Begin);
                            newStream.Write(blobBytes, 0, (int)(byteRange.EndOffset - byteRange.BeginOffset + 1));

                            offset += (byteRange.EndOffset - byteRange.BeginOffset + 1);
                        }
                    }
                }
            }

            // rename .new file to original
            File.Replace(localFilePath + ".new", localFilePath,null);
        }

        private byte[] DownloadBytes(string containerName, string blobName, long beginOffset, long endOffset)
        {
            var client = AzureHelper.GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blobRef = container.GetBlockBlobReference(blobName);

            var buffer = new byte[ endOffset - beginOffset +1];
            blobRef.DownloadRangeToByteArray(buffer, 0, beginOffset, endOffset - beginOffset + 1);

            return buffer;
        }

        private List<RemainingBytes> GenerateByteRangesOfBlobToDownload(List<BlockSignature> sigsToReuseList, SizeBasedCompleteSignature cloudBlobSig, string containerName, string blobName)
        {

            var blobSize = AzureHelper.GetBlobSize(containerName, blobName);
            var remainingBytesList = new List<RemainingBytes>();
            var allBlobSigs = cloudBlobSig.Signatures.Values.SelectMany(x => x.SignatureList).OrderBy(a => a.Offset).ToList();

            var sortedSigs = (from sig in sigsToReuseList orderby sig.Offset ascending select sig).ToList();


            // loop through all cloudBlobSigs.
            // If have a match in sigsToReuse, skip it.
            // otherwise, take note of offset and size to download.

            var count = 0;
            while (count < allBlobSigs.Count - 1)
            {
                // sig and next sig.
                var sig1 = allBlobSigs[count];
                var sig2 = allBlobSigs[count + 1];

                // check if sig is already in local file.
                var haveMatchingSig = sigsToReuseList.Any(s => s.MD5Signature == sig1.MD5Signature);
                if (!haveMatchingSig)
                {
                    remainingBytesList.Add(new RemainingBytes()
                       {
                           BeginOffset = sig1.Offset,
                           EndOffset = sig2.Offset - 1
                       });
                }
                count++;
            }

            var lastSig = allBlobSigs.Last();
            if (lastSig.Offset + lastSig.Size < blobSize)
            {
                remainingBytesList.Add(new RemainingBytes()
                {
                    BeginOffset = lastSig.Offset + lastSig.Size,
                    EndOffset = blobSize -1
                });
            }
            else if (lastSig.Offset + lastSig.Size == blobSize && lastSig.Offset == 0)
            {
                // This means that the last sig is the entire blob (offset == 0 and offset + size == blob size). Remaining bytes is just entire blob.
                // then just go from offset to blobSize
                remainingBytesList.Add(new RemainingBytes()
                {
                    BeginOffset = lastSig.Offset,
                    EndOffset = blobSize - 1
                });
            }
            return remainingBytesList;
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
