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
using System.Collections.Concurrent;
using System.Diagnostics;

namespace BlobSync
{
    public class AzureOps : ICloudOps
    {
        
        // ugly lock.
        static object parallelLock = new object();

        // updates blob if possible.
        // if blob doesn't already exist OR does not have a signature file 
        // then we just upload as usual.
        public long UploadFile(string containerName, string blobName, string localFilePath, int parallelFactor=2)
        {
            var fileLength = CommonOps.GetFileSize(localFilePath);
            var sw = new Stopwatch();
            sw.Start();

            // 1) Does remote blob exist?
            // 2) if so, download existing signature for blob.
            if (AzureHelper.DoesBlobExist(containerName, blobName) && AzureHelper.DoesBlobSignatureExist(containerName, blobName))
            {
                var md5ForBlob = GetBlobMD5(containerName, blobName);
                var md5ForFile = GetFileMD5(localFilePath);

                // only continue if files are actually different.
                if (md5ForBlob != md5ForFile)
                {
                    // 3) If blob exists and have signature, then let the magic begin.
                    // 3.1) Download existing blob signature from Azure.
                    // 3.2) Search through local file for matches in existing blob signature.
                    // 3.3) Upload differences to Azure
                    // 3.4) Upload new signature.s

                    var blobSig = DownloadSignatureForBlob(containerName, blobName);
                    Console.WriteLine(string.Format("Dowloaded sig {0}ms", sw.ElapsedMilliseconds));

                    var searchResults = CommonOps.SearchLocalFileForSignatures(localFilePath, blobSig);

                    Console.WriteLine(string.Format("Searched for common {0}ms", sw.ElapsedMilliseconds));

                    var allBlocks = UploadDelta(localFilePath, searchResults, containerName, blobName, parallelFactor: parallelFactor);
                    var sig = CommonOps.CreateSignatureFromNewAndReusedBlocks(allBlocks);

                    UploadSignatureForBlob(blobName, containerName, sig);

                    // set md5 for entire blob
                    AzureHelper.SetBlobMD5(containerName, blobName, md5ForFile);

                    long bytesUploaded = allBlocks.Where(b => b.IsNew).Select(b => b.Size).Sum();

                    return bytesUploaded;
                }

                return 0;   // no bytes changed, no bytes uploaded
            }
            else
            {
                // 4) If blob or signature does NOT exist, just upload as normal. No tricky stuff to do here.
                // 4.1) Generate signature and upload it.

                var remainingBytes = new RemainingBytes()
                {
                    BeginOffset = 0,
                    EndOffset = fileLength - 1
                };

                var allUploadedBlocks = UploadBytesParallel(remainingBytes, localFilePath, containerName, blobName, parallelFactor: parallelFactor);  
                var res = (from b in allUploadedBlocks orderby b.Offset ascending select b.BlockId);
                PutBlockList(res.ToArray(), containerName, blobName);
                
                var sig = CommonOps.CreateSignatureForLocalFile(localFilePath);
                UploadSignatureForBlob(blobName, containerName, sig);

                return fileLength;
            }
        }

        private string GetFileMD5(string localFilePath)
        {
            var md5Hash = MD5.Create();
            byte[] hashByteArray;
            using(var fs = new FileStream( localFilePath, FileMode.Open))
            {
                hashByteArray = md5Hash.ComputeHash(fs);
            }

            return Convert.ToBase64String(hashByteArray);
        }

        /// <summary>
        /// Not currently used but DONT DELETE YET!!
        /// </summary>
        /// <param name="allUploadedBlocks"></param>
        private void FilterUploadedBlocks(List<UploadedBlock> allUploadedBlocks)
        {
            var blockDict = new Dictionary<long, UploadedBlock>();
            var newList = new List<UploadedBlock>();

            foreach( var block in allUploadedBlocks)
            {
                if (blockDict.ContainsKey( block.Offset))
                {
                    // error. this should not happen.

                }
                else
                {
                    newList.Add(block);
                }
            }

            allUploadedBlocks.Clear();
            allUploadedBlocks.AddRange(newList);
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
                    total += (remainingBytes.EndOffset - remainingBytes.BeginOffset +1);

                }

                return total;
            }
        }

        // updates blob if possible.
        // if blob doesn't already exist OR does not have a signature file 
        // then we just upload as usual.
        public SizeBasedCompleteSignature GenerateDeltaSigFromLocalResources(string localSigPath, string localFilePath)
        {

            using (var fs = new FileStream(localSigPath, FileMode.Open))
            {
                var sig = SerializationHelper.ReadSizeBasedBinarySignature(fs);
                var searchResults = CommonOps.SearchLocalFileForSignatures(localFilePath, sig);
                var allBlocks = UploadDelta(localFilePath, searchResults, null, null, true);

                var newSig = CommonOps.CreateSignatureFromNewAndReusedBlocks(allBlocks);
                

                return newSig;
            }
        }



        // updates blob if possible.
        // if blob doesn't already exist OR does not have a signature file 
        // then we just upload as usual.
        public long CalculateDeltaSize(string containerName, string blobName, string localFilePath)
        {
            // 1) Does remote blob exist?
            // 2) if so, download existing signature for blob.
            if ( !string.IsNullOrEmpty( blobName) && !string.IsNullOrEmpty( containerName) && AzureHelper.DoesBlobExist(containerName, blobName) && AzureHelper.DoesBlobSignatureExist(containerName, blobName))
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

                var remainingBytes = new RemainingBytes()
                {
                    BeginOffset = 0,
                    EndOffset = fileLength - 1
                };

                // upload all bytes of new file. UploadBytes method will break into appropriate sized blocks.
                var allUploadedBlocks = UploadBytes(remainingBytes, localFilePath, containerName, blobName, true);

                var sizeUploaded = allUploadedBlocks.Where(b => !b.IsDuplicate).Sum(b => b.Size);

                return sizeUploaded;
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
        private List<UploadedBlock> UploadDelta(string localFilePath, SignatureSearchResult searchResults, string containerName, string blobName, bool testMode = false, int parallelFactor=2)
        {
            var allUploadedBlocks = new List<UploadedBlock>();

            // loop through each section of the search results.
            // create blob from each RemainingBytes instances.
            // reuse the blocks already in use.
            foreach (var remainingBytes in searchResults.ByteRangesToUpload)
            {
                var uploadedBlockList = UploadBytesParallel(remainingBytes, localFilePath, containerName, blobName, testMode, parallelFactor);
                allUploadedBlocks.AddRange(uploadedBlockList);
            }

            // once we're here we should have uploaded ALL new data to Azure Blob Storage.
            // We then need to send the "construct" blob message.
            // loop through existing blocks and get offset + blockId's.
            foreach (var sig in searchResults.SignaturesToReuse)
            {
                if (sig.MD5Signature != null)
                {
                    var blockId = Convert.ToBase64String(sig.MD5Signature);
                    allUploadedBlocks.Add(new UploadedBlock() { BlockId = blockId, Offset = sig.Offset, Size = sig.Size, Sig = sig, IsNew = false });
                }
            }

            if (!testMode)
            {
                // needs to be sorted by offset so the final blob constructed is in correct order.
                var res = (from b in allUploadedBlocks orderby b.Offset ascending select b.BlockId);
                PutBlockList(res.ToArray(), containerName, blobName);
            }

            return allUploadedBlocks;
        }

        private List<UploadedBlock> UploadBytes(RemainingBytes remainingBytes, string localFilePath, string containerName, string blobName, bool testMode = false)
        {
            var uploadedBlockList = new List<UploadedBlock>();

            try
            {
                CloudBlockBlob blob = null;
                if (!testMode)
                {
                    var client = AzureHelper.GetCloudBlobClient();
                    var container = client.GetContainerReference(containerName);
                    container.CreateIfNotExists();
                    blob = container.GetBlockBlobReference(blobName);

                }
                var blockCount =
                    Math.Round((double)(remainingBytes.EndOffset - remainingBytes.BeginOffset + 1) /
                               (double)ConfigHelper.SignatureSize, MidpointRounding.AwayFromZero);

                using (var stream = new FileStream(localFilePath, FileMode.Open))
                {
                    for (var offset = remainingBytes.BeginOffset; offset <= remainingBytes.EndOffset; )
                    {
                        var sizeToRead = offset + ConfigHelper.SignatureSize <= remainingBytes.EndOffset
                            ? ConfigHelper.SignatureSize
                            : remainingBytes.EndOffset - offset + 1;

                        if (sizeToRead == 0)
                        {
                            var error = "";
                        }

                        // seek to the offset we need. Dont forget remaining bytes may be bigger than the signature size
                        // we want to deal with.
                        stream.Seek(offset, SeekOrigin.Begin);
                        var bytesToRead = new byte[sizeToRead];
                        var bytesRead = stream.Read(bytesToRead, 0, (int)sizeToRead);

                        var sig = CommonOps.GenerateBlockSig(bytesToRead, offset, (int)sizeToRead, 0);
                        var blockId = Convert.ToBase64String(sig.MD5Signature);
                        var isDupe = uploadedBlockList.Any(ub => ub.BlockId == blockId);
                        
                        if (!testMode)
                        {
                            // only upload bytes IF another block hasn't already covered it.
                            // unlikely situation I think, but possibly going to happen for 
                            // VM images etc where there is lots of "blank space".

                            if (!isDupe)
                            {
                                // yes, putting into memory stream is probably a waste here.
                                using (var ms = new MemoryStream(bytesToRead))
                                {
                                    var options = new BlobRequestOptions() { ServerTimeout = new TimeSpan(0, 90, 0) };
                                    blob.PutBlock(blockId, ms, null, null, options);
                                }
                            }
                        }

                        // store the block id that is associated with this byte range.
                        uploadedBlockList.Add(new UploadedBlock()
                        {
                            BlockId = blockId,
                            Offset = offset,
                            Sig = sig,
                            Size = bytesRead,
                            IsNew = true,
                            IsDuplicate = isDupe
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

        private List<UploadedBlock> UploadBytesParallel(RemainingBytes remainingBytes, string localFilePath, string containerName, string blobName, bool testMode=false, int parallelFactor = 2)
        {
            var uploadedBlockList = new ConcurrentBag<UploadedBlock>();
            try
            {
                CloudBlockBlob blob = null;
                if (!testMode)
                {
                    var client = AzureHelper.GetCloudBlobClient();
                    var container = client.GetContainerReference(containerName);
                    container.CreateIfNotExists();
                    blob = container.GetBlockBlobReference(blobName);

                }
                var blockCount =
                    Math.Round((double) (remainingBytes.EndOffset - remainingBytes.BeginOffset + 1)/
                               (double) ConfigHelper.SignatureSize, MidpointRounding.AwayFromZero);

                var taskList = new List<Task>();

                long offset = remainingBytes.BeginOffset;
                    
                using (var stream = new FileStream(localFilePath, FileMode.Open))
                {
                    while (offset <= remainingBytes.EndOffset)
                    {
                        while (offset <= remainingBytes.EndOffset && taskList.Count < parallelFactor)
                        {
                            var sizeToRead = offset + ConfigHelper.SignatureSize <= remainingBytes.EndOffset
                                ? ConfigHelper.SignatureSize
                                : remainingBytes.EndOffset - offset + 1;


                            if (sizeToRead > 0)
                            {
                                // seek to the offset we need. Dont forget remaining bytes may be bigger than the signature size
                                // we want to deal with.
                                stream.Seek(offset, SeekOrigin.Begin);
                                var bytesToRead = new byte[sizeToRead];
                                var bytesRead = stream.Read(bytesToRead, 0, (int)sizeToRead);

                                var t = WriteBytes(offset, bytesRead, bytesToRead, blob, uploadedBlockList, testMode);

                                taskList.Add(t);

                                offset += sizeToRead;
                            }
                        }

                        // wait until we've all uploaded.
                        var waitedIndex = Task.WaitAny(taskList.ToArray());
                        taskList.RemoveAt(waitedIndex);

                    }

                    // wait on remaining tasks.
                    Task.WaitAll(taskList.ToArray());
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

            return uploadedBlockList.ToList();
            //return uploadedBlockList;
        }

        /// <summary>
        /// Yes, copying the byte array to here. But given we'll not have many of these tasks going to parallel
        /// and each byte array is AT MOST 4M, I think I can live with the memory overhead.
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="bytesRead"></param>
        /// <param name="bytesToRead"></param>
        /// <param name="blob"></param>
        /// <param name="uploadedBlockList"></param>
        /// <param name="testMode"></param>
        /// <returns></returns>
        private Task WriteBytes(long offset, int bytesRead, byte[] bytesToRead, CloudBlockBlob blob, ConcurrentBag<UploadedBlock> uploadedBlockList, bool testMode)
        {

            var t = Task.Factory.StartNew(() =>
                {
                    var sig = CommonOps.GenerateBlockSig(bytesToRead, offset, (int)bytesRead, 0);
                    var blockId = Convert.ToBase64String(sig.MD5Signature);

                    bool isDupe = false;
                    lock (parallelLock)
                    {

                        isDupe = uploadedBlockList.Any(ub => ub.BlockId == blockId);

                        // store the block id that is associated with this byte range.
                        uploadedBlockList.Add(new UploadedBlock()
                        {
                            BlockId = blockId,
                            Offset = offset,
                            Sig = sig,
                            Size = bytesRead,
                            IsNew = true,
                            IsDuplicate = isDupe
                        });

                    }

                    if (!testMode)
                    {
                        if (!isDupe)
                        {
                            // yes, putting into memory stream is probably a waste here.
                            using (var ms = new MemoryStream(bytesToRead))
                            {
                                var options = new BlobRequestOptions() { ServerTimeout = new TimeSpan(0, 90, 0) };
                                blob.PutBlock(blockId, ms, null, null, options);

                            }
                        }
                    }
              });

            return t;
        }

        private string GetBlobMD5(string containerName, string blobName)
        {
            var client = AzureHelper.GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var url = AzureHelper.GenerateUrl(containerName, blobName);
            var blobRef = client.GetBlobReferenceFromServer(new Uri(url));
            blobRef.FetchAttributes();
            return blobRef.Properties.ContentMD5;
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

        // download blob to stream
        public long DownloadBlob(string containerName, string blobName, Stream stream, int parallelFactor = 2)
        {
            var client = AzureHelper.GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var url = AzureHelper.GenerateUrl(containerName, blobName);
            var blobRef = client.GetBlobReferenceFromServer(new Uri(url));

            ReadBlockBlob(blobRef, stream, parallelFactor);

            return stream.Length;

        }


        public long DownloadBlob(string containerName, string blobName, string localFilePath, int parallelFactor = 2)
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

                RegenerateBlob(containerName, blobName, byteRangesToDownload, localFilePath, searchResults.SignaturesToReuse, blobSig, parallelFactor);
                
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
                    bytesDownloaded = DownloadBlob(containerName, blobName, stream, parallelFactor);
                }
            }

            return bytesDownloaded;
        }

        // regenerate blob locally.
        // we need to either download byte ranges from Azure.
        // OR
        // need to copy from local file.
        private void RegenerateBlob(string containerName, string blobName, List<RemainingBytes> byteRangesToDownload, string localFilePath, List<BlockSignature> reusableBlockSignatures, SizeBasedCompleteSignature blobSig, int parallelFactor = 2)
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
                                byteRange.EndOffset, parallelFactor);

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

        private byte[] DownloadBytes(string containerName, string blobName, long beginOffset, long endOffset, int parallelFactor=2)
        {
            var client = AzureHelper.GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blobRef = container.GetBlockBlobReference(blobName);

            var buffer = new byte[ endOffset - beginOffset +1];
            blobRef.DownloadRangeToByteArray(buffer, 0, beginOffset, endOffset - beginOffset + 1, options: new BlobRequestOptions { ParallelOperationThreadCount = parallelFactor });

            return buffer;
        }

        private List<RemainingBytes> GenerateByteRangesOfBlobToDownload(List<BlockSignature> sigsToReuseList, SizeBasedCompleteSignature cloudBlobSig, string containerName, string blobName)
        {

            var blobSize = AzureHelper.GetBlobSize(containerName, blobName);
            var remainingBytesList = new List<RemainingBytes>();
            var allBlobSigs = cloudBlobSig.Signatures.Values.SelectMany(x => x.SignatureList).OrderBy(a => a.Offset).ToList();

            var sortedSigs = (from sig in sigsToReuseList orderby sig.Offset ascending select sig).ToList();

            long startOffsetToCopy = 0;

            // loop through all cloudBlobSigs.
            // If have a match in sigsToReuse, skip it.
            // otherwise, take note of offset and size to download.
            foreach( var sig in allBlobSigs)
            {
                var haveMatchingSig = sigsToReuseList.Any(s => s.MD5Signature.SequenceEqual(sig.MD5Signature));
                if (!haveMatchingSig)
                {
                    // if no match then we need to copy everything from startOffsetToCopy to sig.Offset + sig.Size
                    remainingBytesList.Add(new RemainingBytes()
                    {
                        BeginOffset = startOffsetToCopy,
                        EndOffset = sig.Offset + sig.Size -1
                    });
                    startOffsetToCopy = sig.Offset + sig.Size;
                }
                else
                {
                    // we have a match therefore dont need to copy the data.
                    // change startOffsetToCopy to just after current sig.
                    startOffsetToCopy = sig.Offset + sig.Size;
                }
            }

            return remainingBytesList;
        }

        private void ReadBlockBlob(ICloudBlob blobRef, Stream stream, int parallelFactor=2)
        {
            var blockBlob = blobRef as CloudBlockBlob;

            // no parallel yet.
            blockBlob.DownloadToStream(stream, options: new BlobRequestOptions { ParallelOperationThreadCount = parallelFactor });
        }

        public void GetBlockListInfo(string containerName, string blobName)
        {
            var client = AzureHelper.GetCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            var blobIdList = blob.DownloadBlockList(BlockListingFilter.Committed);
            var all = blob.DownloadBlockList(BlockListingFilter.All).ToList();
            foreach( var i in all)
            {
                Console.WriteLine(string.Format("{0}:{1}:{2}", i.Name, i.Length, i.Committed));
            }
        }

        /// <summary>
        /// Merge smaller blocks into something at least fragmentMergeSize bytes long.
        /// Only upload at most maxUploadLimit (0 == no limit).
        /// Should this be in CommonOps?
        /// Lame... really? DEFRAG? Then again I suppose the term IS appropriate.
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        public void DefragBlob(string containerName, string blobName, long maxUploadLimitMB = 2)
        {
            var blobSig = DownloadSignatureForBlob(containerName, blobName);
            DefragBlob(blobSig, maxUploadLimitMB);
        }

        /// <summary>
        /// Merge smaller blocks into something at least fragmentMergeSize bytes long.
        /// Only upload at most maxUploadLimit (0 == no limit).
        /// Should this be in CommonOps?
        /// Lame... really? DEFRAG? Then again I suppose the term IS appropriate.
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        public void DefragBlob(string filePath, long maxUploadLimitMB = 2)
        {
            using (var fs = new FileStream(filePath, FileMode.Open))
            {
                var blobSig = SerializationHelper.ReadSizeBasedBinarySignature(fs);
                DefragBlob(blobSig, maxUploadLimitMB);               
            }
        }

        /// <summary>
        /// Merge smaller blocks into something at least fragmentMergeSize bytes long.
        /// Only upload at most maxUploadLimit (0 == no limit).
        /// Should this be in CommonOps?
        /// Lame... really? DEFRAG? Then again I suppose the term IS appropriate.
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        public void DefragBlob(SizeBasedCompleteSignature blobSig, long maxUploadLimitMB = 2)
        {
            var allBlobSigs = blobSig.Signatures.Values.SelectMany(x => x.SignatureList).OrderBy(a => a.Offset).ToList();

            var targetSigSize = ConfigHelper.SignatureSize;

            // loop through sigs, merge what we can but dont exceed maxUploadLimit
            long bytesToUpload = 0;
            var byteRangesToUpload = new List<RemainingBytes>();
            var defragNodeList = new List<DefragNode>();
            for (var i = 0; i < allBlobSigs.Count; i++)
            {
                uint sigSize = 0;
                var j = i;

                while (j < allBlobSigs.Count)
                {
                    var sig = allBlobSigs[j];
                    j++;

                    // break if we get too big.
                    if (sigSize + sig.Size > targetSigSize)
                    {
                        break;
                    }

                    sigSize += sig.Size;

                }

                defragNodeList.Add(new DefragNode { Offset = allBlobSigs[i].Offset, Size = sigSize, SigPos = i, NoSigs = j - i - 1 });
            }

            // defragNodeList is a list of sigs, and size. These ones will be merged.
            var sortedList = defragNodeList.OrderByDescending(n => n.NoSigs).ToList();


            // the entries in defragNodeList that has the max number of sigs in it (ie most fragmentation) will be the ones to get merged.
            foreach( var sig in sortedList)
            {
                DefragSigGroup(blobSig, sig);
                bytesToUpload += sig.Size;

                if (bytesToUpload > maxUploadLimitMB)
                {
                    break;
                }
            }


        }

        // defrags a group of sigs... merges them together.
        private void DefragSigGroup(SizeBasedCompleteSignature blobSig, DefragNode sig)
        {
            
        }
    }
}
