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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BlobSync.Datatypes;
using System.IO.MemoryMappedFiles;
using System.IO;
using BlobSync.Helpers;
using System.Security.Cryptography;

namespace BlobSync
{
    public class CommonOps
    {
        static MD5 md5Hash;

        static CommonOps()
        {
            md5Hash = MD5.Create();
        }

        public static SizeBasedCompleteSignature CreateSignatureForLocalFile(string localFilePath)
        {
            var sig = new SizeBasedCompleteSignature();

            var buffer = new byte[ConfigHelper.SignatureSize];
            var sigDict = new Dictionary<int, List<BlockSignature>>();

            using (var fs = new FileStream(localFilePath, FileMode.Open))
            {
                var offset = 0;
                uint idCount = 0;
                int bytesRead = 0;

                while ((bytesRead = fs.Read(buffer, 0, ConfigHelper.SignatureSize)) > 0)
                {
                    var blockSig = GenerateBlockSig(buffer, offset,bytesRead, idCount);
                    List<BlockSignature> sigList;
                    if (!sigDict.TryGetValue(bytesRead, out sigList))
                    {
                        sigList = new List<BlockSignature>();
                        sigDict[bytesRead] = sigList;
                    }

                    sigList.Add(blockSig);

                    offset += bytesRead;
                    idCount++;
                }

            }

            var sizedBaseSignature = new SizeBasedCompleteSignature();
            sizedBaseSignature.Signatures = new Dictionary<int, CompleteSignature>();

            foreach (var key in sigDict.Keys)
            {
                var compSig = new CompleteSignature() {SignatureList = sigDict[key].ToArray()};
                sizedBaseSignature.Signatures[key] = compSig;

            }

            return sizedBaseSignature;
        }

        public static long GetFileSize(string localFilePath)
        {
            var f = File.Open(localFilePath, FileMode.Open);
            var fileLength = f.Length;
            f.Close();

            return fileLength;
        }

        public static bool DoesFileExist(string localFilePath)
        {
            return File.Exists(localFilePath);
        }

        public static SignatureSearchResult SearchLocalFileForSignatures(string localFilePath, SizeBasedCompleteSignature sig)
        {
            var result = new SignatureSearchResult();

            // length of file.
            var tempFile = File.Open(localFilePath, FileMode.Open);
            var fileLength = tempFile.Length;
            tempFile.Close();

            var offset = 0;
            var windowSize = ConfigHelper.SignatureSize;
            var windowBuffer = new byte[windowSize];

            // signatures we can reuse.
            var signaturesToReuse = new List<BlockSignature>();

            // get sizes of signatures (block sizes) from existing sig.
            // then loop through all sizes looking for matches in local file.
            // important to search from largest to smallest.
            var signatureSizes = sig.Signatures.Keys.ToList();
            signatureSizes.Sort();
            signatureSizes.Reverse();

            // byte ranges that have not been matched to existing blocks yet.
            var remainingByteList = new List<RemainingBytes>();
            remainingByteList.Add(new RemainingBytes {BeginOffset = 0, EndOffset = fileLength - 1});

            // Create the memory-mapped file. 
            using (var mmf = MemoryMappedFile.CreateFromFile(localFilePath, FileMode.Open))
            {
                using (var accessor = mmf.CreateViewAccessor())
                {
                    // loop through each sig size.
                    foreach (var sigSize in signatureSizes)
                    {
                        var sigs = sig.Signatures[sigSize];
                        var newRemainingByteList = SearchLocalFileForSignaturesBasedOnSize(sigs, accessor, remainingByteList, sigSize, fileLength, signaturesToReuse);
                        remainingByteList = newRemainingByteList;
                    }
                }
            }

            result.ByteRangesToUpload = remainingByteList;
            result.SignaturesToReuse = signaturesToReuse;
            return result;
        }

        private static List<RemainingBytes> SearchLocalFileForSignaturesBasedOnSize(CompleteSignature sig, MemoryMappedViewAccessor accessor, List<RemainingBytes> remainingByteList, int sigSize, long fileSize, List<BlockSignature> signaturesToReuse)
        {
            var windowSize = sigSize;
            var newRemainingBytes = new List<RemainingBytes>();
            var sigDict = GenerateBlockDict(sig);
            var buffer = new byte[sigSize];
            var offset = 0L;
            foreach (var byteRange in remainingByteList)
            {
                // if byteRange is smaller than the key we're using, then there cannot be a match so add
                // it to the newRemainingBytes list
                if (byteRange.EndOffset - byteRange.BeginOffset + 1 >= windowSize)
                {
                    var byteRangeSize = byteRange.EndOffset - byteRange.BeginOffset + 1;
                    // search this byterange for all possible keys.
                    offset = byteRange.BeginOffset;
                    var generateFreshSig = true;
                    var bytesRead = 0L;
                    RollingSignature? currentSig = null;
                    long oldEndOffset = byteRange.BeginOffset;

                    do
                    {
                        if (generateFreshSig)
                        {
                            bytesRead = accessor.ReadArray(offset, buffer, 0, windowSize);
                            currentSig = CreateRollingSignature(buffer, (int)bytesRead);

                        }
                        else
                        {
                            // roll existing sig.
                            var previousByte = accessor.ReadByte(offset - 1);
                            var nextByte = accessor.ReadByte(offset + windowSize - 1);  // Need bounds checking?
                            currentSig = RollSignature(windowSize, previousByte, nextByte, currentSig.Value);
                        }

                        if (sigDict.ContainsKey(currentSig.Value))
                        {
                            // populate buffer. Potential waste of IO here.
                            bytesRead = accessor.ReadArray(offset, buffer, 0, windowSize);

                            // check md5 sig.
                            var md5Sig = CreateMD5Signature(buffer, (int) bytesRead);
                            var sigsForCurrentRollingSig = sigDict[currentSig.Value];

                            // have a matching md5? If so, we have a match.
                            var matchingSigs =
                                sigsForCurrentRollingSig.Where(s => s.MD5Signature.SequenceEqual(md5Sig))
                                    .Select(n => n)
                                    .ToList();

                            if (matchingSigs.Any())
                            {
                                // need to add any byte ranges between oldEndOffset and offset as bytes remaining (ie not part of any sig).
                                if (oldEndOffset != offset)
                                {
                                    newRemainingBytes.Add(new RemainingBytes()
                                    {
                                        BeginOffset = oldEndOffset,
                                        EndOffset = offset - 1
                                    });
                                    
                                }

                                var matchingSig = matchingSigs[0];

                                // when storing which existing sig to use, make sure we know the offset in the NEW file it should appear.
                                matchingSig.Offset = offset;
                                signaturesToReuse.Add(matchingSig);
                                offset += windowSize;
                                generateFreshSig = true;
                                oldEndOffset = offset;
                            }
                            else
                            {
                                offset++;
                                generateFreshSig = false;
                            }
                        }
                        else
                        {
                            // no match. Just increment offset and generate rolling sig.
                            offset++;
                            generateFreshSig = false;
                        }
                    } while (offset + windowSize < byteRangeSize);

                    // add remaining bytes to newRemainingBytes list
                    if (offset < byteRange.EndOffset)
                    {
                        newRemainingBytes.Add(new RemainingBytes()
                        {
                            BeginOffset = oldEndOffset,
                            EndOffset = byteRange.EndOffset
                        });
                    }
                }
                else
                {
                    newRemainingBytes.Add(byteRange);
                }
            }

            return newRemainingBytes;
        }

        internal static Dictionary<RollingSignature, List<BlockSignature>> GenerateBlockDict(CompleteSignature sig)
        {
            return GenerateBlockDict(sig.SignatureList);
        }


        // generates a dictionary with rolling sig as the key
        // BUT it assumes that the signatures passed as param are all of the same
        // signature size. 
        internal static Dictionary<RollingSignature, List<BlockSignature>> GenerateBlockDict(BlockSignature[] sigArray)
        {
            var blockDict = new Dictionary<RollingSignature, List<BlockSignature>>();

            List<BlockSignature> bsl;
            foreach (var element in sigArray)
            {
                if (blockDict.TryGetValue(element.RollingSig, out bsl))
                {
                    var addToList = false;
                    // loop through sigs that have particular rolling sig and check for matching md5.
                    foreach (var bs in bsl)
                    {
                        // if md5's are different then throw exception, if they're the same can keep proceeding.
                        if (!bs.MD5Signature.SequenceEqual(element.MD5Signature))
                        {
                            // sig already exists... can happen, but hopefully rare.
                            addToList = true;
                        }
                        else
                        {
                            // matching md5....  so dont add to list.
                        }
                    }

                    if (addToList)
                    {
                        bsl.Add(element);
                    }
                }
                else
                {
                    bsl = new List<BlockSignature>();
                    bsl.Add(element);
                    blockDict[element.RollingSig] = bsl;
                }
            }

            return blockDict;

        }


        internal static BlockSignature GenerateBlockSig(byte[] buffer, long offset, int blockSize, uint id)
        {
            var sig = new BlockSignature();

            var rollingSig = CreateRollingSignature(buffer, blockSize);
            var md5Sig = CreateMD5Signature(buffer, blockSize);
            sig.RollingSig = rollingSig;
            sig.MD5Signature = md5Sig;
            sig.Offset = offset;
            sig.BlockNumber = id;
            sig.Size = (uint) blockSize;

            return sig;
        }
        internal static byte[] CreateMD5Signature(byte[] byteBlock, int length)
        {
            var res = md5Hash.ComputeHash(byteBlock, 0, length);
            return res;
        }


        public static RollingSignature CreateRollingSignature(byte[] byteBlock, int length)
        {
            decimal s1 = 0;
            decimal s2 = 0;

            for (var i = 0; i < length; i++)
            {
                s1 += byteBlock[i];
            }

            for (int i = 0; i < length; i++)
            {
                s2 += (uint)(length - i) * byteBlock[i];
            }


            var signature = new RollingSignature() { Sig1 = s1, Sig2 = s2 };

            return signature;
        }

        internal static RollingSignature RollSignature(int length, byte previousByte, byte nextByte, RollingSignature existingSignature)
        {

            decimal s1 = 0;
            decimal s2 = 0;

            s1 = existingSignature.Sig1;
            s2 = existingSignature.Sig2;

            s1 = s1 - previousByte + nextByte;
            s2 = s2 - (previousByte * length) + s1;

            var res = new RollingSignature() { Sig1 = s1, Sig2 = s2 };
            return res;
        }


        /// <summary>
        /// Existing blocks + sigs are in searchResults
        /// new 
        /// </summary>
        /// <param name="bytesUploaded"></param>
        /// <returns></returns>
        internal static SizeBasedCompleteSignature CreateSignatureFromNewAndReusedBlocks(List<UploadedBlock> allBlocks)
        {
            var sigDict = new Dictionary<int, List<BlockSignature>>();

            List<BlockSignature> sigList;

            // new blocks
            foreach (var newBlock in allBlocks )
            {
                if (!sigDict.TryGetValue((int)newBlock.Sig.Size, out sigList))
                {
                    sigList = new List<BlockSignature>();
                    sigDict[(int) newBlock.Sig.Size] = sigList;
                }

                // add sig to the list.
                sigList.Add( newBlock.Sig);
            }

            var sizedBaseSignature = new SizeBasedCompleteSignature();
            sizedBaseSignature.Signatures = new Dictionary<int, CompleteSignature>();

            foreach (var key in sigDict.Keys)
            {
                var compSig = new CompleteSignature() {SignatureList = sigDict[key].ToArray()};
                sizedBaseSignature.Signatures[key] = compSig;

            }

            return sizedBaseSignature;
        }
    }
}
