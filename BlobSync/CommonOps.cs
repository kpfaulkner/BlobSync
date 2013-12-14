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
    class CommonOps
    {
        static MD5 md5Hash;

        static CommonOps()
        {
            md5Hash = MD5.Create();
        }

        public static SizeBasedCompleteSignature CreateSignatureForLocalFile(string localFilePath)
        {
            var sig = new SizeBasedCompleteSignature();

            return sig;
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
            var signatureSizes = sig.Signatures.Keys.ToList();
            signatureSizes.Sort();

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
                        SearchLocalFileForSignaturesBasedOnSize(sigs, accessor, remainingByteList, sigSize, fileLength, signaturesToReuse);
                    }

                    var bytesRead = accessor.ReadArray(offset, windowBuffer, 0, windowSize);
                    if (bytesRead != windowSize)
                    {
                        windowSize = bytesRead;
                    }

                    while (offset + windowSize < fileLength)
                    {
                        
                    }

                }
            }

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

                    do
                    {
                        if (generateFreshSig)
                        {
                            bytesRead = accessor.ReadArray(offset, buffer, 0, windowSize);
                            currentSig = CreateRollingSignature(buffer);

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
                            // populate buffer.
                            bytesRead = accessor.ReadArray(offset, buffer, 0, windowSize);

                            // check md5 sig.
                            var md5Sig = CreateMD5Signature(buffer);
                            var sigsForCurrentRollingSig = sigDict[currentSig.Value];

                            // have a matching md5? If so, we have a match.
                            var matchingSigs =
                                sigsForCurrentRollingSig.Where(s => s.MD5Signature.SequenceEqual(md5Sig))
                                    .Select(n => n)
                                    .ToList();

                            if (matchingSigs.Any())
                            {
                                var matchingSig = matchingSigs[0];

                                // when storing which existing sig to use, make sure we know the offset in the NEW file it should appear.
                                matchingSig.Offset = offset;
                                signaturesToReuse.Add(matchingSig);
                                offset += windowSize;
                                generateFreshSig = true;
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
                            BeginOffset = offset,
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

        // generates a dictionary with rolling sig as the key
        // BUT it assumes that the signatures passed as param are all of the same
        // signature size. 
        internal static Dictionary<RollingSignature, List<BlockSignature>> GenerateBlockDict(CompleteSignature sig)
        {
            var blockDict = new Dictionary<RollingSignature, List<BlockSignature>>();

            List<BlockSignature> bsl;
            foreach (var element in sig.SignatureList)
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


        internal static BlockSignature GenerateBlockSig(byte[] buffer, long offset, uint id)
        {
            var sig = new BlockSignature();

            var rollingSig = CreateRollingSignature(buffer);
            var md5Sig = CreateMD5Signature(buffer);
            sig.RollingSig = rollingSig;
            sig.MD5Signature = md5Sig;
            sig.Offset = offset;
            sig.BlockNumber = id;
            sig.Size = (uint)buffer.Length;

            return sig;
        }
        internal static byte[] CreateMD5Signature(byte[] byteBlock)
        {
            var res = md5Hash.ComputeHash(byteBlock);

            return res;
        }


        public static RollingSignature CreateRollingSignature(byte[] byteBlock)
        {
            var length = (uint)byteBlock.Length;

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


    }
}
