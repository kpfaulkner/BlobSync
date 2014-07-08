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

using BlobSync.Datatypes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobSync.Helpers
{
    public class SerializationHelper
    {
        /// <summary>
        /// Gets SizeBasedCompleteSignature.
        /// Format is: first 4 bytes are number of CompleteSig's there are. 
        /// For each complete sig, the format is 4 bytes, number of entries. 
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public static SizeBasedCompleteSignature ReadSizeBasedBinarySignature(Stream s)
        {
            var sig = new SizeBasedCompleteSignature();
            sig.Signatures = new Dictionary<int, CompleteSignature>();

            // always go to beginning of stream.
            s.Seek(0, SeekOrigin.Begin);

            var reader = new BinaryReader(s);
            int numberOfCompleteSignatures = reader.ReadInt32();

            for (var i = 0; i < numberOfCompleteSignatures; i++)
            {
                int keySize = reader.ReadInt32();

                var completeSig = ReadBinaryCompleteSignature(s);
                sig.Signatures[keySize] = completeSig;

            }

            return sig;

        }

        // very manual writer... but want to see how small I can get the data.
        public static CompleteSignature ReadBinaryCompleteSignature(Stream s)
        {
            var sig = new CompleteSignature();

            var l = new List<BlockSignature>();

            var reader = new BinaryReader(s);

            int numberOfEntries = reader.ReadInt32();

            for (var i = 0; i < numberOfEntries; i++)
            {
                var entry = new BlockSignature();

                // 8 bytes. offset
                long offset = reader.ReadInt64();

                // 4 bytes. size
                int size = reader.ReadInt32();

                // 4 bytes. Block Number;
                int blockNumber = reader.ReadInt32();

                // 4 bytes. Rolling Signature.
                decimal sig1 = reader.ReadDecimal();
                decimal sig2 = reader.ReadDecimal();
                RollingSignature rollingSig = new RollingSignature() { Sig1 = sig1, Sig2 = sig2 };

                // should be 16 bytes.
                byte[] md5 = reader.ReadBytes(16);

                entry.BlockNumber = (UInt32)blockNumber;
                entry.RollingSig = (RollingSignature)rollingSig;
                entry.MD5Signature = md5;
                entry.Offset = offset;
                entry.Size = (uint)size;

                l.Add(entry);
            }
            sig.SignatureList = l.ToArray<BlockSignature>();
            return sig;

        }


        public static void WriteBinarySizedBasedSignature(SizeBasedCompleteSignature sig, Stream s)
        {
            var writer = new BinaryWriter(s);

            int numberOfSizes = sig.Signatures.Keys.Count;

            // 4 bytes. Number of key sizes.
            writer.Write(numberOfSizes);

            foreach (int keySize in sig.Signatures.Keys)
            {
                // write key size.
                writer.Write(keySize);
                var completeSigForKeySize = sig.Signatures[keySize];

                int numberOfEntries = completeSigForKeySize.SignatureList.Length;

                // number of entries for this key size.
                writer.Write(numberOfEntries);

                foreach (var i in completeSigForKeySize.SignatureList)
                {

                    // 8 bytes
                    writer.Write(i.Offset);

                    // 4 bytes
                    writer.Write(i.Size);

                    // 4 bytes.
                    writer.Write(i.BlockNumber);

                    // 8 bytes.
                    writer.Write(i.RollingSig.Sig1);

                    // 8 bytes.
                    writer.Write(i.RollingSig.Sig2);


                    // should be 16 bytes.
                    foreach (byte b in i.MD5Signature)
                    {
                        writer.Write(b);
                    }
                }
            }
        }
    }
}
