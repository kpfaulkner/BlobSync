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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobSync.Datatypes
{
    [Serializable]
    public struct RollingSignature
    {
        public decimal Sig1 { get; set; }
        public decimal Sig2 { get; set; }
    }

    [Serializable]
    public struct BlockSignature
    {
        public long Offset { get; set; }

        // unsure if we really need this.
        public UInt32 Size { get; set; }

        //public UInt32 RollingSignature { get; set; }
        public RollingSignature RollingSig { get; set; }

        public byte[] MD5Signature { get; set; }
        // this is block number.
        public UInt32 BlockNumber { get; set; }
    }

    [Serializable]
    public struct CompleteSignature
    {
        public BlockSignature[] SignatureList { get; set; }
    }

    [Serializable]
    public struct SizeBasedCompleteSignature
    {
        // key is size of the rolling sig used to generate the signature.
        // this way can perform multiple comparisons based on size.
        public Dictionary<int, CompleteSignature> Signatures { get; set; }
    }

    internal class RemainingBytes
    {
        /// <summary>
        /// Start offset. Inclusive.
        /// </summary>
        public long BeginOffset { get; set; }

        /// <summary>
        /// End offset. Inclusive.
        /// </summary>
        public long EndOffset { get; set; }
    }

}
