using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BlobSync.Datatypes;

namespace BlobSync
{
    class CommonOps
    {
        public static CompleteSignature CreateSignatureForLocalFile(string localFilePath)
        {
            var sig = new CompleteSignature();

            return sig;
        }

        public static SignatureSearchResult SearchLocalFileForSignatures(string localFilePath, CompleteSignature sig)
        {
            // Create the memory-mapped file. 
            using (var mmf = MemoryMappedFile.CreateFromFile(@"c:\ExtremelyLargeImage.data", FileMode.Open, "ImgA"))
            {
                // Create a random access view, from the 256th megabyte (the offset) 
                // to the 768th megabyte (the offset plus length). 
                using (var accessor = mmf.CreateViewAccessor(offset, length))
                {
                    int colorSize = Marshal.SizeOf(typeof(MyColor));
                    MyColor color;

                    // Make changes to the view. 
                    for (long i = 0; i < length; i += colorSize)
                    {
                        accessor.Read(i, out color);
                        color.Brighten(10);
                        accessor.Write(i, ref color);
                    }
                }
            }

            throw new NotImplementedException();
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
