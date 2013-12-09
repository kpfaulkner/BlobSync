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
            throw new NotImplementedException();
        }
    }
}
