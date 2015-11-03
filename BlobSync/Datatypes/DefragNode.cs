using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobSync.Datatypes
{
    class DefragNode
    {
        public long Offset { get; set; }
        public uint Size { get; set; }

        // position in signature. 1st, 2nd etc.
        public int SigPos { get; set; }

        public int NoSigs { get; set; }

    }
}
