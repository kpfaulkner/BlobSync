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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobSync
{
    // cloud specific ops.
    // ie one implementation for Azure, one for S3 etc etc etc.
    interface ICloudOps
    {
        SizeBasedCompleteSignature DownloadSignatureForBlob(string container, string blobName);

        void UploadSignatureForBlob(SizeBasedCompleteSignature sig);

        void UploadFile(string localFilePath, string container);

        Blob DownloadBlob(string container, string blobName);

        void UpdateRemoteBlobFromLocalFile(string container, string blobName, string localFilePath);
        Blob UpdateLocalFileFromRemoteBlob(string container, string blobName, string localFilePath);

    }
}
