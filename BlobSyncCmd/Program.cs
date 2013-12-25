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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BlobSync;
using BlobSync.Helpers;

namespace BlobSyncCmd
{
    class Program
    {
        static void Main(string[] args)
        {
            var command = args[0];

            if (args.Length == 4)
            {
                var fileName = args[1];
                var containerName = args[2];
                var blobName = args[3];

                var azureOps = new AzureOps();

                switch (command)
                {
                    case "upload":

                        azureOps.UploadFile( containerName, blobName, fileName);

                        var sigFile = fileName + ".sig";

                        // create sig.
                        var sig = CommonOps.CreateSignatureForLocalFile(fileName);

                        var client = AzureHelper.GetCloudBlobClient();
                        var container = client.GetContainerReference(containerName);
                        
                        // upload file/blob
                        var blob = container.GetBlockBlobReference(blobName);
                        blob.UploadFromFile(fileName, FileMode.Open);

                        // upload sig.
                        var sigBlobName = blobName + ".sig";
                        var sigBlob = container.GetBlockBlobReference(sigBlobName);

                        using (Stream s = new MemoryStream())
                        {
                            SerializationHelper.WriteBinarySizedBasedSignature(sig, s);
                            s.Seek(0, SeekOrigin.Begin);
                            sigBlob.UploadFromStream(s);
                        }

                        break;

                    case "update":
                        break;

                    default:
                        Console.WriteLine("blobsynccmd update/upload <local file path> <container> <blobname>");
                        break;
                }

            }
            else
            {
                Console.WriteLine("blobsynccmd update/upload <local file path> <container> <blobname>");
            }


        }
    }
}
