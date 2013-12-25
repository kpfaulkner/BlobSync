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
            if (args.Length == 3)
            {
                var fileName = args[0];
                var containerName = args[1];
                var blobName = args[2];

                var azureOps = new AzureOps();
                azureOps.UploadFile(containerName, blobName, fileName);

            }
            else
            {
                Console.WriteLine("blobsynccmd <local file path> <container> <blobname>");
            }


        }
    }
}
