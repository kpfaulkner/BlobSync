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
using System.Diagnostics;

namespace BlobSyncCmd
{
    class Program
    {
        static void ShowExamples()
        {
            Console.WriteLine("blobsynccmd upload/download/estimate/estimatelocal/createsig <local file path> <container> <blobname>");
            Console.WriteLine("\n\neg.");
            Console.WriteLine("Upload a file to Azure Blob Storage: blobsynccmd upload c:\\temp\\myfile.txt mycontainer myblobname\n");
            Console.WriteLine("Download a file from Azure Blob Storage: blobsynccmd download c:\\temp\\destinationfilename.txt mycontainer myblobname\n");
            Console.WriteLine("Download a file from Azure Blob Storage: blobsynccmd download c:\\temp\\destinationfilename.txt mycontainer myblobname\n");
            Console.WriteLine("Estimate bytes to upload to update a file: blobsynccmd estimate c:\\temp\\newfile.txt mycontainer existingblobname\n");
            Console.WriteLine("Estimate bytes to upload based on a local signature: blobsynccmd estimatelocal c:\\temp\\newfile.txt c:\\temp\\sigforoldfile\n");
            Console.WriteLine("Generate signature for local file: blobsynccmd createsig c:\\temp\\file.txt\n");
            Console.WriteLine("Generate new signature based off existing (old) sig and new local file: blobsynccmd createdeltasig c:\\temp\\newfile.txt c:\\temp\\sigforoldfile\n");
            Console.WriteLine("Show offset/size contents of signature: blobsynccmd showsig c:\\temp\\sigfile\n");

        }


        static void Main(string[] args)
        {
            string command;
            string fileName;

            var sw = new Stopwatch();
            sw.Start();

            if (args.Length == 4)
            {
                command = args[0];
                fileName = args[1];
                var containerName = args[2];
                var blobName = args[3];
                var azureOps = new AzureOps();

                switch (command)
                {
                    case "upload":
                        var bytesUploaded = azureOps.UploadFile(containerName, blobName, fileName);
                        Console.WriteLine("Uploaded {0} bytes", bytesUploaded);
                        break;
                    case "download":
                        var bytesDownloaded = azureOps.DownloadBlob(containerName, blobName, fileName);
                        Console.WriteLine("Downloaded {0} bytes", bytesDownloaded);
                        break;
                    case "estimate":
                        var estimate = azureOps.CalculateDeltaSize(containerName, blobName, fileName);
                        Console.WriteLine( string.Format("Estimate to upload {0} bytes", estimate));
                        break;
                    default:
                        ShowExamples();
                        break;
                }
            }
            else
            if (args.Length == 3)
            {
                command = args[0];
                fileName = args[1];
                var localSigPath = args[2];
                var azureOps = new AzureOps();

                switch (command)
                {
                    case "estimatelocal":
                        var estimatelocal = azureOps.CalculateDeltaSizeFromLocalSig(localSigPath, fileName);
                        Console.WriteLine(string.Format("Estimate to upload {0} bytes", estimatelocal));
                        break;
                    case "createdeltasig":
                        var sig = azureOps.GenerateDeltaSigFromLocalResources(localSigPath, fileName);
                        
                        var sigFileName = fileName + ".sig";
                        using (Stream s = new FileStream(sigFileName, FileMode.Create))
                        {
                            SerializationHelper.WriteBinarySizedBasedSignature(sig, s);
                        }
                        break;
                    case "showblocklist":

                        azureOps.GetBlockListInfo(args[1], args[2]);
                        break;

                    default:
                        ShowExamples();
                        break;
                }
            }
            else
            if (args.Length == 2)
            {
                command = args[0];
                fileName = args[1];

                switch (command)
                {
                    case "createsig":
                        var sig = CommonOps.CreateSignatureForLocalFile(fileName);

                        var sigFileName = fileName + ".sig";
                        using (Stream s = new FileStream(sigFileName, FileMode.Create))
                        {
                            SerializationHelper.WriteBinarySizedBasedSignature(sig, s);
                        }

                        break;
                    case "showsig":
                        using (var fs = new FileStream(fileName, FileMode.Open))
                        {
                            var loadedSig = SerializationHelper.ReadSizeBasedBinarySignature(fs);
                            
                            foreach( var sigSize in loadedSig.Signatures)
                            {
                                foreach( var s in sigSize.Value.SignatureList.OrderBy( s => s.Offset))
                                {
                                    Console.WriteLine(string.Format("{0}:{1}", s.Offset, s.Size));

                                }
                            }
                        }
                        break;
                   
                    default:
                        ShowExamples();
                          break;
                }
                
            }
            else
            {
                ShowExamples();

            }

            sw.Stop();
            Console.WriteLine("Took {0}s", (double)sw.ElapsedMilliseconds / 1000.0);

        }

       
    }
}
