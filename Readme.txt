BlobSync Nuget:

Rename the Dummy_App.config and add entries:

<appSettings>
    <add key="AzureAccountKey" value="Azure Account Key, please see the Azure Portal" />
    <add key="AzureAccountName" value="Azure Account Name" />
	<add key="SignatureSize" value="10000" />
</appSettings>

The assembly will then be ready to use. 


=================================

BlobSyncCmd:

Quick use scenario.

Add an App.config to the BlobSyncCmd project and add the entries:

<appSettings>
    <add key="AzureAccountKey" value="Azure Account Key, please see the Azure Portal" />
    <add key="AzureAccountName" value="Azure Account Name" />
	<add key="SignatureSize" value="100000" />
</appSettings>

Compile the solution then you'll have an example executable called BlobSyncCmd.
As an example usage of uploading files then uploading deltas, find a suitably large file (say test.txt) and issue the command (from command prompt):

blobsynccmd.exe upload test.txt mycontainer myblob

This will upload the file into a container called mycontainer and the blob will be called myblob. All rather unexciting (so far)

Now, modify (or make a copy and modify) test.txt. 

Reissue the same command as before:

blobsynccmd.exe upload test.txt mycontainer myblob

This time around, it has detected which parts of the file need to be uploaded and which can be reused from the previous version.
The granularity of what needs to be uploaded is based off the "SignatureSize" in the app.config. In this case we're dealing with 100k chunks.
This means that chunks of 100k in size will be replaced. Replacing 100k of a 10M file is far better than having to upload the entire 10M again if the 
change was small.

Please see blog post http://kpfaulkner.wordpress.com/2014/01/04/optimising-azure-blob-updating-part-1/ for more details.


