﻿//-----------------------------------------------------------------------
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
using System.ComponentModel;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace BlobSync.Helpers
{
    // reads the app.config for us.

    public static class ConfigHelper
    {
        // default values read from config.
        public static string AzureAccountKey { get; set; }
        public static string AzureAccountName { get; set; }

        public static string IsDev { get; set; }

        // retry attempt details
        public static int RetryAttemptDelayInSeconds {get;set;}
        public static int MaxRetryAttempts { get; set; }

        // misc params
        public static string DownloadDirectory  { get; set; }
        public static bool Verbose  { get; set; }
        public static bool AmDownloading  { get; set; }
        public static bool UseBlobCopy  { get; set; }
        public static bool ListContainer  { get; set; }
        public static bool MonitorBlobCopy  { get; set; }
        public static int ParallelFactor  { get; set; }
        public static int ChunkSizeInMB  { get; set; }

        // for any scenario where we need to create a public signature (SAS for Azure, something else for S3)
        // we want a time limit on how long that signature is valid
        public static int SharedAccessSignatureDurationInSeconds { get; set; }

        static ConfigHelper()
        {
            ReadConfig();
        }


        private static  T GetConfigValue<T>(string key, T defaultValue)
        {
            if (ConfigurationManager.AppSettings.AllKeys.Contains(key))
            {
                TypeConverter converter = TypeDescriptor.GetConverter(typeof(T));
                return (T)converter.ConvertFromString(ConfigurationManager.AppSettings.Get(key));
            }
            return defaultValue;
        }

        private static void SetConfigValue(Configuration config, string key, string val)
        {
            if ( config.AppSettings.Settings.AllKeys.Contains(key))
            {
                config.AppSettings.Settings[key].Value = val;
            }
            else
            {
                config.AppSettings.Settings.Add( new KeyValueConfigurationElement( key, val));
            }

        }

        // populates src and target values IF there is a default set.
        public static void ReadConfig()
        {
            AzureAccountKey = GetConfigValue<string>("AzureAccountKey", "");
            AzureAccountName = GetConfigValue<string>("AzureAccountNames", "");
            IsDev = GetConfigValue<string>("IsDev", "");
           
            // retry policies.
            // can be used in both Azure and AWS (eventually).
            RetryAttemptDelayInSeconds = GetConfigValue<int>("RetryAttemptDelayInSeconds", 2);
            MaxRetryAttempts = GetConfigValue<int>("MaxRetryAttempts", 10);


            DownloadDirectory = GetConfigValue<string>("DownloadDirectory", "c:\\temp");
            Verbose = GetConfigValue<bool>("Verbose", false);
            AmDownloading = GetConfigValue<bool>("AmDownloading", false);
            UseBlobCopy = GetConfigValue<bool>("UseBlobCopy", false);
            ListContainer = GetConfigValue<bool>("ListContainer", false);
            MonitorBlobCopy = GetConfigValue<bool>("MonitorBlobCopy", false);
            ParallelFactor = GetConfigValue<int>("ParallelFactor", 1);
            ChunkSizeInMB = GetConfigValue<int>("ChunkSizeInMB", 2);

            // SAS timeout
            SharedAccessSignatureDurationInSeconds = GetConfigValue<int>("SharedAccessSignatureDurationInSeconds", 600);
        }

        public static void SaveConfig()
        {
            Configuration config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
            SetConfigValue(config, "AzureAccountKey",AzureAccountKey);
            SetConfigValue(config, "AzureAccountName", AzureAccountName);
            SetConfigValue(config, "IsDev", IsDev);
          
            SetConfigValue(config, "RetryAttemptDelayInSeconds",RetryAttemptDelayInSeconds.ToString());
            SetConfigValue(config, "MaxRetryAttempts",MaxRetryAttempts.ToString());
            SetConfigValue(config, "DownloadDirectory",DownloadDirectory);
            SetConfigValue(config, "Verbose",Verbose.ToString());
            SetConfigValue(config, "AmDownloading",AmDownloading.ToString());
            SetConfigValue(config, "UseBlobCopy",UseBlobCopy.ToString()) ;
            SetConfigValue(config, "ListContainer",ListContainer.ToString());
            SetConfigValue(config, "MonitorBlobCopy",MonitorBlobCopy.ToString());
            SetConfigValue(config, "ParallelFactor",ParallelFactor.ToString());
            SetConfigValue(config, "ChunkSizeInMB",ChunkSizeInMB.ToString());
          
            SetConfigValue(config, "SharedAccessSignatureDurationInSeconds", SharedAccessSignatureDurationInSeconds.ToString());
            

            config.Save(ConfigurationSaveMode.Modified);
            ConfigurationManager.RefreshSection("appSettings");
        }

    }
}
