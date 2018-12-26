using System;
using Microsoft.WindowsAzure.Storage;

namespace Aqua.Tests
{
    internal static class StorageAccount
    {
        private static readonly CloudStorageAccount acct = CloudStorageAccount.Parse(GetConnectionString());

        public static CloudStorageAccount Get()
        {
            return acct;
        }

        public static string GetConnectionString()
        {
            string connStr = Environment.GetEnvironmentVariable("CONNECTION_STRING");

            if (String.IsNullOrWhiteSpace(connStr))
            {
                connStr = "UseDevelopmentStorage=true";
            }

            return connStr;
        }
    }
}
