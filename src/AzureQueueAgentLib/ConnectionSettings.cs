using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Queue;
using System;

namespace Aqua
{
    /// <summary>
    /// Provides settings that can be used to connect to a Queue in an Azure Storage Account.
    /// </summary>
    public class ConnectionSettings
    {
        #region Fields

        /// <summary>
        /// The CloudQueueClient to use.
        /// </summary>
        private readonly CloudQueueClient client;

        #endregion

        #region C'tors

        /// <summary>
        /// Initializes a new instance of ConnectionSettings using the Development Storage Account (i.e. the local
        /// emulator).
        /// </summary>
        /// <param name="queueName">
        /// The name of the queue to connect to.
        /// </param>
        public ConnectionSettings(string queueName) : this(CloudStorageAccount.DevelopmentStorageAccount, queueName) { }

        /// <summary>
        /// Initializes a new instance of ConnectionSettings using the given CloudStorageAccount and queue name.
        /// </summary>
        /// <param name="storageAccount">
        /// The CloudStorageAccount to use to connect to the Azure Storage Account.
        /// </param>
        /// <param name="queueName">
        /// The name of the queue to connect to.
        /// </param>
        public ConnectionSettings(CloudStorageAccount storageAccount, string queueName)
        {
            if (null == storageAccount)
            {
                throw new ArgumentNullException("storageAccount");
            }
            else if (String.IsNullOrWhiteSpace(queueName))
            {
                throw new ArgumentNullException("queueName");
            }

            client = storageAccount.CreateCloudQueueClient();

            QueueName = queueName;
        }

        /// <summary>
        /// Initializes a new instance of ConnectionSettings using the given connection string.
        /// </summary>
        /// <param name="connectionString">
        /// The Azure Storage Account connection string to use to connect.
        /// </param>
        /// <param name="queueName">
        /// The name of the queue to connect to.
        /// </param>
        public ConnectionSettings(string connectionString, string queueName)
        {
            if (String.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentNullException("connectionString");
            }
            else if (String.IsNullOrWhiteSpace(queueName))
            {
                throw new ArgumentNullException("queueName");
            }

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            client = storageAccount.CreateCloudQueueClient();

            QueueName = queueName;
        }

        /// <summary>
        /// Initializes a new instance of ConnectionSettings using the given storage account name and key as well as
        /// the provided queue name.
        /// </summary>
        /// <param name="storageAccountName">
        /// The name of the Azure Storage Account to connect to.
        /// </param>
        /// <param name="storageAccountKey">
        /// The access key to the Azure Storage Account to connect to.
        /// </param>
        /// <param name="queueName">
        /// The name of the queue to connect to.
        /// </param>
        public ConnectionSettings(string storageAccountName, string storageAccountKey, string queueName)
        {
            if (String.IsNullOrWhiteSpace(storageAccountName))
            {
                throw new ArgumentNullException("storageAccountName");
            }
            else if (String.IsNullOrWhiteSpace(storageAccountKey))
            {
                throw new ArgumentNullException("storageAccountKey");
            }
            else if (String.IsNullOrWhiteSpace(queueName))
            {
                throw new ArgumentNullException("queueName");
            }

            StorageCredentials credentials = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(credentials, true);
            client = storageAccount.CreateCloudQueueClient();

            QueueName = queueName;
        }

        #endregion

        /// <summary>
        /// Gets or sets the name of the Queue to use.
        /// </summary>
        public string QueueName { get; private set; }

        /// <summary>
        /// Gets an instance of CloudQueue to use to work with the Queue.
        /// </summary>
        /// <returns>
        /// An instance of CloudQueue.
        /// </returns>
        public virtual CloudQueue GetQueue()
        {
            return client.GetQueueReference(QueueName);
        }
    }
}
