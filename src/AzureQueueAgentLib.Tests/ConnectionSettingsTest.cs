using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class ConnectionSettingsTest
    {
        private static readonly CloudStorageAccount acct = StorageAccount.Get();
        private static readonly CloudQueueClient client = acct.CreateCloudQueueClient();

        [Test]
        public void ByStorageAccount()
        {
            ConnectionSettings settings = new ConnectionSettings(acct, "connectionsettingstest");
            CloudQueue queue = settings.GetQueue();

            Assert.That(queue, Is.Not.Null);
            Assert.That(queue.Exists());

            settings = new ConnectionSettings(acct, "connectionsettingstest-devaccount");
            queue = settings.GetQueue();

            Assert.That(queue, Is.Not.Null);
            Assert.That(!queue.Exists());
        }

        [Test]
        public void ByStorageAccountConnectionString()
        {
            string connStr = StorageAccount.GetConnectionString();

            ConnectionSettings settings = new ConnectionSettings(connStr, "connectionsettingstest");
            CloudQueue queue = settings.GetQueue();

            Assert.That(queue, Is.Not.Null);
            Assert.That(queue.Exists());

            settings = new ConnectionSettings(connStr, "connectionsettingstest-devaccountconnectionstring");
            queue = settings.GetQueue();

            Assert.That(queue, Is.Not.Null);
            Assert.That(!queue.Exists());
        }

        [Test]
        public void ByAccountNameAndKey()
        {
            ConnectionSettings settings = new ConnectionSettings("devstoreaccount1", "Zm9yYmlkZGVu", "connectionsettingstest");
            CloudQueue queue = settings.GetQueue();

            Assert.That(queue, Is.Not.Null);
            Assert.Throws(Is.TypeOf<StorageException>().And
                .Property("RequestInformation")
                    .Property("HttpStatusCode").EqualTo(403), () => queue.Exists());
        }

        #region Input Validation

        [Test]
        public void CtorInputValidation()
        {
            // Account and Queue
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("storageAccount"),
                () => new ConnectionSettings((CloudStorageAccount)null, "queue"));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("queueName"),
                () => new ConnectionSettings(acct, "   "));

            // Connection String and Queue
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("connectionString"),
                () => new ConnectionSettings((string)null, "queue"));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("connectionString"),
               () => new ConnectionSettings("", "queue"));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("connectionString"),
               () => new ConnectionSettings("    ", "queue"));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("queueName"),
                () => new ConnectionSettings("blah", "   "));

            // Account Name, Account Key and Queue
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("storageAccountName"),
               () => new ConnectionSettings(null, "key", "queue"));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("storageAccountName"),
               () => new ConnectionSettings("", "key", "queue"));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("storageAccountName"),
               () => new ConnectionSettings("    ", "key", "queue"));

            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("storageAccountKey"),
             () => new ConnectionSettings("name", null, "queue"));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("storageAccountKey"),
               () => new ConnectionSettings("name", "", "queue"));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("storageAccountKey"),
               () => new ConnectionSettings("name", "    ", "queue"));

            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("queueName"),
             () => new ConnectionSettings("name", "key", null));
        }

        #endregion

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            client.GetQueueReference("connectionsettingstest").CreateIfNotExists();
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            client.GetQueueReference("connectionsettingstest").DeleteIfExists();
        }
    }
}
