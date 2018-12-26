using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using NUnit.Framework;
using System;
using System.Threading;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class ProducerTest
    {
        private static readonly CloudStorageAccount acct = StorageAccount.Get();
        private static readonly CloudQueueClient client = acct.CreateCloudQueueClient();
        private static CloudQueue queue;
        private JobFactory factory;
        private Producer producer;

        [Test]
        public void CtorInputValidation()
        {
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("connectionSettings"),
                () => new Producer(null, factory));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("factory"),
                () => new Producer(new ConnectionSettings(acct, "producertest"), null));
        }

        [Test]
        public void OneInputValidation()
        {
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("job"),
                () => producer.One((IJob)null));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("descriptor"),
                () => producer.One((JobDescriptor)null));
            Assert.Throws(Is.TypeOf<ArgumentException>().And.Message.EqualTo("The JobDescriptor must have a non-null and non-blank Job property."),
                () => producer.One(new JobDescriptor()));
        }

        [Test]
        public void One()
        {
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);

            producer.One(new HelloWho() { Who = "ProducerTest-One" });

            msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"HelloWho\",\"Properties\":{\"Who\":\"ProducerTest-One\"}}"));

            JobDescriptor descriptor = JsonConvert.DeserializeObject<JobDescriptor>(msg.AsString);
            Assert.That(descriptor, Is.Not.Null.
                And.Property("Job").EqualTo("HelloWho").
                And.Property("Properties").Count.EqualTo(1));
            Assert.That(descriptor.Properties["Who"].ToObject<string>(), Is.EqualTo("ProducerTest-One"));

            queue.DeleteMessage(msg);
        }

        [Test]
        public void OneWithJobDescriptor()
        {
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);

            JobDescriptor descriptor = factory.CreateDescriptor(new HelloWho() { Who = "ProducerTest-OneWithJobDescriptor" });
            producer.One(descriptor);

            msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"HelloWho\",\"Properties\":{\"Who\":\"ProducerTest-OneWithJobDescriptor\"}}"));

            descriptor = JsonConvert.DeserializeObject<JobDescriptor>(msg.AsString);
            Assert.That(descriptor, Is.Not.Null.
                And.Property("Job").EqualTo("HelloWho").
                And.Property("Properties").Count.EqualTo(1));
            Assert.That(descriptor.Properties["Who"].ToObject<string>(), Is.EqualTo("ProducerTest-OneWithJobDescriptor"));

            queue.DeleteMessage(msg);
        }

        [Test]
        public void OneWithInitialVisibilityDelay()
        {
            TimeSpan visibilityDelay = TimeSpan.FromSeconds(1);
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);

            producer.One(new HelloWho() { Who = "ProducerTest-OneWithInitialVisibilityDelay" }, visibilityDelay);

            msg = queue.GetMessage();
            Assert.That(msg, Is.Null);

            Thread.Sleep(visibilityDelay);

            msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"HelloWho\",\"Properties\":{\"Who\":\"ProducerTest-OneWithInitialVisibilityDelay\"}}"));

            JobDescriptor descriptor = JsonConvert.DeserializeObject<JobDescriptor>(msg.AsString);
            Assert.That(descriptor, Is.Not.Null.
                And.Property("Job").EqualTo("HelloWho").
                And.Property("Properties").Count.EqualTo(1));
            Assert.That(descriptor.Properties["Who"].ToObject<string>(), Is.EqualTo("ProducerTest-OneWithInitialVisibilityDelay"));

            queue.DeleteMessage(msg);
        }

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            queue = client.GetQueueReference("producertest");

            factory = new JobFactory();
            factory.RegisterJobType(typeof(HelloWho));
            factory.RegisterJobType(typeof(MockJob));
        }

        [SetUp]
        public void Setup()
        {
            producer = new Producer(new ConnectionSettings(acct, "producertest"), factory);
        }

        [TearDown]
        public void TearDown()
        {
            if (queue.Exists())
            {
                queue.Clear();
            }
        }
    }
}
