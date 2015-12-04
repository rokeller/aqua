using Microsoft.WindowsAzure.Storage.Queue;
using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class ConsumerTest
    {
        private JobFactory factory;
        private Consumer consumer;

        [Test]
        public void CtorInputValidation()
        {
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("connectionSettings"),
                () => new Consumer(null, factory));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("factory"),
                () => new Consumer(new ConnectionSettings("consumertest"), null));

            consumer = new Consumer(new ConnectionSettings("consumertest"), factory, null);
            Assert.That(((IServiceProvider)consumer).GetService<ConsumerSettings>(), Is.Not.Null);
        }

        [Test]
        public void OneSingleTry_Empty()
        {
            Assert.DoesNotThrow(consumer.One);
        }

        [Test]
        public void OneWithNullStrategy()
        {
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("dequeueStrategy"),
                () => consumer.One(null));
        }

        [Test]
        public void OneWithRetry_Empty()
        {
            Assert.DoesNotThrow(() => consumer.One(new SimpleRetryStrategy(5, TimeSpan.FromMilliseconds(1))));
        }

        [Test]
        public void OneWithMessage()
        {
            bool calledBack = false;
            Guid guid = Guid.NewGuid();
            MockJob.Callback = id =>
            {
                Assert.That(id, Is.EqualTo(guid));
                calledBack = true;

                return true;
            };

            CloudQueue queue = consumer.GetService<CloudQueue>();
            queue.AddMessage(new CloudQueueMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}"));

            Assert.DoesNotThrow(consumer.One);
            Assert.That(calledBack, Is.True);
        }

        [Test]
        public void GetService_NotFound()
        {
            IServiceProvider serviceProvider = consumer;
            Assert.That(serviceProvider.GetService(typeof(string)), Is.Null);
        }

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            factory = new JobFactory();
            factory.RegisterJobType(typeof(HelloWho));
            factory.RegisterJobType(typeof(MockJob));
        }

        [SetUp]
        public void Setup()
        {
            consumer = new Consumer(new ConnectionSettings("consumertest"), factory);
        }

        [TearDown]
        public void TearDown()
        {
            MockJob.Callback = null;
        }
    }
}
