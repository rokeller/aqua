using Microsoft.WindowsAzure.Storage.Queue;
using NUnit.Framework;
using System;
using System.Diagnostics;
using System.Threading;

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
            Assert.That(consumer.One(), Is.False);
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
            Assert.That(consumer.One(new SimpleRetryStrategy(5, TimeSpan.FromMilliseconds(1))), Is.False);
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

            Assert.That(consumer.One(), Is.True);
            Assert.That(calledBack, Is.True);
        }

        [Test]
        public void OneWithMessage_CancelledBeforeDequeue()
        {
            using (CancellationTokenSource cancelSource = new CancellationTokenSource())
            {
                cancelSource.Cancel();

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

                Assert.That(consumer.One(new SimpleRetryStrategy(5, TimeSpan.FromMilliseconds(5)), cancelSource.Token),
                    Is.False);
                Assert.That(calledBack, Is.False);
            }
        }

        [Test]
        public void OneWithMessage_CancelledWhileHandling()
        {
            using (CancellationTokenSource cancelSource = new CancellationTokenSource())
            {
                bool calledBack = false;
                Guid guid = Guid.NewGuid();
                MockJob.Callback = id =>
                {
                    Assert.That(id, Is.EqualTo(guid));
                    calledBack = true;
                    cancelSource.Cancel();

                    return true;
                };

                CloudQueue queue = consumer.GetService<CloudQueue>();
                queue.AddMessage(new CloudQueueMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}"));

                Assert.That(consumer.One(new SimpleRetryStrategy(5, TimeSpan.FromMilliseconds(5)), cancelSource.Token),
                    Is.True);
                Assert.That(calledBack, Is.True);
            }
        }

        [Test]
        public void OneEmpty_CancelledWhileWaiting()
        {
            using (CancellationTokenSource cancelSource = new CancellationTokenSource(100))
            {
                Stopwatch watch = Stopwatch.StartNew();

                Assert.That(consumer.One(new SimpleRetryStrategy(5, TimeSpan.FromSeconds(2)), cancelSource.Token),
                    Is.False);

                watch.Stop();

                Assert.That(watch.ElapsedMilliseconds, Is.GreaterThanOrEqualTo(100).And.LessThan(500));
            }
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

            CloudQueue queue = consumer.GetService<CloudQueue>();
            queue.Clear();
        }
    }
}
