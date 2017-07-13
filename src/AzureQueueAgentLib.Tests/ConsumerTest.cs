using Microsoft.WindowsAzure.Storage.Queue;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class ConsumerTest
    {
        private JobFactory factory;
        private Consumer consumer;
        private ConsumerSettings settings;

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
            MockJob.Callback = mockJob =>
            {
                Assert.That(mockJob.Id, Is.EqualTo(guid));
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
                MockJob.Callback = mockJob =>
                {
                    Assert.Fail("The MockJob must not be executed.");
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
                MockJob.Callback = mockJob =>
                {
                    Assert.That(mockJob.Id, Is.EqualTo(guid));
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
        public void One_BadMessage()
        {
            bool calledBack = false;

            settings.BadMessageHandling = BadMessageHandling.DecidePerMessage;
            settings.BadMessageHandlingProvider = (msg) =>
            {
                Assert.That(msg.AsString, Is.EqualTo("One_BadMessage"));
                calledBack = true;

                return BadMessageHandling.Delete;
            };

            CloudQueue queue = consumer.GetService<CloudQueue>();
            queue.AddMessage(new CloudQueueMessage("One_BadMessage"));

            Assert.Throws(Is.TypeOf<MessageFormatException>(), () => consumer.One());
            Assert.That(calledBack, Is.True);

            IReadOnlyDictionary<string, JobPerfData> perfData = consumer.GetPerfSnapshot();
            Assert.That(perfData, Is.Not.Null);
            Assert.That(perfData.Count, Is.EqualTo(0));

            CloudQueueMessage message = queue.GetMessage();
            Assert.That(message, Is.Null);
        }

        [Test]
        public void One_UnknownJob()
        {
            bool calledBack = false;

            settings.UnknownJobHandling = UnknownJobHandling.DedicePerJob;
            settings.UnknownJobHandlingProvider = (jobDesc) =>
            {
                Assert.That(jobDesc.Job, Is.EqualTo("UnknownJob"));
                calledBack = true;

                return UnknownJobHandling.Delete;
            };

            CloudQueue queue = consumer.GetService<CloudQueue>();
            queue.AddMessage(new CloudQueueMessage("{\"Job\":\"UnknownJob\"}"));

            Assert.Throws(Is.TypeOf<UnknownJobException>(), () => consumer.One());
            Assert.That(calledBack, Is.True);

            IReadOnlyDictionary<string, JobPerfData> perfData = consumer.GetPerfSnapshot();
            Assert.That(perfData, Is.Not.Null);
            Assert.That(perfData.Count, Is.EqualTo(1));

            JobPerfData data;
            Assert.That(perfData.TryGetValue("UnknownJob", out data), Is.True);
            Assert.That(data.JobName, Is.EqualTo("UnknownJob"));
            Assert.That(data.SuccessCount, Is.EqualTo(0));
            Assert.That(data.FailureCount, Is.EqualTo(1));
            Assert.That(data.SuccessDuration, Is.EqualTo(0));
            Assert.That(data.FailureDuration, Is.GreaterThanOrEqualTo(0));

            CloudQueueMessage message = queue.GetMessage();
            Assert.That(message, Is.Null);
        }

        [Test]
        public void One_ExecutionFailed()
        {
            bool calledBack = false;
            Guid guid = Guid.NewGuid();
            MockJob.Callback = mockJob =>
            {
                Assert.That(mockJob.Id, Is.EqualTo(guid));
                calledBack = true;

                return false;
            };

            CloudQueue queue = consumer.GetService<CloudQueue>();
            queue.AddMessage(new CloudQueueMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}"));

            Assert.That(consumer.One(), Is.False);
            Assert.That(calledBack, Is.True);

            IReadOnlyDictionary<string, JobPerfData> perfData = consumer.GetPerfSnapshot();
            Assert.That(perfData, Is.Not.Null);
            Assert.That(perfData.Count, Is.EqualTo(1));

            JobPerfData data;
            Assert.That(perfData.TryGetValue("MockJob", out data), Is.True);
            Assert.That(data.JobName, Is.EqualTo("MockJob"));
            Assert.That(data.SuccessCount, Is.EqualTo(0));
            Assert.That(data.FailureCount, Is.EqualTo(1));
            Assert.That(data.SuccessDuration, Is.EqualTo(0));
            Assert.That(data.FailureDuration, Is.GreaterThanOrEqualTo(0));
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
            settings = ConsumerSettings.CreateDefault();
            consumer = new Consumer(new ConnectionSettings("consumertest"), factory, settings);
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
