using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using NUnit.Framework;
using System;
using System.IO;
using System.Text;
using System.Xml.Linq;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class JobExecutionContextTest : IServiceProvider
    {
        private CloudQueue queue;
        private JobFactory factory;
        private ConsumerSettings consumerSettings;
        private JobExecutionContext context;

        #region Dequeueing

        [Test]
        public void DequeueEmpty()
        {
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.True);
        }

        #endregion

        #region Execution

        #region Bad Messages

        [Test]
        public void DequeueBadMessageRequeue()
        {
            AddMessage("DequeueBadMessageRequeue");
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<MessageFormatException>().And.Property("MessageId").Not.Null,
                context.Execute);

            context.Dispose();

            // Verify that the message was queued again.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("DequeueBadMessageRequeue"));
            Assert.That(msg.DequeueCount, Is.EqualTo(2));
        }

        [Test]
        public void DequeueBadMessageDelete()
        {
            consumerSettings.BadMessageHandling = BadMessageHandling.Delete;

            AddMessage("DequeueBadMessageDelete");
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<MessageFormatException>().And.Property("MessageId").Not.Null,
                context.Execute);

            context.Dispose();

            // Verify that the message NOT queued again.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
        }

        [Test]
        public void DequeueBadMessageUnknown()
        {
            consumerSettings.BadMessageHandling = (BadMessageHandling)99;

            AddMessage("DequeueBadMessageUnknown");
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And.Message.EqualTo("Unsupported BadMessageHandling: 99"),
                context.Execute);

            context.Dispose();

            // Verify that the message NOT queued again.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("DequeueBadMessageUnknown"));
        }

        [Test]
        public void DequeueBadMessageAsk()
        {
            consumerSettings.BadMessageHandling = BadMessageHandling.DecidePerMessage;
            consumerSettings.BadMessageHandlingProvider = msg =>
            {
                if (msg.AsString.EndsWith("1"))
                {
                    return BadMessageHandling.Delete;
                }
                else if (msg.AsString.EndsWith("2"))
                {
                    return BadMessageHandling.Requeue;
                }
                else if (msg.AsString.EndsWith("3"))
                {
                    return (BadMessageHandling)999;
                }
                else
                {
                    return BadMessageHandling.DecidePerMessage;
                }
            };

            AddMessage("DequeueBadMessageAsk1");
            AddMessage("DequeueBadMessageAsk2");
            AddMessage("DequeueBadMessageAsk3");
            AddMessage("DequeueBadMessageAsk4");
            AddMessage("DequeueBadMessageAsk5");

            // Dequeue the first message -- it should be deleted.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<MessageFormatException>().And.Property("MessageId").Not.Null,
                context.Execute);

            context.Dispose();

            // Dequeue the second message -- it should be requeued again.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<MessageFormatException>().And.Property("MessageId").Not.Null,
                context.Execute);

            context.Dispose();

            // Dequeue the third message -- it should cause an exception because an unknown behavior is configured.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And.Message.EqualTo("Unsupported BadMessageHandling: 999"),
                context.Execute);

            context.Dispose();

            // Dequeue the fourth message -- it should cause an exception because the decision provider returned an unspported value.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And
                .Message.EqualTo("Unsupported BadMessageHandling: DecidePerMessage"),
                context.Execute);

            context.Dispose();

            // Dequeue the fifth message -- it should cause an exception because the decision provider is missing.
            consumerSettings.BadMessageHandlingProvider = null;
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<InvalidOperationException>().And
                .Message.EqualTo("The BadMessageHandlingProvider must not be null when 'DecidePerMessage' is used."),
                context.Execute);

            context.Dispose();

            // Verify that the message NOT queued again.
            CloudQueueMessage message = queue.GetMessage();
            Assert.That(message, Is.Not.Null);
            Assert.That(message.AsString, Is.EqualTo("DequeueBadMessageAsk2"));
        }

        #endregion

        #region Unknown Jobs

        [Test]
        public void DequeueUnknownJobRequeue()
        {
            AddMessage("{\"Job\":\"DequeueUnknownJobRequeue\"}");
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<UnknownJobException>().And.Property("MessageId").Not.Null,
                context.Execute);

            context.Dispose();

            // Verify that the message was queued again.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"DequeueUnknownJobRequeue\"}"));
            Assert.That(msg.DequeueCount, Is.EqualTo(2));
        }

        [Test]
        public void DequeueUnknownJobDelete()
        {
            consumerSettings.UnknownJobHandling = UnknownJobHandling.Delete;

            AddMessage("{\"Job\":\"DequeueUnknownJobDelete\"}");
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<UnknownJobException>().And.Property("MessageId").Not.Null,
                context.Execute);

            context.Dispose();

            // Verify that the message NOT queued again.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
        }

        [Test]
        public void DequeueUnknownJobUnknown()
        {
            consumerSettings.UnknownJobHandling = (UnknownJobHandling)99;

            AddMessage("{\"Job\":\"DequeueUnknownJobDelete\"}");
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And.Message.EqualTo("Unsupported UnknownJobHandling: 99"),
                context.Execute);

            context.Dispose();

            // Verify that the message NOT queued again.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"DequeueUnknownJobDelete\"}"));
        }

        [Test]
        public void DequeueUnknownJobAsk()
        {
            consumerSettings.UnknownJobHandling = UnknownJobHandling.DedicePerJob;
            consumerSettings.UnknownJobHandlingProvider = descriptor =>
            {
                if (descriptor.Job.EndsWith("1"))
                {
                    return UnknownJobHandling.Delete;
                }
                else if (descriptor.Job.EndsWith("2"))
                {
                    return UnknownJobHandling.Requeue;
                }
                else if (descriptor.Job.EndsWith("3"))
                {
                    return (UnknownJobHandling)999;
                }
                else
                {
                    return UnknownJobHandling.DedicePerJob;
                }
            };

            AddMessage("{\"Job\":\"DequeueUnknownJobAsk1\"}");
            AddMessage("{\"Job\":\"DequeueUnknownJobAsk2\"}");
            AddMessage("{\"Job\":\"DequeueUnknownJobAsk3\"}");
            AddMessage("{\"Job\":\"DequeueUnknownJobAsk4\"}");
            AddMessage("{\"Job\":\"DequeueUnknownJobAsk5\"}");

            // Dequeue the first message -- it should be deleted.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<UnknownJobException>().And.Property("JobName").EqualTo("DequeueUnknownJobAsk1"),
                context.Execute);

            context.Dispose();

            // Dequeue the second message -- it should be requeued again.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<UnknownJobException>().And.Property("JobName").EqualTo("DequeueUnknownJobAsk2"),
                context.Execute);

            context.Dispose();

            // Dequeue the third message -- it should cause an exception because an unknown behavior is configured.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And.Message.EqualTo("Unsupported UnknownJobHandling: 999"),
                context.Execute);

            context.Dispose();

            // Dequeue the fourth message -- it should cause an exception because the decision provider returned an unspported value.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And
                .Message.EqualTo("Unsupported UnknownJobHandling: DedicePerJob"),
                context.Execute);

            context.Dispose();

            // Dequeue the fifth message -- it should cause an exception because the decision provider is missing.
            consumerSettings.UnknownJobHandlingProvider = null;
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<InvalidOperationException>().And
                .Message.EqualTo("The UnknownJobHandlingProvider must not be null when 'DedicePerJob' is used."),
                context.Execute);

            context.Dispose();

            // Verify that the message NOT queued again.
            CloudQueueMessage message = queue.GetMessage();
            Assert.That(message, Is.Not.Null);
            Assert.That(message.AsString, Is.EqualTo("{\"Job\":\"DequeueUnknownJobAsk2\"}"));
        }

        #endregion

        [Test]
        public void ExecuteSuccess()
        {
            Guid guid = Guid.NewGuid();
            AddXmlMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}");
            context = JobExecutionContext.Dequeue(this);

            MockJob.Callback = id =>
            {
                Assert.That(id, Is.EqualTo(guid));

                return true;
            };

            Assert.That(context.Empty, Is.False);
            Assert.DoesNotThrow(context.Execute);

            context.Dispose();

            // Verify that the message was deleted.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
        }

        [Test]
        public void ExecuteFailed()
        {
            Guid guid = Guid.NewGuid();
            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}");
            context = JobExecutionContext.Dequeue(this);

            MockJob.Callback = id =>
            {
                Assert.That(id, Is.EqualTo(guid));

                return false;
            };

            Assert.That(context.Empty, Is.False);
            Assert.DoesNotThrow(context.Execute);

            context.Dispose();

            // Verify that the message was deleted.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}"));
        }

        [Test]
        public void ExecuteException()
        {
            Guid guid = Guid.NewGuid();
            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}");
            context = JobExecutionContext.Dequeue(this);

            MockJob.Callback = id =>
            {
                Assert.That(id, Is.EqualTo(guid));

                throw new InvalidOperationException(guid.ToString());
            };

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<InvalidOperationException>().And.Message.EqualTo(guid.ToString()), context.Execute);

            context.Dispose();

            // Verify that the message was deleted.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}"));
        }

        #endregion

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            CloudQueueClient client = CloudStorageAccount.DevelopmentStorageAccount.CreateCloudQueueClient();

            queue = client.GetQueueReference("jobexecutioncontexttest");
            queue.CreateIfNotExists();

            factory = new JobFactory();
            factory.RegisterJobType(typeof(HelloWho));
            factory.RegisterJobType(typeof(MockJob));
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            queue.DeleteIfExists();
        }

        [SetUp]
        public void Setup()
        {
            queue.Clear();

            consumerSettings = ConsumerSettings.CreateDefault();
        }

        [TearDown]
        public void TearDown()
        {
            MockJob.Callback = null;

            if (null != context)
            {
                context.Dispose();
            }
        }

        object IServiceProvider.GetService(Type serviceType)
        {
            if (serviceType == typeof(CloudQueue))
            {
                return queue;
            }
            else if (serviceType == typeof(JobFactory))
            {
                return factory;
            }
            else if (serviceType == typeof(ConsumerSettings))
            {
                return consumerSettings;
            }

            return null;
        }

        private void AddMessage(string body)
        {
            queue.AddMessage(new CloudQueueMessage(body));
        }

        private void AddXmlMessage(string body)
        {
            XDocument doc = new XDocument(new XDeclaration("1.0", "utf-8", "yes"), new XElement("StorageQueueMessage", new XElement("Message", body)));
            StringBuilder sb = new StringBuilder();

            using (StringWriter writer = new StringWriter(sb))
            {
                doc.Save(writer);
            }

            AddMessage(sb.ToString());
        }
    }
}
