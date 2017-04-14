using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using NUnit.Framework;
using System;
using System.IO;
using System.Text;
using System.Threading;
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
            consumerSettings.BadMessageHandling = BadMessageHandling.Requeue;

            AddMessage("DequeueBadMessageRequeue");
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<MessageFormatException>().And.Property("MessageId").Not.Null,
                () => context.Execute());

            context.Dispose();

            // Verify that the message was queued again.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("DequeueBadMessageRequeue"));
            Assert.That(msg.DequeueCount, Is.EqualTo(2));
        }

        [Test]
        public void DequeueBadMessageRequeue_Timeout()
        {
            consumerSettings.BadMessageHandling = BadMessageHandling.Requeue;
            consumerSettings.BadMessageRequeueTimeout = TimeSpan.FromMilliseconds(100);

            AddMessage("DequeueBadMessageRequeue");
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<MessageFormatException>().And.Property("MessageId").Not.Null,
                () => context.Execute());

            context.Dispose();

            // Verify that the message is not queued yet.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
            Thread.Sleep(1100);

            // Verify that the message is now queued again.
            msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("DequeueBadMessageRequeue"));
            Assert.That(msg.DequeueCount, Is.EqualTo(2));
        }

        [Test]
        public void DequeueBadMessageRequeueThenDeleteAfterThreshold()
        {
            const int NumIterations = 3;
            consumerSettings.BadMessageHandling = BadMessageHandling.RequeueThenDeleteAfterThreshold;
            consumerSettings.BadMessageRequeueThreshold = NumIterations;

            AddMessage("DequeueBadMessageRequeueThenDeleteAfterThreshold");
            CloudQueueMessage msg;

            for (int i = 0; i < NumIterations; i++)
            {
                // Verify that the message is currently queued.
                msg = queue.PeekMessage();
                Assert.That(msg, Is.Not.Null, "Iteration: {0}", i);
                Assert.That(msg.AsString, Is.EqualTo("DequeueBadMessageRequeueThenDeleteAfterThreshold"), "Iteration: {0}", i);
                Assert.That(msg.DequeueCount, Is.EqualTo(i), "Iteration: {0}", i);

                // Dequeue, and try to handle.
                context = JobExecutionContext.Dequeue(this);

                Assert.That(context.Empty, Is.False, "Iteration: {0}", i);
                Assert.Throws(Is.TypeOf<MessageFormatException>().And.Property("MessageId").Not.Null,
                    () => context.Execute(), "Iteration: {0}", i);

                context.Dispose();
            }

            // Verify that the message NOT queued again.
            msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
        }

        [Test]
        public void DequeueBadMessageDelete()
        {
            consumerSettings.BadMessageHandling = BadMessageHandling.Delete;

            AddMessage("DequeueBadMessageDelete");
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<MessageFormatException>().And.Property("MessageId").Not.Null,
                () => context.Execute());

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
                () => context.Execute());

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
                () => context.Execute());

            context.Dispose();

            // Dequeue the second message -- it should be requeued again.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<MessageFormatException>().And.Property("MessageId").Not.Null,
                () => context.Execute());

            context.Dispose();

            // Dequeue the third message -- it should cause an exception because an unknown behavior is configured.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And.Message.EqualTo("Unsupported BadMessageHandling: 999"),
                () => context.Execute());

            context.Dispose();

            // Dequeue the fourth message -- it should cause an exception because the decision provider returned an unspported value.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And
                .Message.EqualTo("Unsupported BadMessageHandling: DecidePerMessage"),
                () => context.Execute());

            context.Dispose();

            // Dequeue the fifth message -- it should cause an exception because the decision provider is missing.
            consumerSettings.BadMessageHandlingProvider = null;
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<InvalidOperationException>().And
                .Message.EqualTo("The BadMessageHandlingProvider must not be null when 'DecidePerMessage' is used."),
                () => context.Execute());

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
                () => context.Execute());

            context.Dispose();

            // Verify that the message was queued again.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"DequeueUnknownJobRequeue\"}"));
            Assert.That(msg.DequeueCount, Is.EqualTo(2));
        }

        [Test]
        public void DequeueUnknownJobRequeue_Timeout()
        {
            consumerSettings.UnknownJobRequeueTimeout = TimeSpan.FromMilliseconds(100);

            AddMessage("{\"Job\":\"DequeueUnknownJobRequeue\"}");
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<UnknownJobException>().And.Property("MessageId").Not.Null,
                () => context.Execute());

            context.Dispose();

            // Verify that the message is not queued yet.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
            Thread.Sleep(1100);

            // Verify that the message is now queued again.
            msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"DequeueUnknownJobRequeue\"}"));
            Assert.That(msg.DequeueCount, Is.EqualTo(2));
        }

        [Test]
        public void DequeueUnknownJobRequeueThenDeleteAfterThreshold()
        {
            const int NumIterations = 13;
            consumerSettings.UnknownJobHandling = UnknownJobHandling.RequeueThenDeleteAfterThreshold;
            consumerSettings.UnknownJobRequeueThreshold = NumIterations;

            AddMessage("{\"Job\":\"DequeueUnknownJobRequeueThenDeleteAfterThreshold\"}");
            CloudQueueMessage msg;

            for (int i = 0; i < NumIterations; i++)
            {
                // Verify that the message is currently queued.
                msg = queue.PeekMessage();
                Assert.That(msg, Is.Not.Null, "Iteration: {0}", i);
                Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"DequeueUnknownJobRequeueThenDeleteAfterThreshold\"}"), "Iteration: {0}", i);
                Assert.That(msg.DequeueCount, Is.EqualTo(i), "Iteration: {0}", i);

                // Dequeue, and try to handle.
                context = JobExecutionContext.Dequeue(this);

                Assert.That(context.Empty, Is.False, "Iteration: {0}", i);
                Assert.Throws(Is.TypeOf<UnknownJobException>().And.Property("MessageId").Not.Null,
                () => context.Execute(), "Iteration: {0}", i);

                context.Dispose();
            }

            // Verify that the message NOT queued again.
            msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
        }

        [Test]
        public void DequeueUnknownJobDelete()
        {
            consumerSettings.UnknownJobHandling = UnknownJobHandling.Delete;

            AddMessage("{\"Job\":\"DequeueUnknownJobDelete\"}");
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<UnknownJobException>().And.Property("MessageId").Not.Null,
                () => context.Execute());

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
                () => context.Execute());

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
                () => context.Execute());

            context.Dispose();

            // Dequeue the second message -- it should be requeued again.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<UnknownJobException>().And.Property("JobName").EqualTo("DequeueUnknownJobAsk2"),
                () => context.Execute());

            context.Dispose();

            // Dequeue the third message -- it should cause an exception because an unknown behavior is configured.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And.Message.EqualTo("Unsupported UnknownJobHandling: 999"),
                () => context.Execute());

            context.Dispose();

            // Dequeue the fourth message -- it should cause an exception because the decision provider returned an unspported value.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And
                .Message.EqualTo("Unsupported UnknownJobHandling: DedicePerJob"),
                () => context.Execute());

            context.Dispose();

            // Dequeue the fifth message -- it should cause an exception because the decision provider is missing.
            consumerSettings.UnknownJobHandlingProvider = null;
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<InvalidOperationException>().And
                .Message.EqualTo("The UnknownJobHandlingProvider must not be null when 'DedicePerJob' is used."),
                () => context.Execute());

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

            MockJob.Callback = mockJob =>
            {
                Assert.That(mockJob.Id, Is.EqualTo(guid));

                return true;
            };

            Assert.That(context.Empty, Is.False);
            Assert.That(context.Execute(), Is.True);
            Assert.That(context.JobName, Is.EqualTo("MockJob"));
            Assert.That(context.WasSuccessful, Is.True);

            context.Dispose();

            // Verify that the message was deleted.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
        }

        #region Failed Jobs

        #region Job Executions Returns False

        [Test]
        public void ExecuteFailed_Requeue()
        {
            Guid guid = Guid.NewGuid();
            string msgBody = "{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}";
            AddMessage(msgBody);
            context = JobExecutionContext.Dequeue(this);

            MockJob.Callback = mockJob =>
            {
                Assert.That(mockJob.Id, Is.EqualTo(guid));

                return false;
            };

            Assert.That(context.Empty, Is.False);
            Assert.That(context.Execute(), Is.False);
            Assert.That(context.JobName, Is.EqualTo("MockJob"));
            Assert.That(context.WasSuccessful, Is.False);

            context.Dispose();

            // Verify that the message was not deleted.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo(msgBody));
        }

        [Test]
        public void ExecuteFailed_Requeue_Timeout()
        {
            consumerSettings.FailedJobRequeueTimeout = TimeSpan.FromMilliseconds(100);

            Guid guid = Guid.NewGuid();
            string msgBody = "{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}";
            AddMessage(msgBody);
            context = JobExecutionContext.Dequeue(this);

            MockJob.Callback = mockJob =>
            {
                Assert.That(mockJob.Id, Is.EqualTo(guid));

                return false;
            };

            Assert.That(context.Empty, Is.False);
            Assert.That(context.Execute(), Is.False);
            Assert.That(context.JobName, Is.EqualTo("MockJob"));
            Assert.That(context.WasSuccessful, Is.False);

            context.Dispose();

            // Verify that the message is not queued yet.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
            Thread.Sleep(1100);

            // Verify that the message is now queued again.
            msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo(msgBody));
        }

        [Test]
        public void ExecuteFailed_RequeueThenDeleteAfterThreshold()
        {
            const int NumIterations = 13;
            consumerSettings.FailedJobHandling = FailedJobHandling.RequeueThenDeleteAfterThreshold;
            consumerSettings.FailedJobRequeueThreshold = NumIterations;

            Guid guid = Guid.NewGuid();
            string msgBody = "{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}";
            AddMessage(msgBody);
            CloudQueueMessage msg;

            for (int i = 0; i < NumIterations; i++)
            {
                // Verify that the message is currently queued.
                msg = queue.PeekMessage();
                Assert.That(msg, Is.Not.Null, "Iteration: {0}", i);
                Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}"),
                    "Iteration: {0}", i);
                Assert.That(msg.DequeueCount, Is.EqualTo(i), "Iteration: {0}", i);

                // Dequeue, and try to handle.
                context = JobExecutionContext.Dequeue(this);

                MockJob.Callback = mockJob =>
                {
                    Assert.That(mockJob.Id, Is.EqualTo(guid));

                    return false;
                };

                Assert.That(context.Empty, Is.False);
                Assert.That(context.Execute(), Is.False);
                Assert.That(context.JobName, Is.EqualTo("MockJob"));
                Assert.That(context.WasSuccessful, Is.False);

                context.Dispose();
            }

            // Verify that the message NOT queued again.
            msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
        }

        [Test]
        public void ExecuteFailed_Delete()
        {
            consumerSettings.FailedJobHandling = FailedJobHandling.Delete;

            Guid guid = Guid.NewGuid();
            string msgBody = "{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}";
            AddMessage(msgBody);
            context = JobExecutionContext.Dequeue(this);

            MockJob.Callback = mockJob =>
            {
                Assert.That(mockJob.Id, Is.EqualTo(guid));

                return false;
            };

            Assert.That(context.Empty, Is.False);
            Assert.That(context.Execute(), Is.False);
            Assert.That(context.JobName, Is.EqualTo("MockJob"));
            Assert.That(context.WasSuccessful, Is.False);

            context.Dispose();

            // Verify that the message NOT queued again.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
        }

        [Test]
        public void ExecuteFailed_Unknown()
        {
            consumerSettings.FailedJobHandling = (FailedJobHandling)99;

            Guid guid = Guid.NewGuid();
            string msgBody = "{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}";
            AddMessage(msgBody);
            context = JobExecutionContext.Dequeue(this);

            MockJob.Callback = mockJob =>
            {
                Assert.That(mockJob.Id, Is.EqualTo(guid));

                return false;
            };

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And.Message.EqualTo("Unsupported FailedJobHandling: 99"),
                () => context.Execute());

            context.Dispose();

            // Verify that the message was not deleted.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo(msgBody));
        }

        [Test]
        public void ExecuteFailed_Ask()
        {
            consumerSettings.FailedJobHandling = FailedJobHandling.DedicePerJob;
            consumerSettings.FailedJobHandlingProvider = (job, descriptor, exception) =>
            {
                Assert.That(job, Is.InstanceOf<MockJob>());
                Assert.That(descriptor, Is.Not.Null);

                MockJob mockJob = (MockJob)job;

                switch (mockJob.Int32)
                {
                    case 1:
                        Assert.That(exception, Is.Null);
                        return FailedJobHandling.Delete;
                    case 2:
                        Assert.That(exception, Is.Null);
                        return FailedJobHandling.Requeue;
                    case 3:
                        Assert.That(exception, Is.Null);
                        //Assert.That(exception, Is.Null.Or.InstanceOf<NotSupportedException>());
                        return (FailedJobHandling)999;

                    default:
                        Assert.That(exception, Is.Null);
                        //Assert.That(exception, Is.Null.Or.InstanceOf<NotSupportedException>());
                        return FailedJobHandling.DedicePerJob;
                }
            };

            MockJob.Callback = mockJob =>
            {
                return false;
            };

            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Int32\":1}}");
            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Int32\":2}}");
            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Int32\":3}}");
            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Int32\":4}}");
            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Int32\":5}}");

            // Dequeue the first message -- it should be deleted.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.That(context.Execute(), Is.False);

            context.Dispose();

            // Dequeue the second message -- it should be requeued again.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.That(context.Execute(), Is.False);

            context.Dispose();

            // Dequeue the third message -- it should cause an exception because an unknown behavior is configured.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And.Message.EqualTo("Unsupported FailedJobHandling: 999"),
                () => context.Execute());

            context.Dispose();

            // Dequeue the fourth message -- it should cause an exception because the decision provider returned an unspported value.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And
                .Message.EqualTo("Unsupported FailedJobHandling: DedicePerJob"),
                () => context.Execute());

            context.Dispose();

            // Dequeue the fifth message -- it should cause an exception because the decision provider is missing.
            consumerSettings.FailedJobHandlingProvider = null;
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<InvalidOperationException>().And
                .Message.EqualTo("The FailedJobHandlingProvider must not be null when 'DedicePerJob' is used."),
                () => context.Execute());

            context.Dispose();

            // Verify that the message NOT queued again.
            CloudQueueMessage message = queue.GetMessage();
            Assert.That(message, Is.Not.Null);
            Assert.That(message.AsString, Is.EqualTo("{\"Job\":\"MockJob\",\"Properties\":{\"Int32\":2}}"));
        }

        #endregion

        #region Job Execution Throws

        [Test]
        public void ExecuteException_Requeue()
        {
            Guid guid = Guid.NewGuid();
            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}");
            context = JobExecutionContext.Dequeue(this);

            MockJob.Callback = mockJob =>
            {
                Assert.That(mockJob.Id, Is.EqualTo(guid));

                throw new InvalidOperationException(guid.ToString());
            };

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<InvalidOperationException>().And.Message.EqualTo(guid.ToString()),
                () => context.Execute());

            context.Dispose();

            // Verify that the message was deleted.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}"));
        }

        [Test]
        public void ExecuteException_Requeue_Timeout()
        {
            consumerSettings.FailedJobRequeueTimeout = TimeSpan.FromMilliseconds(100);

            Guid guid = Guid.NewGuid();
            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}");
            context = JobExecutionContext.Dequeue(this);

            MockJob.Callback = mockJob =>
            {
                Assert.That(mockJob.Id, Is.EqualTo(guid));

                throw new InvalidOperationException(guid.ToString());
            };

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<InvalidOperationException>().And.Message.EqualTo(guid.ToString()),
                () => context.Execute());

            context.Dispose();

            // Verify that the message is not queued yet.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
            Thread.Sleep(1100);

            // Verify that the message is now queued again.
            msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}"));
        }

        [Test]
        public void ExecuteException_RequeueThenDeleteAfterThreshold()
        {
            const int NumIterations = 13;
            consumerSettings.FailedJobHandling = FailedJobHandling.RequeueThenDeleteAfterThreshold;
            consumerSettings.FailedJobRequeueThreshold = NumIterations;

            Guid guid = Guid.NewGuid();
            string msgBody = "{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}";
            AddMessage(msgBody);
            CloudQueueMessage msg;

            for (int i = 0; i < NumIterations; i++)
            {
                // Verify that the message is currently queued.
                msg = queue.PeekMessage();
                Assert.That(msg, Is.Not.Null, "Iteration: {0}", i);
                Assert.That(msg.AsString, Is.EqualTo("{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}"),
                    "Iteration: {0}", i);
                Assert.That(msg.DequeueCount, Is.EqualTo(i), "Iteration: {0}", i);

                // Dequeue, and try to handle.
                context = JobExecutionContext.Dequeue(this);

                MockJob.Callback = mockJob =>
                {
                    Assert.That(mockJob.Id, Is.EqualTo(guid));

                    throw new InvalidOperationException(guid.ToString());
                };

                Assert.That(context.Empty, Is.False);
                Assert.Throws(Is.TypeOf<InvalidOperationException>().And.Message.EqualTo(guid.ToString()),
                    () => context.Execute());
                Assert.That(context.JobName, Is.EqualTo("MockJob"));
                Assert.That(context.WasSuccessful, Is.False);

                context.Dispose();
            }

            // Verify that the message NOT queued again.
            msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
        }

        [Test]
        public void ExecuteException_Delete()
        {
            consumerSettings.FailedJobHandling = FailedJobHandling.Delete;

            Guid guid = Guid.NewGuid();
            string msgBody = "{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}";
            AddMessage(msgBody);
            context = JobExecutionContext.Dequeue(this);

            MockJob.Callback = mockJob =>
            {
                Assert.That(mockJob.Id, Is.EqualTo(guid));

                throw new InvalidOperationException(guid.ToString());
            };

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<InvalidOperationException>().And.Message.EqualTo(guid.ToString()),
                () => context.Execute());
            Assert.That(context.JobName, Is.EqualTo("MockJob"));
            Assert.That(context.WasSuccessful, Is.False);

            context.Dispose();

            // Verify that the message NOT queued again.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Null);
        }

        [Test]
        public void ExecuteException_Unknown()
        {
            consumerSettings.FailedJobHandling = (FailedJobHandling)99;

            Guid guid = Guid.NewGuid();
            string msgBody = "{\"Job\":\"MockJob\",\"Properties\":{\"Id\":\"" + guid + "\"}}";
            AddMessage(msgBody);
            context = JobExecutionContext.Dequeue(this);

            MockJob.Callback = mockJob =>
            {
                Assert.That(mockJob.Id, Is.EqualTo(guid));

                throw new InvalidOperationException(guid.ToString());
            };

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And.Message.EqualTo("Unsupported FailedJobHandling: 99"),
                () => context.Execute());

            context.Dispose();

            // Verify that the message was not deleted.
            CloudQueueMessage msg = queue.GetMessage();
            Assert.That(msg, Is.Not.Null);
            Assert.That(msg.AsString, Is.EqualTo(msgBody));
        }

        [Test]
        public void ExecuteException_Ask()
        {
            consumerSettings.FailedJobHandling = FailedJobHandling.DedicePerJob;
            consumerSettings.FailedJobHandlingProvider = (job, descriptor, exception) =>
            {
                Assert.That(job, Is.InstanceOf<MockJob>());
                Assert.That(descriptor, Is.Not.Null);
                Assert.That(exception, Is.Not.Null.And.InstanceOf<InvalidOperationException>());

                MockJob mockJob = (MockJob)job;

                switch (mockJob.Int32)
                {
                    case 1:
                        return FailedJobHandling.Delete;
                    case 2:
                        return FailedJobHandling.Requeue;
                    case 3:
                        return (FailedJobHandling)999;

                    default:
                        return FailedJobHandling.DedicePerJob;
                }
            };

            MockJob.Callback = mockJob =>
            {
                throw new InvalidOperationException(mockJob.Int32.ToString());
            };

            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Int32\":1}}");
            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Int32\":2}}");
            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Int32\":3}}");
            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Int32\":4}}");
            AddMessage("{\"Job\":\"MockJob\",\"Properties\":{\"Int32\":5}}");

            // Dequeue the first message -- it should be deleted.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<InvalidOperationException>().And.Message.EqualTo("1"), () => context.Execute());

            context.Dispose();

            // Dequeue the second message -- it should be requeued again.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<InvalidOperationException>().And.Message.EqualTo("2"), () => context.Execute());

            context.Dispose();

            // Dequeue the third message -- it should cause an exception because an unknown behavior is configured.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And.Message.EqualTo("Unsupported FailedJobHandling: 999"),
                () => context.Execute());

            context.Dispose();

            // Dequeue the fourth message -- it should cause an exception because the decision provider returned an unspported value.
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<NotSupportedException>().And
                .Message.EqualTo("Unsupported FailedJobHandling: DedicePerJob"),
                () => context.Execute());

            context.Dispose();

            // Dequeue the fifth message -- it should cause an exception because the decision provider is missing.
            consumerSettings.FailedJobHandlingProvider = null;
            context = JobExecutionContext.Dequeue(this);

            Assert.That(context.Empty, Is.False);
            Assert.Throws(Is.TypeOf<InvalidOperationException>().And
                .Message.EqualTo("The FailedJobHandlingProvider must not be null when 'DedicePerJob' is used."),
                () => context.Execute());

            context.Dispose();

            // Verify that the message NOT queued again.
            CloudQueueMessage message = queue.GetMessage();
            Assert.That(message, Is.Not.Null);
            Assert.That(message.AsString, Is.EqualTo("{\"Job\":\"MockJob\",\"Properties\":{\"Int32\":2}}"));
        }

        #endregion

        #endregion

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
