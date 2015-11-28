using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml.XPath;

namespace Aqua
{
    /// <summary>
    /// Defines the context for the execution of a single job.
    /// </summary>
    internal sealed class JobExecutionContext : IDisposable
    {
        #region Fields

        /// <summary>
        /// The JsonSerializerSettings to use when deserializing the job descriptor.
        /// </summary>
        private static readonly JsonSerializerSettings jsonSettings = new JsonSerializerSettings()
        {
            CheckAdditionalContent = true,
            MissingMemberHandling = MissingMemberHandling.Error,
        };

        /// <summary>
        /// A TimeSpan value which defines the default visibility timeout to use when dequeuing messages, and when
        /// updating the visibility timeout for long-running jobs.
        /// </summary>
        private static readonly TimeSpan VisibilityTimeout = TimeSpan.FromMinutes(1);

        /// <summary>
        /// A TimeSpan value which defines the period of time after which the visibility timeout of a dequeued message
        /// will be updated.
        /// </summary>
        private static readonly TimeSpan VisibilityTimeoutUpdatePeriod = VisibilityTimeout.Subtract(TimeSpan.FromSeconds(5));

        /// <summary>
        /// A CancellationTokenSource that is used to cancel pending requests at the end of execution.
        /// </summary>
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        /// <summary>
        /// The IServiceProvider to use.
        /// </summary>
        private readonly IServiceProvider serviceProvider;

        /// <summary>
        /// The CloudQueue to dequeue messages from.
        /// </summary>
        private readonly CloudQueue queue;

        /// <summary>
        /// The JobFactory to use to create instances of IJob from JobDescriptor objects.
        /// </summary>
        private readonly JobFactory factory;

        /// <summary>
        /// The ConsumerSettings to use when consuming messages.
        /// </summary>
        private readonly ConsumerSettings agentSettings;

        /// <summary>
        /// The CloudQueueMessage which defines the dequeued message.
        /// </summary>
        private CloudQueueMessage message;

        /// <summary>
        /// The lazy-loaded IJob that is tracked by this context.
        /// </summary>
        private readonly Lazy<IJob> job;

        /// <summary>
        /// A flag which indicates whether or not this context has already been disposed.
        /// </summary>
        private bool disposed = false;

        /// <summary>
        /// A flag which indicates whether or not the dequeued message should be deleted when disposing this context.
        /// </summary>
        private bool shouldDeleteMessage = false;

        #endregion

        /// <summary>
        /// Initializes a new instance of JobExecutionContext.
        /// </summary>
        /// <param name="serviceProvider">
        /// The IServiceProvider to use.
        /// </param>
        private JobExecutionContext(IServiceProvider serviceProvider)
        {
            Debug.Assert(null != serviceProvider, "The serviceProvider must not be null.");
            this.serviceProvider = serviceProvider;

            queue = serviceProvider.GetService<CloudQueue>();
            Debug.Assert(null != queue, "The queue must not be null.");

            factory = serviceProvider.GetService<JobFactory>();
            Debug.Assert(null != factory, "The factory must not be null.");

            agentSettings = serviceProvider.GetService<ConsumerSettings>();
            Debug.Assert(null != agentSettings, "The agent settings must not be null.");

            job = new Lazy<IJob>(GetJob);
        }

        #region IDisposable Implementation

        /// <summary>
        /// Disposes this instance.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region Internal Interface

        /// <summary>
        /// Gets a flag which indicates whether nothing was dequeued (i.e. the queue was empty).
        /// </summary>
        internal bool Empty { get { return null == message; } }

        /// <summary>
        /// Executes the job.
        /// </summary>
        internal void Execute()
        {
            Debug.Assert(!Empty, "Cannot execute an empty message.");

            IJob job = this.job.Value;

            try
            {
                RefreshVisibilityTimeout(null);
                shouldDeleteMessage = job.Execute();
            }
            catch (Exception)
            {
                // TODO: Log the exception
                shouldDeleteMessage = false;
            }
        }

        /// <summary>
        /// Tries to dequeue a message.
        /// </summary>
        /// <param name="serviceProvider">
        /// The IServiceProvider which is used to identify the queue to dequeue from.
        /// </param>
        /// <returns>
        /// An instance of JobExecutionContext tracking the dequeue operation.
        /// </returns>
        internal static JobExecutionContext Dequeue(IServiceProvider serviceProvider)
        {
            Debug.Assert(null != serviceProvider, "The serviceProvider must not be null.");

            return new JobExecutionContext(serviceProvider).Dequeue();
        }

        #endregion

        #region Private Methods

        private JobExecutionContext Dequeue()
        {
            message = queue.GetMessage(VisibilityTimeout);

            return this;
        }

        private IJob GetJob()
        {
            string body = GetMessageBody();
            JobDescriptor jobDesc;

            try
            {
                jobDesc = JsonConvert.DeserializeObject<JobDescriptor>(body, jsonSettings);
                jobDesc.QueueMessageId = message.Id;
            }
            catch (Exception ex)
            {
                ApplyBadMessageHandling();
                throw new MessageFormatException(message.Id, ex);
            }

            try
            {
                IJob job = factory.CreateJob(jobDesc);
                Debug.Assert(null != job, "The job must not be null.");

                return job;
            }
            catch (UnknownJobException)
            {
                ApplyUnknownJobHandling(jobDesc);
                throw;
            }
        }

        private string GetMessageBody()
        {
            Debug.Assert(null != message, "The message must not be null.");

            string body = message.AsString;

            if (body.StartsWith("<?xml version=\"1.0\""))
            {
                // This message is wrapped in an XML envelope, probably coming from the Azure Scheduler.
                // Unwrap it and return the body.
                XDocument doc = XDocument.Parse(body);
                body = doc.XPathSelectElement("/StorageQueueMessage/Message").Value;
            }

            return body;
        }

        private void RefreshVisibilityTimeout(Task task)
        {
            if (null != task)
            {
                if (task.IsCanceled)
                {
                    // Execution was cancelled, so no need to extend the visibility timeout or schedule another extension.
                    return;
                }

                Console.WriteLine("Extend visibility. Old = {0}, now = {1}", message.NextVisibleTime, DateTime.UtcNow);
                queue.UpdateMessage(message, VisibilityTimeout, MessageUpdateFields.Visibility);
                Console.WriteLine("Extended visibility. New = {0}, now = {1}", message.NextVisibleTime, DateTime.UtcNow);
            }

            Task.Delay(VisibilityTimeoutUpdatePeriod, cancellationTokenSource.Token)
                .ContinueWith(RefreshVisibilityTimeout, cancellationTokenSource.Token);
        }

        private void ApplyBadMessageHandling()
        {
            switch (agentSettings.BadMessageHandling)
            {
                case BadMessageHandling.Requeue:
                case BadMessageHandling.Delete:
                    ApplyBadMessageHandling(agentSettings.BadMessageHandling);
                    break;

                case BadMessageHandling.DecidePerMessage:
                    if (null == agentSettings.BadMessageHandlingProvider)
                    {
                        throw new InvalidOperationException("The BadMessageHandlingProvider must not be null when 'DecidePerMessage' is used.");
                    }

                    ApplyBadMessageHandling(agentSettings.BadMessageHandlingProvider(message));
                    break;

                default:
                    Debug.Fail("Unsupported BadMessageHandling: " + agentSettings.BadMessageHandling);
                    throw new NotSupportedException("Unsupported BadMessageHandling: " + agentSettings.BadMessageHandling);
            }
        }

        private void ApplyBadMessageHandling(BadMessageHandling handling)
        {
            switch (handling)
            {
                case BadMessageHandling.Requeue:
                    break;

                case BadMessageHandling.Delete:
                    shouldDeleteMessage = true;
                    break;

                case BadMessageHandling.DecidePerMessage:
                default:
                    Debug.Fail("Unsupported BadMessageHandling: " + handling);
                    throw new NotSupportedException("Unsupported BadMessageHandling: " + handling);
            }
        }

        private void ApplyUnknownJobHandling(JobDescriptor jobDesc)
        {
            Debug.Assert(null != jobDesc, "The job descriptor must not be null.");

            switch (agentSettings.UnknownJobHandling)
            {
                case UnknownJobHandling.Requeue:
                case UnknownJobHandling.Delete:
                    ApplyUnknownJobHandling(agentSettings.UnknownJobHandling);
                    break;

                case UnknownJobHandling.DedicePerJob:
                    if (null == agentSettings.UnknownJobHandlingProvider)
                    {
                        throw new InvalidOperationException("The UnknownJobHandlingProvider must not be null when 'DecidePerMessage' is used.");
                    }

                    ApplyUnknownJobHandling(agentSettings.UnknownJobHandlingProvider(jobDesc));
                    break;

                default:
                    Debug.Fail("Unsupported UnknownJobHandling: " + agentSettings.UnknownJobHandling);
                    throw new NotSupportedException("Unsupported UnknownJobHandling: " + agentSettings.UnknownJobHandling);
            }
        }

        private void ApplyUnknownJobHandling(UnknownJobHandling handling)
        {
            switch (handling)
            {
                case UnknownJobHandling.Requeue:
                    break;

                case UnknownJobHandling.Delete:
                    shouldDeleteMessage = true;
                    break;

                case UnknownJobHandling.DedicePerJob:
                default:
                    Debug.Fail("Unsupported UnknownJobHandling: " + handling);
                    throw new NotSupportedException("Unsupported UnknownJobHandling: " + handling);
            }
        }

        private void Dispose(bool disposing)
        {
            if (disposed)
            {
                return;
            }

            if (disposing)
            {
                // No managed resources to dispose yet.
            }

            UpdateQueue();

            disposed = true;
        }

        private void UpdateQueue()
        {
            if (null != message)
            {
                if (shouldDeleteMessage)
                {
                    queue.DeleteMessage(message);
                }
                else
                {
                    queue.UpdateMessage(message, TimeSpan.Zero, MessageUpdateFields.Visibility);
                }
            }

            // Cancel all remaining outstanding tasks and requests.
            cancellationTokenSource.Cancel();
        }

        #endregion
    }
}
