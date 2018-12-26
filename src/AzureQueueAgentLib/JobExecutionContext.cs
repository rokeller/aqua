using Microsoft.WindowsAzure.Storage;
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
        private readonly ConsumerSettings consumerSettings;

        /// <summary>
        /// The CloudQueueMessage which defines the dequeued message.
        /// </summary>
        private CloudQueueMessage message;

        /// <summary>
        /// The lazy-loaded JobDescriptor describing the job, if any.
        /// </summary>
        private readonly Lazy<JobDescriptor> jobDescriptor;

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

        /// <summary>
        /// The visibility timeout to use when requeueing a message.
        /// </summary>
        private TimeSpan requeueVisibilityTimeout = TimeSpan.Zero;

        #endregion

        #region C'tors

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

            consumerSettings = serviceProvider.GetService<ConsumerSettings>();
            Debug.Assert(null != consumerSettings, "The consumer settings must not be null.");

            jobDescriptor = new Lazy<JobDescriptor>(GetJobDescriptor);
            job = new Lazy<IJob>(GetJob);
        }

        #endregion

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
        /// Gets the name of the job to be executed with this instance.
        /// </summary>
        internal string JobName { get { return jobDescriptor.Value.Job; } }

        /// <summary>
        /// Gets a flag which indicates whether or not job execution was successful.
        /// </summary>
        internal bool WasSuccessful { get; private set; }

        /// <summary>
        /// Executes the job.
        /// </summary>
        /// <returns>
        /// True if the message from the context was correctly handled and dequeued, or false if the message was
        /// enqueued again.
        /// </returns>
        internal bool Execute()
        {
            Debug.Assert(!Empty, "Cannot execute an empty message.");

            IJob job = null;

            try
            {
                job = this.job.Value;
                RefreshVisibilityTimeout(task: null);
                WasSuccessful = job.Execute();
            }
            catch (MessageFormatException)
            {
                ApplyBadMessageHandling();
                throw;
            }
            catch (UnknownJobException)
            {
                ApplyUnknownJobHandling(jobDescriptor.Value);
                throw;
            }
            catch (Exception ex)
            {
                // TODO: Log the exception
                WasSuccessful = false;
                ApplyFailedJobHandling(job, jobDescriptor.Value, ex);
                throw;
            }

            // If execution was not successful, apply the 'FailedJobHandling' behavior now.
            if (!WasSuccessful)
            {
                ApplyFailedJobHandling(job, jobDescriptor.Value, null);
            }
            else
            {
                // Since the job was handled successfully, we want to delete the message in any case.
                shouldDeleteMessage = true;
            }

            return WasSuccessful;
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

        /// <summary>
        /// Dequeues a message from the current queue.
        /// </summary>
        /// <returns>
        /// A reference to this instance.
        /// </returns>
        private JobExecutionContext Dequeue()
        {
            message = queue.GetMessageAsync(VisibilityTimeout, null, null).GetAwaiter().GetResult();

            return this;
        }

        /// <summary>
        /// Gets the JobDescriptor from the message.
        /// </summary>
        /// <returns>
        /// An instance of JobDescriptor.
        /// </returns>
        private JobDescriptor GetJobDescriptor()
        {
            JobDescriptor jobDesc;

            try
            {
                string body = GetMessageBody();

                jobDesc = JsonConvert.DeserializeObject<JobDescriptor>(body, jsonSettings);
                jobDesc.QueueMessageId = message.Id;
                jobDesc.DequeueCount = message.DequeueCount;
            }
            catch (Exception ex)
            {
                throw new MessageFormatException(message.Id, ex);
            }

            return jobDesc;
        }

        /// <summary>
        /// Gets the IJob for the current message.
        /// </summary>
        /// <returns>
        /// An instance of IJob.
        /// </returns>
        /// <exception cref="MessageFormatException">
        /// A MessageFormatException is thrown if the message is badly formatted.
        /// </exception>
        /// <exception cref="UnknownJobException">
        /// An UnknownJobException is thrown if the job from the message is not registered in the current JobFactory.
        /// </exception>
        private IJob GetJob()
        {
            IJob job = factory.CreateJob(jobDescriptor.Value);
            Debug.Assert(null != job, "The job must not be null.");

            return job;
        }

        /// <summary>
        /// Gets the message string from the body of the current message.
        /// </summary>
        /// <returns>
        /// A string which contains the body of the current message.
        /// </returns>
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

        /// <summary>
        /// Periodically refreshes the visibility timeout for the current message in the queue to prevent other
        /// consumers from seeing and dequeueing the message.
        /// </summary>
        /// <param name="task">
        /// The Task which tracked the previous wait, or null if this is the first attempt at refreshing the visibility
        /// timeout.
        /// </param>
        private void RefreshVisibilityTimeout(Task task)
        {
            if (null != task)
            {
                if (task.IsCanceled)
                {
                    // Execution was cancelled, so no need to extend the visibility timeout or schedule another extension.
                    return;
                }

                queue.UpdateMessageAsync(message, VisibilityTimeout, MessageUpdateFields.Visibility).GetAwaiter().GetResult();
            }

            Task.Delay(VisibilityTimeoutUpdatePeriod, cancellationTokenSource.Token)
                .ContinueWith(RefreshVisibilityTimeout, cancellationTokenSource.Token);
        }

        #region Bad Message Handling

        /// <summary>
        /// Applies the BadMessageHandling behavior from the current ConsumerSettings.
        /// </summary>
        private void ApplyBadMessageHandling()
        {
            switch (consumerSettings.BadMessageHandling)
            {
                case BadMessageHandling.Requeue:
                case BadMessageHandling.RequeueThenDeleteAfterThreshold:
                case BadMessageHandling.Delete:
                    ApplyBadMessageHandling(consumerSettings.BadMessageHandling);
                    break;

                case BadMessageHandling.DecidePerMessage:
                    if (null == consumerSettings.BadMessageHandlingProvider)
                    {
                        throw new InvalidOperationException("The BadMessageHandlingProvider must not be null when 'DecidePerMessage' is used.");
                    }

                    ApplyBadMessageHandling(consumerSettings.BadMessageHandlingProvider(message));
                    break;

                default:
                    throw new NotSupportedException("Unsupported BadMessageHandling: " + consumerSettings.BadMessageHandling);
            }
        }

        /// <summary>
        /// Applies the given BadMessageHandling to the current message.
        /// </summary>
        /// <param name="handling">
        /// The BadMessageHandling value to apply.
        /// </param>
        private void ApplyBadMessageHandling(BadMessageHandling handling)
        {
            switch (handling)
            {
                case BadMessageHandling.Requeue:
                    shouldDeleteMessage = false;
                    break;

                case BadMessageHandling.RequeueThenDeleteAfterThreshold:
                    shouldDeleteMessage = (message.DequeueCount >= consumerSettings.BadMessageRequeueThreshold);
                    break;

                case BadMessageHandling.Delete:
                    shouldDeleteMessage = true;
                    break;

                case BadMessageHandling.DecidePerMessage:
                default:
                    throw new NotSupportedException("Unsupported BadMessageHandling: " + handling);
            }

            requeueVisibilityTimeout = consumerSettings.BadMessageRequeueTimeout;
        }

        #endregion

        #region Unknown Job Handling

        /// <summary>
        /// Applies the UnknownJobHandling behavior from the current ConsumerSettings.
        /// </summary>
        /// <param name="jobDesc">
        /// The JobDescriptor that describes the unknown job.
        /// </param>
        private void ApplyUnknownJobHandling(JobDescriptor jobDesc)
        {
            Debug.Assert(null != jobDesc, "The job descriptor must not be null.");

            switch (consumerSettings.UnknownJobHandling)
            {
                case UnknownJobHandling.Requeue:
                case UnknownJobHandling.RequeueThenDeleteAfterThreshold:
                case UnknownJobHandling.Delete:
                    ApplyUnknownJobHandling(consumerSettings.UnknownJobHandling);
                    break;

                case UnknownJobHandling.DecidePerJob:
                    if (null == consumerSettings.UnknownJobHandlingProvider)
                    {
                        throw new InvalidOperationException("The UnknownJobHandlingProvider must not be null when 'DecidePerJob' is used.");
                    }

                    ApplyUnknownJobHandling(consumerSettings.UnknownJobHandlingProvider(jobDesc));
                    break;

                default:
                    throw new NotSupportedException("Unsupported UnknownJobHandling: " + consumerSettings.UnknownJobHandling);
            }
        }

        /// <summary>
        /// Applies the given UnknownJobHandling behavior.
        /// </summary>
        /// <param name="handling">
        /// The UnknownJobHandling value to apply.
        /// </param>
        private void ApplyUnknownJobHandling(UnknownJobHandling handling)
        {
            switch (handling)
            {
                case UnknownJobHandling.Requeue:
                    shouldDeleteMessage = false;
                    break;

                case UnknownJobHandling.RequeueThenDeleteAfterThreshold:
                    shouldDeleteMessage = (message.DequeueCount >= consumerSettings.UnknownJobRequeueThreshold);
                    break;

                case UnknownJobHandling.Delete:
                    shouldDeleteMessage = true;
                    break;

                case UnknownJobHandling.DecidePerJob:
                default:
                    throw new NotSupportedException("Unsupported UnknownJobHandling: " + handling);
            }

            requeueVisibilityTimeout = consumerSettings.UnknownJobRequeueTimeout;
        }

        #endregion

        #region Failed Job Handling

        /// <summary>
        /// Applies the FailedJobHandling behavior from the current ConsumerSettings.
        /// </summary>
        /// <param name="job">
        /// The IJob for which execution failed.
        /// </param>
        /// <param name="jobDesc">
        /// The JobDescriptor that describes the failed job.
        /// </param>
        /// <param name="exception">
        /// The Exception which was raised, or null if no exception was raised.
        /// </param>
        private void ApplyFailedJobHandling(IJob job, JobDescriptor jobDesc, Exception exception)
        {
            Debug.Assert(null != job, "The job must not be null.");
            Debug.Assert(null != jobDesc, "The job descriptor must not be null.");

            switch (consumerSettings.FailedJobHandling)
            {
                case FailedJobHandling.Requeue:
                case FailedJobHandling.RequeueThenDeleteAfterThreshold:
                case FailedJobHandling.Delete:
                    ApplyFailedJobHandling(consumerSettings.FailedJobHandling);
                    break;

                case FailedJobHandling.DecidePerJob:
                    if (null == consumerSettings.FailedJobHandlingProvider)
                    {
                        throw new InvalidOperationException("The FailedJobHandlingProvider must not be null when 'DecidePerJob' is used.");
                    }

                    ApplyFailedJobHandling(consumerSettings.FailedJobHandlingProvider(job, jobDesc, exception));
                    break;

                default:
                    throw new NotSupportedException("Unsupported FailedJobHandling: " + consumerSettings.FailedJobHandling);
            }
        }

        /// <summary>
        /// Applies the given FailedJobHandling behavior.
        /// </summary>
        /// <param name="handling">
        /// The FailedJobHandling value to apply.
        /// </param>
        private void ApplyFailedJobHandling(FailedJobHandling handling)
        {
            switch (handling)
            {
                case FailedJobHandling.Requeue:
                    shouldDeleteMessage = false;
                    break;

                case FailedJobHandling.RequeueThenDeleteAfterThreshold:
                    shouldDeleteMessage = (message.DequeueCount >= consumerSettings.FailedJobRequeueThreshold);
                    break;

                case FailedJobHandling.Delete:
                    shouldDeleteMessage = true;
                    break;

                case FailedJobHandling.DecidePerJob:
                default:
                    shouldDeleteMessage = false;
                    throw new NotSupportedException("Unsupported FailedJobHandling: " + handling);
            }

            requeueVisibilityTimeout = consumerSettings.FailedJobRequeueTimeout;
        }

        #endregion

        /// <summary>
        /// Disposes this instance.
        /// </summary>
        /// <param name="disposing">
        /// A flag which indicates whether or not this call is from the Dispose or from the finalizer.
        /// </param>
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

        /// <summary>
        /// Updates the queue by either deleting the message from it, or by resetting the visibility timeout and thus
        /// effectively requeue the message in the queue.
        /// </summary>
        private void UpdateQueue()
        {
            if (null != message)
            {
                try
                {
                    Task t;

                    if (shouldDeleteMessage)
                    {
                        t = queue.DeleteMessageAsync(message);
                    }
                    else
                    {
                        // We should keep the message, so update just its visibility timeout.
                        t = queue.UpdateMessageAsync(message, requeueVisibilityTimeout, MessageUpdateFields.Visibility);
                    }

                    t.GetAwaiter().GetResult();
                }
                catch (StorageException ex)
                {
                    if (404 == ex.RequestInformation.HttpStatusCode)
                    {
                        // The message does not exist anymore, so there's nothing left for us to do here.
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            // Cancel all remaining outstanding tasks and requests.
            cancellationTokenSource.Cancel();
        }

        #endregion
    }
}
