﻿using Microsoft.WindowsAzure.Storage.Queue;
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
        /// Executes the job.
        /// </summary>
        /// <returns>
        /// True if the message from the context was correctly handled and dequeued, or false if the message was
        /// enqueued again.
        /// </returns>
        internal bool Execute()
        {
            Debug.Assert(!Empty, "Cannot execute an empty message.");

            IJob job = this.job.Value;

            try
            {
                RefreshVisibilityTimeout(null);
                return shouldDeleteMessage = job.Execute();
            }
            catch (Exception)
            {
                // TODO: Log the exception
                shouldDeleteMessage = false;
                throw;
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

        /// <summary>
        /// Dequeues a message from the current queue.
        /// </summary>
        /// <returns>
        /// A reference to this instance.
        /// </returns>
        private JobExecutionContext Dequeue()
        {
            message = queue.GetMessage(VisibilityTimeout);

            return this;
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

                queue.UpdateMessage(message, VisibilityTimeout, MessageUpdateFields.Visibility);
            }

            Task.Delay(VisibilityTimeoutUpdatePeriod, cancellationTokenSource.Token)
                .ContinueWith(RefreshVisibilityTimeout, cancellationTokenSource.Token);
        }

        /// <summary>
        /// Applies the BadMessageHandling behavior from the current ConsumerSettings.
        /// </summary>
        private void ApplyBadMessageHandling()
        {
            switch (consumerSettings.BadMessageHandling)
            {
                case BadMessageHandling.Requeue:
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
                    break;

                case BadMessageHandling.Delete:
                    shouldDeleteMessage = true;
                    break;

                case BadMessageHandling.DecidePerMessage:
                default:
                    throw new NotSupportedException("Unsupported BadMessageHandling: " + handling);
            }
        }

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
                case UnknownJobHandling.Delete:
                    ApplyUnknownJobHandling(consumerSettings.UnknownJobHandling);
                    break;

                case UnknownJobHandling.DedicePerJob:
                    if (null == consumerSettings.UnknownJobHandlingProvider)
                    {
                        throw new InvalidOperationException("The UnknownJobHandlingProvider must not be null when 'DedicePerJob' is used.");
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
                    break;

                case UnknownJobHandling.Delete:
                    shouldDeleteMessage = true;
                    break;

                case UnknownJobHandling.DedicePerJob:
                default:
                    throw new NotSupportedException("Unsupported UnknownJobHandling: " + handling);
            }
        }

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
