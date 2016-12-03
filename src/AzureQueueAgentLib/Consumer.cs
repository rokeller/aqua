using Microsoft.WindowsAzure.Storage.Queue;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Aqua
{
    /// <summary>
    /// Provides a consumer for messages defining Aqua-based jobs in Azure Queues.
    /// </summary>
    public sealed class Consumer : IServiceProvider
    {
        #region Fields

        /// <summary>
        /// The ConnectionSettings to use to interact with the queue.
        /// </summary>
        private readonly ConnectionSettings connectionSettings;

        /// <summary>
        /// The CloudQueue to consume messages from.
        /// </summary>
        private readonly CloudQueue queue;

        /// <summary>
        /// The JobFactory to use to create instances of IJob from JobDescriptor objects.
        /// </summary>
        private readonly JobFactory factory;

        /// <summary>
        /// The ConsumerSettings that define the behavior of the consumer.
        /// </summary>
        private readonly ConsumerSettings consumerSettings;

        #endregion

        #region C'tors

        /// <summary>
        /// Initializes a new instance of Consumer using the given connectionSettings to connect to the Queue, and
        /// using the default ConsumerSettings.
        /// </summary>
        /// <param name="connectionSettings">
        /// The ConnectionSettings which define the Azure Storage Account and Queue to use.
        /// </param>
        /// <param name="factory">
        /// The JobFactory to use to create instances of IJob.
        /// </param>
        public Consumer(ConnectionSettings connectionSettings, JobFactory factory)
            : this(connectionSettings, factory, ConsumerSettings.CreateDefault())
        {
        }

        /// <summary>
        /// Initializes a new instance of Consumer using the given connectionSettings to connect to the Queue.
        /// </summary>
        /// <param name="connectionSettings">
        /// The ConnectionSettings which define the Storage Account and Queue to use.
        /// </param>
        /// <param name="factory">
        /// The JobFactory to use to create instances of IJob.
        /// </param>
        /// <param name="settings">
        /// The ConsumerSettings to use. If null, the default ConsumerSettings will be used.
        /// </param>
        public Consumer(ConnectionSettings connectionSettings, JobFactory factory, ConsumerSettings settings)
        {
            if (null == connectionSettings)
            {
                throw new ArgumentNullException("connectionSettings");
            }
            else if (null == factory)
            {
                throw new ArgumentNullException("factory");
            }

            queue = connectionSettings.GetQueue();
            queue.CreateIfNotExists();

            this.connectionSettings = connectionSettings;
            this.factory = factory;
            this.consumerSettings = settings ?? ConsumerSettings.CreateDefault();
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Consumes one message from the queue. Returns immediately if the queue has no messages.
        /// </summary>
        /// <returns>
        /// True if a message was successfully consumed, false otherwise.
        /// </returns>
        public bool One()
        {
            return One(NoRetryStrategy.Default);
        }

        /// <summary>
        /// Consumes one message from the queue, applying the given IRetryStrategy to wait for a message if the queue
        /// is empty.
        /// </summary>
        /// <param name="dequeueStrategy">
        /// An instance of IRetryStrategy which defines how long and how often to query the queue for a single message.
        /// </param>
        /// <returns>
        /// True if a message was successfully consumed, false otherwise.
        /// </returns>
        public bool One(IRetryStrategy dequeueStrategy)
        {
            return One(dequeueStrategy, CancellationToken.None);
        }

        /// <summary>
        /// Consumes one message from the queue, applying the given IRetryStrategy to wait for a message if the queue
        /// is empty.
        /// </summary>
        /// <param name="dequeueStrategy">
        /// An instance of IRetryStrategy which defines how long and how often to query the queue for a single message.
        /// </param>
        /// <param name="cancellationToken">
        /// A CancellationToken to use to check if the operation should be cancelled.
        /// </param>
        /// <returns>
        /// True if a message was successfully consumed, false otherwise.
        /// </returns>
        public bool One(IRetryStrategy dequeueStrategy, CancellationToken cancellationToken)
        {
            if (null == dequeueStrategy)
            {
                throw new ArgumentNullException("dequeueStrategy");
            }

            for (int i = 1; ; i++)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return false;
                }

                using (JobExecutionContext context = JobExecutionContext.Dequeue(this))
                {
                    if (context.Empty)
                    {
                        if (dequeueStrategy.ShouldRetry(i))
                        {
                            Task.Delay(dequeueStrategy.GetWaitTime(i), cancellationToken)
                                .ContinueWith(NoopTaskContinuation)
                                .Wait();

                            continue;
                        }

                        break;
                    }

                    context.Execute();

                    return true;
                }
            }

            return false;
        }

        #endregion

        #region IServiceProvider Implementation

        /// <summary>
        /// Gets the service object of the specified type.
        /// </summary>
        /// <param name="serviceType">
        /// An object that specifies the type of service object to get.
        /// </param>
        /// <returns>
        /// A service object of type serviceType.-or- null if there is no service object of type serviceType.
        /// </returns>
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

        #endregion

        #region Private Methods

        /// <summary>
        /// A simple method which does nothing.
        /// </summary>
        /// <param name="task">
        /// A Task.
        /// </param>
        /// <remarks>
        /// This method can be used as a continuation in order to avoid exceptions on error or cancel.
        /// </remarks>
        private static void NoopTaskContinuation(Task task)
        {
            // Intentionally left blank.
        }

        #endregion
    }
}
