using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using System;

namespace Aqua
{
    /// <summary>
    /// Provides a producer for messages defining Aqua-based jobs in Azure Queues.
    /// </summary>
    public sealed class Producer
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
        /// The JobFactory to use to serialize instances of IJob to JobDescriptor objects.
        /// </summary>
        private readonly JobFactory factory;

        #endregion

        #region C'tors

        /// <summary>
        /// Initializes a new instance of Producer using the given connectionSettings to connect to the Queue.
        /// </summary>
        /// <param name="connectionSettings">
        /// The ConnectionSettings which define the Storage Account and Queue to use.
        /// </param>
        /// <param name="factory">
        /// The JobFactory to use to serialize instances of IJob.
        /// </param>
        public Producer(ConnectionSettings connectionSettings, JobFactory factory)
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
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Produces one message for the given job and adds it to the queue.
        /// </summary>
        /// <param name="job">
        /// The IJob to produce.
        /// </param>
        public void One(IJob job)
        {
            One(job, null);
        }

        /// <summary>
        /// Produces one message for the given job and adds it to the queue.
        /// </summary>
        /// <param name="job">
        /// The IJob to produce.
        /// </param>
        /// <param name="initialVisibilityDelay">
        /// A TimeSpan value specifying the interval of time from now during which the message will be invisible. If
        /// null then the message will be visible immediately.
        /// </param>
        public void One(IJob job, TimeSpan? initialVisibilityDelay)
        {
            if (null == job)
            {
                throw new ArgumentNullException("job");
            }

            One(factory.CreateDescriptor(job), initialVisibilityDelay);
        }

        /// <summary>
        /// Produces one message for the given job descriptor and adds it to the queue.
        /// </summary>
        /// <param name="descriptor">
        /// The JobDescriptor which describes the job to produce.
        /// </param>
        public void One(JobDescriptor descriptor)
        {
            One(descriptor, null);
        }

        /// <summary>
        /// Produces one message for the given job descriptor and adds it to the queue.
        /// </summary>
        /// <param name="descriptor">
        /// The JobDescriptor which describes the job to produce.
        /// </param>
        /// <param name="initialVisibilityDelay">
        /// A TimeSpan value specifying the interval of time from now during which the message will be invisible. If
        /// null then the message will be visible immediately.
        /// </param>
        public void One(JobDescriptor descriptor, TimeSpan? initialVisibilityDelay)
        {
            if (null == descriptor)
            {
                throw new ArgumentNullException("descriptor");
            }
            else if (String.IsNullOrWhiteSpace(descriptor.Job))
            {
                throw new ArgumentException("The JobDescriptor must have a non-null and non-blank Job property.");
            }

            string body = JsonConvert.SerializeObject(descriptor);
            CloudQueueMessage msg = new CloudQueueMessage(body);

            queue.AddMessage(msg, /* timeToLive */ null, initialVisibilityDelay);
        }

        #endregion
    }
}
