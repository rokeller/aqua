using Microsoft.WindowsAzure.Storage.Queue;
using System;

namespace Aqua
{
    /// <summary>
    /// Defines settings to control a Consumer's behavior.
    /// </summary>
    public class ConsumerSettings
    {
        #region Bad Message Behavior

        /// <summary>
        /// Gets or sets a BadMessageHandling value which defines the behavior for bad messages consumed from the queue.
        /// Defaults to 'Requeue' when using CreateDefault().
        /// </summary>
        public BadMessageHandling BadMessageHandling { get; set; }

        /// <summary>
        /// Gets or sets a function which is used to decide at runtime the behavior for bad messages consumed from the
        /// queue. It is ignored unless BadMessageHandling is set to 'DecidePerMessage', and it is required if
        /// 'DecidePerMessage' is used.
        /// </summary>
        public Func<CloudQueueMessage, BadMessageHandling> BadMessageHandlingProvider { get; set; }

        /// <summary>
        /// Gets or sets the threshold for number of dequeues of a message before a bad message gets deleted from the
        /// queue.
        /// </summary>
        public int BadMessageRequeueThreshold { get; set; }

        /// <summary>
        /// Gets or sets the visibility timeout to use when re-queueing bad messages.
        /// </summary>
        public TimeSpan BadMessageRequeueTimeout { get; set; }

        #endregion

        #region Unknown Job Behavior

        /// <summary>
        /// Gets or sets a UnknownJobHandling value which defines the behavior for unknown jobs consumed from the queue.
        /// Defaults to 'Requeue' when using CreateDefault().
        /// </summary>
        public UnknownJobHandling UnknownJobHandling { get; set; }

        /// <summary>
        /// Gets or sets a function which is used to decide at runtime the behavior for bad jobs consumed from the queue.
        /// It is ignored unless UnknownJobHandling is set to 'DecidePerJob', and it is required if 'DecidePerJob' is
        /// used.
        /// </summary>
        public Func<JobDescriptor, UnknownJobHandling> UnknownJobHandlingProvider { get; set; }

        /// <summary>
        /// Gets or sets the threshold for number of dequeues of a message before a message with an unknown job gets
        /// deleted from the queue.
        /// </summary>
        public int UnknownJobRequeueThreshold { get; set; }

        /// <summary>
        /// Gets or sets the visibility timeout to use when re-queueing messages with unknown jobs.
        /// </summary>
        public TimeSpan UnknownJobRequeueTimeout { get; set; }

        #endregion

        #region Failed Job Behavior

        /// <summary>
        /// Gets or sets a FailedJobHandling value which defines the behavior for failed jobs consumed from the queue.
        /// Defaults to 'Requeue' when using CreateDefault().
        /// </summary>
        public FailedJobHandling FailedJobHandling { get; set; }

        /// <summary>
        /// Gets or sets a function which is used to decide at runtime the behavior for failed jobs consumed from the
        /// queue. It is ignored unless FailedJobHandling is set to 'DecidePerJob', and it is required if 'DecidePerJob'
        /// is used.
        /// </summary>
        public Func<IJob, JobDescriptor, Exception, FailedJobHandling> FailedJobHandlingProvider { get; set; }

        /// <summary>
        /// Gets or sets the threshold for number of dequeues of a message before a message with a failed job gets
        /// deleted from the queue.
        /// </summary>
        public int FailedJobRequeueThreshold { get; set; }

        /// <summary>
        /// Gets or sets the visibility timeout to use when re-queueing messages with failed jobs.
        /// </summary>
        public TimeSpan FailedJobRequeueTimeout { get; set; }

        #endregion

        /// <summary>
        /// Creates a new instance of ConsumerSettings using the default values.
        /// </summary>
        /// <returns>
        /// An instance of ConsumerSettings using the default values.
        /// </returns>
        public static ConsumerSettings CreateDefault()
        {
            return new ConsumerSettings()
            {
                BadMessageHandling = BadMessageHandling.Requeue,
                BadMessageHandlingProvider = null,
                BadMessageRequeueThreshold = Int32.MaxValue,
                BadMessageRequeueTimeout = TimeSpan.Zero,

                UnknownJobHandling = UnknownJobHandling.Requeue,
                UnknownJobHandlingProvider = null,
                UnknownJobRequeueThreshold = Int32.MaxValue,
                UnknownJobRequeueTimeout = TimeSpan.Zero,

                FailedJobHandling = FailedJobHandling.Requeue,
                FailedJobHandlingProvider = null,
                FailedJobRequeueThreshold = Int32.MaxValue,
                FailedJobRequeueTimeout = TimeSpan.Zero,
            };
        }
    }

    /// <summary>
    /// Defines the available behaviors to handle bad messages.
    /// </summary>
    public enum BadMessageHandling
    {
        /// <summary>
        /// Requeue the bad message in the Azure Queue. The message still counts as consumed.
        /// </summary>
        Requeue,

        /// <summary>
        /// Requeue the bad message in the Azure Queue unless the dequeue count has reached a certain threshold.
        /// </summary>
        RequeueThenDeleteAfterThreshold,

        /// <summary>
        /// Delete the bad message from the Azure Queue. The message still counts as consumed.
        /// </summary>
        Delete,

        /// <summary>
        /// Decide per message, using the BadMessageHandlingProvider from the settings.
        /// </summary>
        DecidePerMessage,
    }

    /// <summary>
    /// Defines the available behaviors to handle unknown jobs.
    /// </summary>
    public enum UnknownJobHandling
    {
        /// <summary>
        /// Requeue the message that contains the unknown job in the Azure Queue. The message still counts as consumed.
        /// </summary>
        Requeue,

        /// <summary>
        /// Requeue the message that contains the unknown job in the Azure Queue unless the dequeue count has reached a
        /// certain threshold.
        /// </summary>
        RequeueThenDeleteAfterThreshold,

        /// <summary>
        /// Delete the message that contains the unknown job from the Azure Queue. The message still counts as consumed.
        /// </summary>
        Delete,

        /// <summary>
        /// Decide per job, using the UnknownJobHandlingProvider from the settings.
        /// </summary>
        DedicePerJob,
    }

    /// <summary>
    /// Defines the available behaviors to handle failed jobs.
    /// </summary>
    public enum FailedJobHandling
    {
        /// <summary>
        /// Requeue the message that contains the failed job in the Azure Queue.
        /// </summary>
        Requeue,

        /// <summary>
        /// Requeue the message that contains the failed job in the Azure Queue unless the dequeue count has reached a
        /// certain threshold.
        /// </summary>
        RequeueThenDeleteAfterThreshold,

        /// <summary>
        /// Delete the message that contains the failed job from the Azure Queue.
        /// </summary>
        Delete,

        /// <summary>
        /// Decide per job, using the FailedJobHandlingProvider from the settings.
        /// </summary>
        DedicePerJob,
    }
}
