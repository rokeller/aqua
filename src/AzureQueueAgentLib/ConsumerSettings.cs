using Microsoft.WindowsAzure.Storage.Queue;
using System;

namespace Aqua
{
    /// <summary>
    /// Defines settings to control a Consumer's behavior.
    /// </summary>
    public class ConsumerSettings
    {
        /// <summary>
        /// Gets or sets a BadMessageHandling value which defines the behavior for bad messages consumed from the queue.
        /// Defaults to 'Requeue'.
        /// </summary>
        public BadMessageHandling BadMessageHandling { get; set; }

        /// <summary>
        /// Gets or sets a function which is used to decide at runtime the behavior for bad messages consumed from the
        /// queue. It is ignored unless BadMessageHandling is set to 'DecidePerMessage', and it is required if
        /// 'DecidePerMessage' is used.
        /// </summary>
        public Func<CloudQueueMessage, BadMessageHandling> BadMessageHandlingProvider { get; set; }

        /// <summary>
        /// Gets or sets a UnknownJobHandling value which defines the behavior for unknown jobs consumed from the queue.
        /// Defaults to 'Requeue'.
        /// </summary>
        public UnknownJobHandling UnknownJobHandling { get; set; }

        /// <summary>
        /// Gets or sets a function which is used to decide at runtime the behavior for bad jobs consumed from the queue.
        /// It is ignored unless UnknownJobHandling is set to 'DecidePerJob', and it is required if 'DecidePerJob' is
        /// used.
        /// </summary>
        public Func<JobDescriptor, UnknownJobHandling> UnknownJobHandlingProvider { get; set; }

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
                UnknownJobHandling = UnknownJobHandling.Requeue,
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
        /// Delete the message that contains the unknown job from the Azure Queue. The message still counts as consumed.
        /// </summary>
        Delete,

        /// <summary>
        /// Decide per job, using the UnknownJobHandlingProvider from the settings.
        /// </summary>
        DedicePerJob,
    }
}
