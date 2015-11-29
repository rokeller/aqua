using System;

namespace Aqua
{
    /// <summary>
    /// Implements IDequeStrategy with a maximum number of retries and a constant wait time between tries.
    /// </summary>
    public sealed class SimpleRetryStrategy : IRetryStrategy
    {
        /// <summary>
        /// Initializes a new instance of SimpleRetryStrategy.
        /// </summary>
        /// <param name="retryCount">
        /// The maximum number of retries to allow. Passing 0 implies no retries, i.e. a single try to dequeue a message
        /// is used.
        /// </param>
        /// <param name="waitTime">
        /// A TimeSpan value which defines the time to wait before retries.
        /// </param>
        public SimpleRetryStrategy(int retryCount, TimeSpan waitTime)
        {
            if (retryCount < 0)
            {
                throw new ArgumentOutOfRangeException("retryCount");
            }
            else if (waitTime.TotalMilliseconds < 0)
            {
                throw new ArgumentOutOfRangeException("waitTime");
            }

            RetryCount = retryCount;
            WaitTime = waitTime;
        }

        /// <summary>
        /// Gets or sets the maximum number of retries to allow.
        /// </summary>
        public int RetryCount { get; private set; }

        /// <summary>
        /// Gets or sets the TimeSpan value which defines how long to wait between tries.
        /// </summary>
        public TimeSpan WaitTime { get; private set; }

        /// <summary>
        /// Checks if we should retry after an unsuccessful attempt to dequeue a message.
        /// </summary>
        /// <param name="attempt">
        /// The number of attempts carried out so far. That is, after the first attempt (for the first retry), attempt
        /// will be set to 1, after the second attempt it is set to 2, and so on.
        /// </param>
        /// <returns>
        /// True if another attempt to dequeue a message should be made, false otherwise.
        /// </returns>
        public bool ShouldRetry(int attempt)
        {
            return attempt >= 1 && attempt <= RetryCount;
        }

        /// <summary>
        /// Gets a TimeSpan value which defines how long to wait before trying again after an unsuccessful attempt to
        /// dequeue a message.
        /// </summary>
        /// <param name="attempt">
        /// The number of attempts carried out so far. That is, after the first attempt (for the first retry), attempt
        /// will be set to 1, after the second attempt it is set to 2, and so on.
        /// </param>
        /// <returns>
        /// A TimeSpan value which defines how long to wait before the next attempt.
        /// </returns>
        public TimeSpan GetWaitTime(int attempt)
        {
            return WaitTime;
        }
    }
}
