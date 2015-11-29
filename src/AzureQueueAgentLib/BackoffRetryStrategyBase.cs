using System;

namespace Aqua
{
    /// <summary>
    /// Implements IDequeStrategy with pre-calculated back off times for a maximum number of retries.
    /// </summary>
    public abstract class BackoffRetryStrategyBase : IRetryStrategy
    {
        #region Fields

        /// <summary>
        /// The pre-calculated wait times for the different attempts.
        /// </summary>
        private readonly TimeSpan[] waitTimes;

        #endregion

        #region C'tors

        /// <summary>
        /// Initializes a new instance of BackoffRetryStrategyBase using the given maximum retry count and
        /// pre-calculated wait times for the attempts.
        /// </summary>
        /// <param name="retryCount">
        /// The maximum number of retries to allow. Passing 0 implies no retries, i.e. a single try to dequeue a message
        /// is used.
        /// </param>
        /// <param name="waitTimes">
        /// The pre-calculated wait times for the attempts.
        /// </param>
        protected BackoffRetryStrategyBase(int retryCount, TimeSpan[] waitTimes)
        {
            if (null == waitTimes)
            {
                throw new ArgumentNullException("waitTimes");
            }
            else if (retryCount != waitTimes.Length)
            {
                throw new ArgumentOutOfRangeException("retryCount");
            }

            RetryCount = retryCount;
            this.waitTimes = waitTimes;
        }

        #endregion

        /// <summary>
        /// Gets or sets the maximum number of retries to allow.
        /// </summary>
        public int RetryCount { get; private set; }

        #region IDequeStrategy Implementation

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
            if (attempt < 1 || attempt > RetryCount)
            {
                throw new ArgumentOutOfRangeException("attempt");
            }

            return waitTimes[attempt - 1];
        }

        #endregion
    }
}
