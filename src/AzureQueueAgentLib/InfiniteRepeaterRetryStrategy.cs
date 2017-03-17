using System;
using System.Diagnostics;

namespace Aqua
{
    /// <summary>
    /// Implements IRetryStrategy that is based on another retry strategy from which it remembers its last wait time
    /// and repeats it virtually forever.
    /// </summary>
    public sealed class InfiniteRepeaterRetryStrategy : IRetryStrategy
    {
        #region Fields

        /// <summary>
        /// The IRetryStrategy to get (and remember) the wait times from.
        /// </summary>
        private readonly IRetryStrategy inner;

        /// <summary>
        /// The last remembered wait time.
        /// </summary>
        private TimeSpan? lastWaitTime;

        #endregion

        #region C'tors

        /// <summary>
        /// Initializes a new instance of InfiniteRepeaterRetryStrategy.
        /// </summary>
        /// <param name="inner">
        /// The IRetryStrategy to remember the last wait time infinitely.
        /// </param>
        public InfiniteRepeaterRetryStrategy(IRetryStrategy inner)
        {
            if (null == inner)
            {
                throw new ArgumentNullException("inner");
            }

            this.inner = inner;
        }

        #endregion

        #region IRetryStrategy Implementation

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
            return attempt > 0;
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
            if (inner.ShouldRetry(attempt))
            {
                lastWaitTime = inner.GetWaitTime(attempt);
            }

            if (!lastWaitTime.HasValue)
            {
                // We don't know the last wait time used by the inner strategy yet, so let's go and discover it.
                int lastSupportedAttempt = 1;

                while (inner.ShouldRetry(lastSupportedAttempt + 1))
                {
                    lastSupportedAttempt++;
                }

                lastWaitTime = inner.GetWaitTime(lastSupportedAttempt);
            }

            Debug.Assert(lastWaitTime.HasValue, "The last wait time must have a value.");

            return lastWaitTime.Value;
        }

        #endregion
    }
}
