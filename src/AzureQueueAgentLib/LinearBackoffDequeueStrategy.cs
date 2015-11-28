using System;

namespace Aqua
{
    /// <summary>
    /// Implements IDequeStrategy with linear back off based on a maximum number of retries and a base wait time.
    /// </summary>
    public sealed class LinearBackoffDequeueStrategy : BackoffDequeueStrategyBase
    {
        #region C'tors

        /// <summary>
        /// Initializes a new instance of LinearBackoffDequeueStrategy using the given maximum number of retries and
        /// the specified baseWaitTime.
        /// </summary>
        /// <param name="retryCount">
        /// The maximum number of retries to allow. Passing 0 implies no retries, i.e. a single try to dequeue a message
        /// is used.
        /// </param>
        /// <param name="baseWaitTime">
        /// A TimeSpan value which defines the base wait time, i.e. the wait time before the first retry. For the
        /// second, retry this strategy will wait twice as long, for the third, 3 times as long and so on.
        /// </param>
        public LinearBackoffDequeueStrategy(int retryCount, TimeSpan baseWaitTime) :
            base(retryCount, GetWaitTimes(retryCount, baseWaitTime))
        {
        }

        #endregion

        /// <summary>
        /// Pre-calculates the linear growing wait times for the different attempts.
        /// </summary>
        /// <param name="retryCount">
        /// The maximum number of retries to calculate the wait times for.
        /// </param>
        /// <param name="baseWaitTime">
        /// A TimeSpan value which defines the base wait time, i.e. the wait time before the first retry. For the
        /// second, retry this strategy will wait twice as long, for the third, 3 times as long and so on.
        /// </param>
        /// <returns>
        /// An array of TimeSpan values of length retryCount.
        /// </returns>
        private static TimeSpan[] GetWaitTimes(int retryCount, TimeSpan baseWaitTime)
        {
            if (retryCount < 0)
            {
                throw new ArgumentOutOfRangeException("retryCount");
            }
            else if (baseWaitTime.TotalMilliseconds < 0)
            {
                throw new ArgumentOutOfRangeException("baseWaitTime");
            }

            TimeSpan[] waitTimes = new TimeSpan[retryCount];

            waitTimes[0] = baseWaitTime;

            for (int i = 1; i < waitTimes.Length; i++)
            {
                waitTimes[i] = waitTimes[i - 1].Add(baseWaitTime);
            }

            return waitTimes;
        }
    }
}
