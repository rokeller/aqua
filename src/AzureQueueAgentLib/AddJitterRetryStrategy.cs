using System;

namespace Aqua
{
    /// <summary>
    /// Implements IRetryStrategy by adding a configurable variation to the wait times provided by another IRetryStrategy.
    /// </summary>
    public sealed class AddJitterRetryStrategy : IRetryStrategy
    {
        #region Fields

        /// <summary>
        /// The pseudo-random number generator used to add jitter.
        /// </summary>
        private static readonly Random rng = new Random();

        /// <summary>
        /// The IRetryStrategy to get the base wait times from.
        /// </summary>
        private readonly IRetryStrategy inner;

        /// <summary>
        /// The inclusive minimum (percentage) of the base wait time for the jitter range.
        /// </summary>
        private readonly double min;

        /// <summary>
        /// The exclusive maximum (percentage) of the base wait time for the jitter range.
        /// </summary>
        private readonly double max;

        #endregion

        #region C'tors

        /// <summary>
        /// Initializes a new instance of AddJitterRetryStrategy using a range of 0% (inclusive) to 100% (exclusive).
        /// </summary>
        /// <param name="inner">
        /// The IRetryStrategy to add the jitter to.
        /// </param>
        public AddJitterRetryStrategy(IRetryStrategy inner) : this(inner, 0, 1) { }

        /// <summary>
        /// Initializes a new instance of AddJitterRetryStrategy using a range of the given minPercent (inclusive) to
        /// 100% (exclusive).
        /// </summary>
        /// <param name="inner">
        /// The IRetryStrategy to add the jitter to.
        /// </param>
        /// <param name="minPercent">
        /// The inclusive minimum percentage of the range in which to vary the wait times. Must be &gt;= 0 and &lt; 1.
        /// </param>
        public AddJitterRetryStrategy(IRetryStrategy inner, double minPercent) : this(inner, minPercent, 1) { }

        /// <summary>
        /// Initializes a new instance of AddJitterRetryStrategy using a range of the given minPercent (inclusive) to
        /// maxPercent (exclusive).
        /// </summary>
        /// <param name="inner">
        /// The IRetryStrategy to add the jitter to.
        /// </param>
        /// <param name="minPercent">
        /// The inclusive minimum percentage of the range in which to vary the wait times. Must be &gt;= 0 and &lt;
        /// maxPercent.
        /// </param>
        /// <param name="maxPercent">
        /// The exclusive maximum percentage of the range in which to vary the wait times. Must be &gt; 0 and &gt;
        /// minPercent. Can be &gt; 1.
        /// </param>
        /// <remarks>
        /// E.g. by setting minPercent = 0.5 and maxPercent = 2, you can achieve a variation of the wait times from the
        /// inner IRetryStrategy in the range of 50% to 200% i.e. wait at least half as long and at most twice as long.
        /// </remarks>
        public AddJitterRetryStrategy(IRetryStrategy inner, double minPercent, double maxPercent)
        {
            if (null == inner)
            {
                throw new ArgumentNullException("inner");
            }
            else if (minPercent < 0)
            {
                throw new ArgumentOutOfRangeException("minPercent");
            }
            else if (maxPercent <= 0)
            {
                throw new ArgumentOutOfRangeException("maxPercent");
            }
            else if (minPercent >= maxPercent)
            {
                throw new ArgumentException("minPercent must be less than maxPercent.");
            }

            this.inner = inner;

            min = minPercent;
            max = maxPercent;
        }

        #endregion

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
            return inner.ShouldRetry(attempt);
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
            TimeSpan waitTime = inner.GetWaitTime(attempt);
            double random = rng.NextDouble();
            double multiplier = max - (random * (max - min));

            return new TimeSpan(Convert.ToInt64(waitTime.Ticks * multiplier));
        }
    }
}
