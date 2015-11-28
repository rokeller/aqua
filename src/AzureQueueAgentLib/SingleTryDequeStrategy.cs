using System;

namespace Aqua
{
    /// <summary>
    /// Implements IDequeStrategy which does not allow any retries at all.
    /// </summary>
    public sealed class SingleTryDequeStrategy : IDequeStrategy
    {
        /// <summary>
        /// The default instance of SingleTryDequeStrategy.
        /// </summary>
        public static readonly SingleTryDequeStrategy Default = new SingleTryDequeStrategy();

        /// <summary>
        /// Initializes a new instance of SingleTryDequeStrategy.
        /// </summary>
        private SingleTryDequeStrategy() { }

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
            return false;
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
            throw new NotSupportedException();
        }
    }
}
