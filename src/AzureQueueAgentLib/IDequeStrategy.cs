using System;

namespace Aqua
{
    /// <summary>
    /// Defines the contract for a strategy to dequeue messages from an Azure Storage Account Queue.
    /// </summary>
    public interface IDequeStrategy
    {
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
        bool ShouldRetry(int attempt);

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
        TimeSpan GetWaitTime(int attempt);
    }
}
