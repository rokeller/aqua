using System;

namespace Aqua
{
    /// <summary>
    /// Defines the contract for a Job that can be executed by a Consumer.
    /// </summary>
    public interface IJob
    {
        /// <summary>
        /// Executes the job.
        /// </summary>
        /// <returns>
        /// True if the job executed successfully and the message defining the job should be removed from the queue,
        /// or false otherwise.
        /// </returns>
        bool Execute();
    }
}
