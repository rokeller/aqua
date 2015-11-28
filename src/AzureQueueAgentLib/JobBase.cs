using System;

namespace Aqua
{
    /// <summary>
    /// Light-weight base class for jobs whose execution might through exceptions.
    /// </summary>
    public abstract class JobBase : IJob
    {
        /// <summary>
        /// Executes the job.
        /// </summary>
        /// <returns>
        /// True if the job executed successfully and the message defining the job should be removed from the queue,
        /// or false otherwise.
        /// </returns>
        public bool Execute()
        {
            bool executed;

            try
            {
                executed = ExecuteJob();
            }
            catch (Exception ex)
            {
                // TODO: Log exception

                // Check if the job should be deleted despite the exception.
                executed = ShouldDeleteJob(ex);
            }

            return executed;
        }

        #region Protected Interface

        /// <summary>
        /// Actually executes the job.
        /// </summary>
        /// <returns>
        /// True if the job executed successfully and the message defining the job should be removed from the queue,
        /// or false otherwise.
        /// </returns>
        protected abstract bool ExecuteJob();

        /// <summary>
        /// Checks if the job should be deleted from the queue despite the given exception.
        /// </summary>
        /// <param name="ex">
        /// The Exception that was raised during the job's execution.
        /// </param>
        /// <returns>
        /// True if the job should be deleted from the queue, false otherwise.
        /// </returns>
        /// <remarks>
        /// The default implementation always returns false, i.e. all failing jobs will be re-queued.
        /// </remarks>
        protected virtual bool ShouldDeleteJob(Exception ex)
        {
            return false;
        }

        #endregion
    }
}
