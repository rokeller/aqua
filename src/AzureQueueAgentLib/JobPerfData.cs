using System;
using System.Threading;

namespace Aqua
{
    /// <summary>
    /// Tracks information about job execution performance.
    /// </summary>
    public sealed class JobPerfData
    {
        #region Fields

        /// <summary>
        /// The number of times the job was executed successfully.
        /// </summary>
        private long successCount;

        /// <summary>
        /// The number of times the job was executed with failures.
        /// </summary>
        private long failureCount;

        /// <summary>
        /// The total duration (in milliseconds) spent on successful job executions.
        /// </summary>
        private long successDuration;

        /// <summary>
        /// The total duration (in milliseconds) spent on failed job executions.
        /// </summary>
        private long failureDuration;

        #endregion

        #region C'tors

        /// <summary>
        /// Initializes a new instance of JobPerfData.
        /// </summary>
        /// <param name="jobName">
        /// The name of the job this instance is tracking perf data for.
        /// </param>
        public JobPerfData(string jobName)
        {
            if (String.IsNullOrWhiteSpace(jobName))
            {
                throw new ArgumentNullException("jobName");
            }

            JobName = jobName;
        }

        #endregion

        /// <summary>
        /// Gets the name of the job this instance is tracking perf data for.
        /// </summary>
        public string JobName { get; private set; }

        /// <summary>
        /// Gets the number of times the job was executed successfully.
        /// </summary>
        public long SuccessCount { get { return successCount; } }

        /// <summary>
        /// Gets the number of times the job was executed with failures.
        /// </summary>
        public long FailureCount { get { return failureCount; } }

        /// <summary>
        /// Gets the total duration (in milliseconds) spent on successful job executions.
        /// </summary>
        public long SuccessDuration { get { return successDuration; } }

        /// <summary>
        /// Gets the total duration (in milliseconds) spent on failed job executions.
        /// </summary>
        public long FailureDuration { get { return failureDuration; } }

        #region Calculated Values

        /// <summary>
        /// Gets the success rate of the executions.
        /// </summary>
        public float SuccessRate
        {
            get
            {
                return successCount + failureCount > 0 ?
                  1f * successCount / (successCount + failureCount) :
                  0;
            }
        }

        /// <summary>
        /// Gets the average duration for successful executions.
        /// </summary>
        public float AverageSuccessDuration
        {
            get
            {
                return successCount > 0 ?
                  1f * successDuration / successCount :
                  0;
            }
        }

        /// <summary>
        /// Gets the average duration for successful executions.
        /// </summary>
        public float AverageFailureDuration
        {
            get
            {
                return failureCount > 0 ?
                  failureDuration * 1f / failureCount :
                  0;
            }
        }

        #endregion

        #region Internal Methods

        /// <summary>
        /// Updates this instance for a successful execution.
        /// </summary>
        /// <param name="duration">
        /// The duration of the execution.
        /// </param>
        internal void UpdateSuccess(long duration)
        {
            Interlocked.Increment(ref successCount);
            Interlocked.Add(ref successDuration, duration);
        }

        /// <summary>
        /// Updates this instance for a failed execution.
        /// </summary>
        /// <param name="duration">
        /// The duration of the execution.
        /// </param>
        internal void UpdateFailure(long duration)
        {
            Interlocked.Increment(ref failureCount);
            Interlocked.Add(ref failureDuration, duration);
        }

        #endregion
    }
}
