using System;

namespace Aqua
{
    /// <summary>
    /// Defines a Job which prints a parametrized greeting on the console output.
    /// </summary>
    /// <remarks>
    /// Sample job descriptor (JSON):
    /// 
    /// {"Job":"HelloWho","Properties":{"Who":"World"}}
    /// </remarks>
    public sealed class HelloWho : JobBase
    {
        /// <summary>
        /// Gets or sets a strings which defines who to greet.
        /// </summary>
        public string Who { get; set; }

        /// <summary>
        /// Executes the job.
        /// </summary>
        /// <returns>
        /// True if the job executed successfully and the message defining the job should be removed from the queue,
        /// or false otherwise.
        /// </returns>
        protected override bool ExecuteJob()
        {
            Console.WriteLine("Hello, {0}!", Who);

            return true;
        }
    }
}
