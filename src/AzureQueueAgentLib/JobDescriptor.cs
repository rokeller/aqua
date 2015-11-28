using Newtonsoft.Json.Linq;
using System.Collections.Generic;

namespace Aqua
{
    /// <summary>
    /// Describes a Job through its name and properties.
    /// </summary>
    public sealed class JobDescriptor
    {
        /// <summary>
        /// Gets or sets the ID of the underlying message from a Queue, if any.
        /// </summary>
        internal string QueueMessageId { get; set; }

        /// <summary>
        /// Gets or set the name of the Job.
        /// </summary>
        public string Job { get; set; }

        /// <summary>
        /// Gets or sets the properties of the job.
        /// </summary>
        public Dictionary<string, JToken> Properties { get; set; }
    }
}
