using System;
using System.Runtime.Serialization;

namespace Aqua
{
    /// <summary>
    /// Defines the exception that is raised when a message describes a job that is not known to the current JobFactory.
    /// </summary>
    public sealed class UnknownJobException : Exception
    {
        #region C'tors

        /// <summary>
        /// Initializes a new instance of UnknownJobException.
        /// </summary>
        /// <param name="messageId">
        /// The ID of the message the job descriptor is from.
        /// </param>
        /// <param name="jobName">
        /// The name of the job that is not registered.
        /// </param>
        public UnknownJobException(string messageId, string jobName)
        {
            MessageId = messageId;
            JobName = jobName;
        }

        /// <summary>
        /// Initializes a new instance of UnknownJobException.
        /// </summary>
        /// <param name="info">
        /// The SerializationInfo to deserialize the details of the exception from.
        /// </param>
        /// <param name="context">
        /// The StreamingContext for the deserialization.
        /// </param>
        public UnknownJobException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            MessageId = info.GetString("MessageId");
            JobName = info.GetString("JobName");
        }

        #endregion

        /// <summary>
        /// Gets or sets the ID of the message the job descriptor is from.
        /// </summary>
        public string MessageId { get; private set; }

        /// <summary>
        /// Gets or sets the name of the job that is not registered.
        /// </summary>
        public string JobName { get; private set; }

        /// <summary>
        /// Gets the message string for this exception.
        /// </summary>
        public override string Message
        {
            get
            {
                return String.Format("The job '{0}' from message with ID '{1}' is unknown.", JobName, MessageId);
            }
        }

        /// <summary>
        /// Gets the object data for this exception for serialization.
        /// </summary>
        /// <param name="info">
        /// The SerializationInfo to serialize the details to.
        /// </param>
        /// <param name="context">
        /// The StreamingContext for the serialization.
        /// </param>
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            info.AddValue("MessageId", MessageId);
            info.AddValue("JobName", JobName);
        }
    }
}
