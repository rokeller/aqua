using System;
using System.Runtime.Serialization;

namespace Aqua
{
    /// <summary>
    /// Defines the exception that is raised when a message's format is not understand and cannot be interpreted as a
    /// JobDescriptor.
    /// </summary>
    public sealed class MessageFormatException : Exception
    {
        /// <summary>
        /// Initializes a new instance of MessageFormatException.
        /// </summary>
        /// <param name="messageId">
        /// The ID of the message causing this exception.
        /// </param>
        public MessageFormatException(string messageId)
        {
            MessageId = messageId;
        }

        /// <summary>
        /// Initializes a new instance of MessageFormatException.
        /// </summary>
        /// <param name="messageId">
        /// The ID of the message causing this exception.
        /// </param>
        /// <param name="innerException">
        /// The Exception that caused this exception.
        /// </param>
        public MessageFormatException(string messageId, Exception innerException)
            : base(null, innerException)
        {
            MessageId = messageId;
        }

        /// <summary>
        /// Initializes a new instance of MessageFormatException.
        /// </summary>
        /// <param name="info">
        /// The SerializationInfo to deserialize the details of the exception from.
        /// </param>
        /// <param name="context">
        /// The StreamingContext for the deserialization.
        /// </param>
        public MessageFormatException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            MessageId = info.GetString("MessageId");
        }

        /// <summary>
        /// Gets or sets the ID of the message causing this exception.
        /// </summary>
        public string MessageId { get; private set; }

        /// <summary>
        /// Gets the message string for this exception.
        /// </summary>
        public override string Message
        {
            get
            {
                return String.Format("The message with ID '{0}' does not hold a valid job descriptor.", MessageId);
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
        }
    }
}
