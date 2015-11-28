using System;
using System.Runtime.Serialization;

namespace Aqua
{
    public sealed class MessageFormatException : Exception
    {
        public MessageFormatException(string messageId)
        {
            MessageId = messageId;
        }

        public MessageFormatException(string messageId, Exception innerException)
            : base(null, innerException)
        {
            MessageId = messageId;
        }

        public string MessageId { get; private set; }

        public MessageFormatException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public override string Message
        {
            get
            {
                return String.Format("The message with ID '{0}' does not hold a valid job description.", MessageId);
            }
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            info.AddValue("MessageId", MessageId);
        }
    }
}
