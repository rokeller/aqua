using System;
using System.Runtime.Serialization;

namespace Aqua
{
    public sealed class UnknownJobException : Exception
    {
        public UnknownJobException(string messageId, string jobName)
        {
            MessageId = messageId;
            JobName = jobName;
        }

        public UnknownJobException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            MessageId = info.GetString("MessageId");
            JobName = info.GetString("JobName");
        }

        public string MessageId { get; private set; }
        public string JobName { get; private set; }

        public override string Message
        {
            get
            {
                return String.Format("The job '{0}' from message with ID '{1}' is unknown.", JobName, MessageId);
            }
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);

            info.AddValue("MessageId", MessageId);
            info.AddValue("JobName", JobName);
        }
    }
}
