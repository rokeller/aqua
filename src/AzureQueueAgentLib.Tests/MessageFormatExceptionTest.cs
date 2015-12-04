using Newtonsoft.Json;
using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class MessageFormatExceptionTest
    {
        [Test]
        public void Ctors()
        {
            MessageFormatException ex = new MessageFormatException("MessageFormatExceptionTest-Ctors");
            Assert.That("MessageFormatExceptionTest-Ctors", Is.EqualTo(ex.MessageId));
            Assert.That("The message with ID 'MessageFormatExceptionTest-Ctors' does not hold a valid job descriptor.",
                Is.EqualTo(ex.Message));

            ex = new MessageFormatException("MessageFormatExceptionTest-Ctors", new NotSupportedException());
            Assert.That("MessageFormatExceptionTest-Ctors", Is.EqualTo(ex.MessageId));
            Assert.That("The message with ID 'MessageFormatExceptionTest-Ctors' does not hold a valid job descriptor.",
                Is.EqualTo(ex.Message));
            Assert.That(ex.InnerException, Is.TypeOf<NotSupportedException>());
        }

        [Test]
        public void SerializationDeserialization()
        {
            MessageFormatException ex = new MessageFormatException("MessageFormatExceptionTest-SerializationDeserialization",
                new InvalidOperationException("Inner-SerializationDeserialization"));
            string serialized = JsonConvert.SerializeObject(ex);

            MessageFormatException ex2 = JsonConvert.DeserializeObject<MessageFormatException>(serialized);
            Assert.That(ex2.MessageId, Is.EqualTo("MessageFormatExceptionTest-SerializationDeserialization"));
            Assert.That(ex2.InnerException, Is.Not.Null);
            Assert.That(ex2.InnerException.Message, Is.EqualTo("Inner-SerializationDeserialization"));
        }
    }
}
