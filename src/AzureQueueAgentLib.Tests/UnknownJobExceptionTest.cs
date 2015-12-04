using Newtonsoft.Json;
using NUnit.Framework;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class UnknownJobExceptionTest
    {
        [Test]
        public void Ctors()
        {
            UnknownJobException ex = new UnknownJobException("UnknownJobExceptionTest-Ctors", "UnknownJob");
            Assert.That("UnknownJobExceptionTest-Ctors", Is.EqualTo(ex.MessageId));
            Assert.That("The job 'UnknownJob' from message with ID 'UnknownJobExceptionTest-Ctors' is unknown.",
                Is.EqualTo(ex.Message));
        }

        [Test]
        public void SerializationDeserialization()
        {
            UnknownJobException ex = new UnknownJobException("UnknownJobExceptionTest-SerializationDeserialization", "UnknownJob");
            string serialized = JsonConvert.SerializeObject(ex);

            UnknownJobException ex2 = JsonConvert.DeserializeObject<UnknownJobException>(serialized);
            Assert.That(ex2.MessageId, Is.EqualTo("UnknownJobExceptionTest-SerializationDeserialization"));
            Assert.That(ex2.JobName, Is.EqualTo("UnknownJob"));
        }
    }
}
