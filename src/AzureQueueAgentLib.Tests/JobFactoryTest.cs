using Newtonsoft.Json.Linq;
using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class JobFactoryTest
    {
        private JobFactory factory;

        [Test]
        public void RegisterJobTypeInputValidation()
        {
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("jobType"),
                () => factory.RegisterJobType(null));
            Assert.Throws(Is.TypeOf<ArgumentException>().And.Message.EqualTo("The jobType must implement the IJob interface."),
                () => factory.RegisterJobType(typeof(JobFactoryTest)));

            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("jobName"),
                () => factory.RegisterJobType(null, null));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("jobName"),
                () => factory.RegisterJobType(String.Empty, null));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("jobName"),
                () => factory.RegisterJobType("     ", null));
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("jobType"),
                () => factory.RegisterJobType("Blah", null));
            Assert.Throws(Is.TypeOf<ArgumentException>().And.Message.EqualTo("The jobType must implement the IJob interface."),
                () => factory.RegisterJobType("Blah", typeof(JobFactoryTest)));
        }

        [Test]
        public void RegisterJobWithDifferentName()
        {
            factory.RegisterJobType("ThisIsAlsoHelloWorld", typeof(HelloWho));

            JobDescriptor desc;
            JToken token;
            IJob job;
            HelloWho helloJob;

            // Create a descriptor and test it.
            desc = factory.CreateDescriptor(new HelloWho() { Who = "RegisterJobWithDifferentName" });
            Assert.That(desc.Job, Is.EqualTo("ThisIsAlsoHelloWorld"));
            Assert.That(desc.Properties, Is.Not.Null.And.Count.EqualTo(1));
            Assert.That(desc.Properties.TryGetValue("Who", out token), Is.True);
            Assert.That(token, Is.TypeOf<JValue>().And.Property("Type").EqualTo(JTokenType.String));
            Assert.That(token.ToObject<string>(), Is.EqualTo("RegisterJobWithDifferentName"));

            // Use the descriptor to create a job, and test it.
            job = factory.CreateJob(desc);
            Assert.That(job, Is.Not.Null);
            Assert.That(job, Is.InstanceOf<HelloWho>());
            helloJob = (HelloWho)job;
            Assert.That(helloJob.Who, Is.EqualTo("RegisterJobWithDifferentName"));

            // Now use the descriptor with the original name to create a job, and test it.
            desc.Job = "HelloWho";
            job = factory.CreateJob(desc);
            Assert.That(job, Is.Not.Null);
            Assert.That(job, Is.InstanceOf<HelloWho>());
            helloJob = (HelloWho)job;
            Assert.That(helloJob.Who, Is.EqualTo("RegisterJobWithDifferentName"));
        }

        [Test]
        public void CreateJobInputValidation()
        {
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("descriptor"),
                () => factory.CreateJob(null));
        }

        [Test]
        public void CreateDescriptorInputValidation()
        {
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("job"),
                () => factory.CreateDescriptor(null));

            Assert.Throws(Is.TypeOf<UnknownJobException>().And.Property("MessageId").Null
                            .And.Property("JobName").EqualTo("UnknownJob"),
                () => factory.CreateDescriptor(new UnknownJob()));
        }

        [Test]
        public void CreateDescriptor()
        {
            JobDescriptor desc;
            JToken token;

            // Property with value.
            desc = factory.CreateDescriptor(new HelloWho() { Who = "CreateDescriptor" });
            Assert.That(desc, Is.Not.Null);
            Assert.That(desc.Job, Is.EqualTo("HelloWho"));
            Assert.That(desc.Properties, Is.Not.Null.And.Count.EqualTo(1));
            Assert.That(desc.Properties.TryGetValue("Who", out token), Is.True);
            Assert.That(token, Is.TypeOf<JValue>().And.Property("Type").EqualTo(JTokenType.String));
            Assert.That(token.ToObject<string>(), Is.EqualTo("CreateDescriptor"));
            Assert.That(desc.QueueMessageId, Is.Null);

            // Property with null value.
            desc = factory.CreateDescriptor(new HelloWho() { Who = null });
            Assert.That(desc, Is.Not.Null);
            Assert.That(desc.Job, Is.EqualTo("HelloWho"));
            Assert.That(desc.Properties, Is.Not.Null.And.Count.EqualTo(1));
            Assert.That(desc.Properties.TryGetValue("Who", out token), Is.True);
            Assert.That(token, Is.TypeOf<JValue>().And.Property("Type").EqualTo(JTokenType.Null));
            Assert.That(token.ToObject<string>(), Is.Null);
            Assert.That(desc.QueueMessageId, Is.Null);

            // Job with no properties.
            desc = factory.CreateDescriptor(new NoopJob());
            Assert.That(desc, Is.Not.Null);
            Assert.That(desc.Job, Is.EqualTo("NoopJob"));
            Assert.That(desc.Properties, Is.Null);
            Assert.That(desc.QueueMessageId, Is.Null);

            // Job with value-type properties.
            Guid guid = Guid.NewGuid();
            desc = factory.CreateDescriptor(new MockJob() { Id = guid });
            Assert.That(desc, Is.Not.Null);
            Assert.That(desc.Job, Is.EqualTo("MockJob"));
            Assert.That(desc.Properties, Is.Not.Null.And.Count.EqualTo(1));
            Assert.That(desc.Properties.TryGetValue("Id", out token), Is.True);
            Assert.That(token, Is.TypeOf<JValue>().And.Property("Type").EqualTo(JTokenType.Guid));
            Assert.That(token.ToObject<Guid>(), Is.EqualTo(guid));
            Assert.That(desc.QueueMessageId, Is.Null);
        }

        [Test]
        public void Roundtrip()
        {
            MockJob job = new MockJob() { Id = Guid.NewGuid() };
            JobDescriptor desc = factory.CreateDescriptor(job);
            JToken token;

            Assert.That(desc, Is.Not.Null);
            Assert.That(desc.Job, Is.EqualTo("MockJob"));
            Assert.That(desc.Properties, Is.Not.Null.And.Count.EqualTo(1));
            Assert.That(desc.Properties.TryGetValue("Id", out token), Is.True);
            Assert.That(token, Is.TypeOf<JValue>().And.Property("Type").EqualTo(JTokenType.Guid));
            Assert.That(token.ToObject<Guid>(), Is.EqualTo(job.Id));
            Assert.That(desc.QueueMessageId, Is.Null);

            IJob job2 = factory.CreateJob(desc);
            Assert.That(job2, Is.Not.Null.And.TypeOf<MockJob>());
            MockJob mockJob2 = (MockJob)job2;
            Assert.That(mockJob2.Id, Is.EqualTo(job.Id));
        }

        [SetUp]
        public void Setup()
        {
            factory = new JobFactory();

            factory.RegisterJobType(typeof(HelloWho));
            factory.RegisterJobType(typeof(MockJob));
            factory.RegisterJobType(typeof(NoopJob));
        }
    }
}
