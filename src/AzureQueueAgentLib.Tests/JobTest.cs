using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class JobTest
    {
        [Test]
        public void HelloWho()
        {
            HelloWho job = new HelloWho() { Who = "World" };
            Assert.That(job.Execute(), Is.True);
        }

        [Test]
        public void JobBase()
        {
            TestJob job = new TestJob();

            job.OnExecute = () => true;
            Assert.That(job.Execute(), Is.True);

            job.OnExecute = null;
            Assert.That(job.Execute(), Is.False);

            job.OnShouldDeleteJob = (ex) => true;
            Assert.That(job.Execute(), Is.True);
        }

        private sealed class TestJob : JobBase
        {
            public Func<bool> OnExecute;
            public Func<Exception, bool> OnShouldDeleteJob;

            protected override bool ExecuteJob()
            {
                if (null != OnExecute)
                {
                    return OnExecute();
                }

                throw new NotSupportedException();
            }

            protected override bool ShouldDeleteJob(Exception ex)
            {
                if (null != OnShouldDeleteJob)
                {
                    return OnShouldDeleteJob(ex);
                }

                return base.ShouldDeleteJob(ex);
            }
        }
    }
}
