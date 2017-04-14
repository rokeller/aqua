using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class JobPerfDataTest
    {
        [Test]
        public void CtorInputValidation()
        {
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("jobName"),
                () => new JobPerfData(null));

            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("jobName"),
              () => new JobPerfData(""));

            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("jobName"),
              () => new JobPerfData("     "));
        }

        [Test]
        public void LifeCycle()
        {
            JobPerfData data = new JobPerfData("UnitTest");

            Assert.That(data.JobName, Is.EqualTo("UnitTest"));

            Assert.That(data.SuccessCount, Is.EqualTo(0));
            Assert.That(data.FailureCount, Is.EqualTo(0));
            Assert.That(data.SuccessDuration, Is.EqualTo(0));
            Assert.That(data.FailureDuration, Is.EqualTo(0));
            Assert.That(data.AverageSuccessDuration, Is.EqualTo(0));
            Assert.That(data.AverageFailureDuration, Is.EqualTo(0));
            Assert.That(data.SuccessRate, Is.EqualTo(0));

            data.UpdateSuccess(123);

            Assert.That(data.SuccessCount, Is.EqualTo(1));
            Assert.That(data.FailureCount, Is.EqualTo(0));
            Assert.That(data.SuccessDuration, Is.EqualTo(123));
            Assert.That(data.FailureDuration, Is.EqualTo(0));
            Assert.That(data.AverageSuccessDuration, Is.EqualTo(123));
            Assert.That(data.AverageFailureDuration, Is.EqualTo(0));
            Assert.That(data.SuccessRate, Is.EqualTo(1));

            data.UpdateSuccess(987);

            Assert.That(data.SuccessCount, Is.EqualTo(2));
            Assert.That(data.FailureCount, Is.EqualTo(0));
            Assert.That(data.SuccessDuration, Is.EqualTo(1110));
            Assert.That(data.FailureDuration, Is.EqualTo(0));
            Assert.That(data.AverageSuccessDuration, Is.EqualTo(555));
            Assert.That(data.AverageFailureDuration, Is.EqualTo(0));
            Assert.That(data.SuccessRate, Is.EqualTo(1));

            data.UpdateFailure(12);

            Assert.That(data.SuccessCount, Is.EqualTo(2));
            Assert.That(data.FailureCount, Is.EqualTo(1));
            Assert.That(data.SuccessDuration, Is.EqualTo(1110));
            Assert.That(data.FailureDuration, Is.EqualTo(12));
            Assert.That(data.AverageSuccessDuration, Is.EqualTo(555));
            Assert.That(data.AverageFailureDuration, Is.EqualTo(12));
            Assert.That(data.SuccessRate, Is.EqualTo(2f / 3));

            data.UpdateFailure(13);

            Assert.That(data.SuccessCount, Is.EqualTo(2));
            Assert.That(data.FailureCount, Is.EqualTo(2));
            Assert.That(data.SuccessDuration, Is.EqualTo(1110));
            Assert.That(data.FailureDuration, Is.EqualTo(25));
            Assert.That(data.AverageSuccessDuration, Is.EqualTo(555));
            Assert.That(data.AverageFailureDuration, Is.EqualTo(12.5f));
            Assert.That(data.SuccessRate, Is.EqualTo(1f / 2));
        }
    }
}
