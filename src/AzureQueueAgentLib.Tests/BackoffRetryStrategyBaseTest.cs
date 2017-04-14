using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class BackoffRetryStrategyBaseTest
    {
        [Test]
        public void CtorInputValidation()
        {
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("waitTimes"),
                () => new TestBackoffRetryStrategy(123, null));

            Assert.Throws(Is.TypeOf<ArgumentOutOfRangeException>().And.Property("ParamName").EqualTo("retryCount"),
               () => new TestBackoffRetryStrategy(12, new TimeSpan[0]));
        }

        [Test]
        public void LifeCycle()
        {
            TestBackoffRetryStrategy strategy = new BackoffRetryStrategyBaseTest.TestBackoffRetryStrategy(
                3,
                new TimeSpan[]
                {
                    TimeSpan.FromSeconds(1),
                    TimeSpan.FromSeconds(10),
                    TimeSpan.FromSeconds(100),
                });

            Assert.That(strategy.RetryCount, Is.EqualTo(3));

            Assert.That(strategy.ShouldRetry(0), Is.False);
            Assert.That(strategy.ShouldRetry(1), Is.True);
            Assert.That(strategy.ShouldRetry(2), Is.True);
            Assert.That(strategy.ShouldRetry(3), Is.True);
            Assert.That(strategy.ShouldRetry(4), Is.False);

            Assert.That(strategy.GetWaitTime(1), Is.EqualTo(TimeSpan.FromSeconds(1)));
            Assert.That(strategy.GetWaitTime(2), Is.EqualTo(TimeSpan.FromSeconds(10)));
            Assert.That(strategy.GetWaitTime(3), Is.EqualTo(TimeSpan.FromSeconds(100)));

            Assert.Throws(Is.TypeOf<ArgumentOutOfRangeException>().And.Property("ParamName").EqualTo("attempt"),
                () => strategy.GetWaitTime(0));

            Assert.Throws(Is.TypeOf<ArgumentOutOfRangeException>().And.Property("ParamName").EqualTo("attempt"),
                () => strategy.GetWaitTime(40));
        }

        private sealed class TestBackoffRetryStrategy : BackoffRetryStrategyBase
        {
            public TestBackoffRetryStrategy(int retryCount, TimeSpan[] waitTimes) : base(retryCount, waitTimes)
            {
            }
        }
    }
}
