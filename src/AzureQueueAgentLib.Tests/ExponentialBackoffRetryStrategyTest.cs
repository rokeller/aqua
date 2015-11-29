using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class ExponentialBackoffRetryStrategyTest
    {
        [Test]
        public void CtorInputValidation()
        {
            Assert.Throws(Is.TypeOf<ArgumentOutOfRangeException>().And.Property("ParamName").EqualTo("retryCount"),
                () => new ExponentialBackoffRetryStrategy(-1, TimeSpan.FromSeconds(1)));

            Assert.Throws(Is.TypeOf<ArgumentOutOfRangeException>().And.Property("ParamName").EqualTo("baseWaitTime"),
              () => new ExponentialBackoffRetryStrategy(0, TimeSpan.FromSeconds(-1)));
        }

        [Test]
        public void ShouldAttempt()
        {
            ExponentialBackoffRetryStrategy strategy = new ExponentialBackoffRetryStrategy(4, TimeSpan.FromSeconds(1));

            Assert.That(strategy.RetryCount, Is.EqualTo(4));

            Assert.That(strategy.ShouldRetry(-1), Is.False, "Attempt -1");
            Assert.That(strategy.ShouldRetry(0), Is.False, "Attempt 0");
            Assert.That(strategy.ShouldRetry(1), Is.True, "Attempt 1");
            Assert.That(strategy.ShouldRetry(2), Is.True, "Attempt 2");
            Assert.That(strategy.ShouldRetry(3), Is.True, "Attempt 3");
            Assert.That(strategy.ShouldRetry(4), Is.True, "Attempt 4");
            Assert.That(strategy.ShouldRetry(5), Is.False, "Attempt 5");
            Assert.That(strategy.ShouldRetry(6), Is.False, "Attempt 6");
        }

        [Test]
        public void WaitTimes()
        {
            TimeSpan[] waitTimes = new TimeSpan[] {
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(4),
                TimeSpan.FromSeconds(8),
                TimeSpan.FromSeconds(16),
                TimeSpan.FromSeconds(32),
                TimeSpan.FromSeconds(64),
                TimeSpan.FromSeconds(128),
                TimeSpan.FromSeconds(256),
            };
            ExponentialBackoffRetryStrategy strategy = new ExponentialBackoffRetryStrategy(9, TimeSpan.FromSeconds(1));

            Assert.That(strategy.RetryCount, Is.EqualTo(9));

            for (int i = 0; i < strategy.RetryCount; i++)
            {
                Assert.That(strategy.ShouldRetry(i + 1), Is.True, "Attempt " + (i + 1));
                Assert.That(strategy.GetWaitTime(i + 1), Is.EqualTo(waitTimes[i]), "Attempt " + (i + 1));
            }

            Assert.That(strategy.ShouldRetry(11), Is.False, "Attempt 11");
            Assert.Throws(Is.TypeOf<ArgumentOutOfRangeException>().And.Property("ParamName").EqualTo("attempt"),
              () => strategy.GetWaitTime(11));

            Assert.That(strategy.ShouldRetry(0), Is.False, "Attempt 0");
            Assert.Throws(Is.TypeOf<ArgumentOutOfRangeException>().And.Property("ParamName").EqualTo("attempt"),
              () => strategy.GetWaitTime(0));

            Assert.That(strategy.ShouldRetry(-1), Is.False, "Attempt -2");
            Assert.Throws(Is.TypeOf<ArgumentOutOfRangeException>().And.Property("ParamName").EqualTo("attempt"),
              () => strategy.GetWaitTime(-1));
        }
    }
}
