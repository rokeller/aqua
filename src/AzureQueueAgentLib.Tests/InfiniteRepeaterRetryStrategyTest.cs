using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class InfiniteRepeaterRetryStrategyTest
    {
        [Test]
        public void CtorInputValidation()
        {
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("inner"),
                () => new InfiniteRepeaterRetryStrategy(null));
        }

        [Test]
        public void ShouldAttempt()
        {
            LinearBackoffRetryStrategy baseStrategy = new LinearBackoffRetryStrategy(3, TimeSpan.FromSeconds(1));
            InfiniteRepeaterRetryStrategy strategy = new InfiniteRepeaterRetryStrategy(baseStrategy);

            Assert.That(strategy.ShouldRetry(-1), Is.False, "Attempt -1");
            Assert.That(strategy.ShouldRetry(0), Is.False, "Attempt 0");
            Assert.That(strategy.ShouldRetry(1), Is.True, "Attempt 1");
            Assert.That(strategy.ShouldRetry(2), Is.True, "Attempt 2");
            Assert.That(strategy.ShouldRetry(3), Is.True, "Attempt 3");
            Assert.That(strategy.ShouldRetry(4), Is.True, "Attempt 4");
            Assert.That(strategy.ShouldRetry(50), Is.True, "Attempt 50");
            Assert.That(strategy.ShouldRetry(600), Is.True, "Attempt 600");
            Assert.That(strategy.ShouldRetry(7000), Is.True, "Attempt 7000");
            Assert.That(strategy.ShouldRetry(9999999), Is.True, "Attempt 9999999");
            Assert.That(strategy.ShouldRetry(int.MaxValue), Is.True, "Attempt MaxValue");
        }

        [Test]
        public void WaitTimes()
        {
            TimeSpan inifiteWaitTime = TimeSpan.FromSeconds(3);
            TimeSpan[] waitTimes = new TimeSpan[] {
                TimeSpan.FromSeconds(1),
                TimeSpan.FromSeconds(2),
                TimeSpan.FromSeconds(3),
            };
            LinearBackoffRetryStrategy baseStrategy = new LinearBackoffRetryStrategy(3, TimeSpan.FromSeconds(1));
            InfiniteRepeaterRetryStrategy strategy = new InfiniteRepeaterRetryStrategy(baseStrategy);

            for (int i = 0; i < waitTimes.Length; i++)
            {
                Assert.That(strategy.ShouldRetry(i + 1), Is.True, "Attempt " + (i + 1));
                Assert.That(strategy.GetWaitTime(i + 1), Is.EqualTo(waitTimes[i]), "Attempt " + (i + 1));
            }

            Assert.That(strategy.GetWaitTime(4), Is.EqualTo(inifiteWaitTime), "Attempt 4");
            Assert.That(strategy.GetWaitTime(50), Is.EqualTo(inifiteWaitTime), "Attempt 50");
            Assert.That(strategy.GetWaitTime(600), Is.EqualTo(inifiteWaitTime), "Attempt 600");
            Assert.That(strategy.GetWaitTime(7000), Is.EqualTo(inifiteWaitTime), "Attempt 7000");
            Assert.That(strategy.GetWaitTime(9999999), Is.EqualTo(inifiteWaitTime), "Attempt 9999999");
            Assert.That(strategy.GetWaitTime(int.MaxValue), Is.EqualTo(inifiteWaitTime), "Attempt MaxValue");
        }

        [Test]
        public void WaitTimes_SkippingFirstFew()
        {
            WaitTimes_SkippingFirstFew(5);
            WaitTimes_SkippingFirstFew(60);
            WaitTimes_SkippingFirstFew(700);
            WaitTimes_SkippingFirstFew(8000);
            WaitTimes_SkippingFirstFew(90000);
        }

        private void WaitTimes_SkippingFirstFew(int skip)
        {
            TimeSpan inifiteWaitTime = TimeSpan.FromSeconds(3);
            LinearBackoffRetryStrategy baseStrategy = new LinearBackoffRetryStrategy(3, TimeSpan.FromSeconds(1));
            InfiniteRepeaterRetryStrategy strategy = new InfiniteRepeaterRetryStrategy(baseStrategy);

            Assert.That(strategy.GetWaitTime(skip), Is.EqualTo(inifiteWaitTime), "Attempt " + skip);
        }
    }
}
