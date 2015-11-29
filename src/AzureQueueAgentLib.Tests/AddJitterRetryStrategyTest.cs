using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class AddJitterRetryStrategyTest
    {
        private static readonly TimeSpan DefaultMax = TimeSpan.FromSeconds(10);

        private AddJitterRetryStrategy strategy;

        #region Input Validation

        [Test]
        public void CtorInputValidation()
        {
            Assert.Throws(Is.TypeOf<ArgumentNullException>().And.Property("ParamName").EqualTo("inner"),
                () => new AddJitterRetryStrategy(null));

            Assert.Throws(Is.TypeOf<ArgumentOutOfRangeException>().And.Property("ParamName").EqualTo("minPercent"),
                () => new AddJitterRetryStrategy(new SimpleRetryStrategy(1, DefaultMax), -1));
            Assert.Throws(Is.TypeOf<ArgumentException>().And.Message.EqualTo("minPercent must be less than maxPercent."),
                () => new AddJitterRetryStrategy(new SimpleRetryStrategy(1, DefaultMax), 1));

            Assert.Throws(Is.TypeOf<ArgumentOutOfRangeException>().And.Property("ParamName").EqualTo("maxPercent"),
                () => new AddJitterRetryStrategy(new SimpleRetryStrategy(1, DefaultMax), 0, 0));
        }

        #endregion

        #region Should Attempt

        [Test]
        public void ShouldAttempt()
        {
            Assert.That(strategy.ShouldRetry(-1), Is.False, "Attempt -1");
            Assert.That(strategy.ShouldRetry(0), Is.False, "Attempt 0");
            Assert.That(strategy.ShouldRetry(1), Is.True, "Attempt 1");
            Assert.That(strategy.ShouldRetry(2), Is.True, "Attempt 2");
            Assert.That(strategy.ShouldRetry(3), Is.False, "Attempt 3");
            Assert.That(strategy.ShouldRetry(4), Is.False, "Attempt 4");
        }

        #endregion

        #region Range Distribution

        [Test]
        public void WaitTimeRangeDefault()
        {
            // We expect wait times between 0 (inclusive) and 10 (exclusive) seconds, with a mean value of about 5 seconds.
            TimeSpan total = TimeSpan.Zero;
            const int Iterations = 10000;

            for (int i = 0; i < Iterations; i++)
            {
                TimeSpan wait = strategy.GetWaitTime(1);
                total += wait;

                Assert.That(wait, Is.GreaterThanOrEqualTo(TimeSpan.Zero));
                Assert.That(wait, Is.LessThan(DefaultMax));
            }

            TimeSpan mean = new TimeSpan(total.Ticks / Iterations);
            TimeSpan diffTo5Sec = TimeSpan.FromSeconds(5) - mean;
            Assert.That(Math.Abs(diffTo5Sec.TotalMilliseconds), Is.LessThanOrEqualTo(100));
        }

        [Test]
        public void WaitTimeRangeCustomMin()
        {
            TimeSpan min = TimeSpan.FromSeconds(5);
            strategy = new AddJitterRetryStrategy(new SimpleRetryStrategy(1, TimeSpan.FromSeconds(10)), 0.5);

            // We expect wait times between 0 (inclusive) and 10 (exclusive) seconds, with a mean value of about 5 seconds.
            TimeSpan total = TimeSpan.Zero;
            const int Iterations = 10000;

            for (int i = 0; i < Iterations; i++)
            {
                TimeSpan wait = strategy.GetWaitTime(1);
                total += wait;

                Assert.That(wait, Is.GreaterThanOrEqualTo(min));
                Assert.That(wait, Is.LessThan(DefaultMax));
            }

            TimeSpan mean = new TimeSpan(total.Ticks / Iterations);
            TimeSpan diffTo10Sec = TimeSpan.FromSeconds(7.5) - mean;
            Assert.That(Math.Abs(diffTo10Sec.TotalMilliseconds), Is.LessThanOrEqualTo(100));
        }

        [Test]
        public void WaitTimeRangeCustomMinMax()
        {
            TimeSpan min = TimeSpan.FromSeconds(7.5);
            TimeSpan max = TimeSpan.FromSeconds(12.5);
            strategy = new AddJitterRetryStrategy(new SimpleRetryStrategy(1, TimeSpan.FromSeconds(10)), 0.75, 1.25);

            // We expect wait times between 0 (inclusive) and 10 (exclusive) seconds, with a mean value of about 5 seconds.
            TimeSpan total = TimeSpan.Zero;
            const int Iterations = 10000;

            for (int i = 0; i < Iterations; i++)
            {
                TimeSpan wait = strategy.GetWaitTime(1);
                total += wait;

                Assert.That(wait, Is.GreaterThanOrEqualTo(min));
                Assert.That(wait, Is.LessThan(max));
            }

            TimeSpan mean = new TimeSpan(total.Ticks / Iterations);
            TimeSpan diffTo10Sec = TimeSpan.FromSeconds(10) - mean;
            Assert.That(Math.Abs(diffTo10Sec.TotalMilliseconds), Is.LessThanOrEqualTo(100));
        }

        #endregion

        [SetUp]
        public void Setup()
        {
            strategy = new AddJitterRetryStrategy(new SimpleRetryStrategy(2, TimeSpan.FromSeconds(10)));
        }
    }
}
