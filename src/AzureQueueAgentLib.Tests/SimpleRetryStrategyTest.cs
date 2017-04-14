using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class SimpleRetryStrategyTest
    {
        [Test]
        public void CtorInputValidation()
        {
            Assert.Throws(Is.InstanceOf<ArgumentOutOfRangeException>().And.Property("ParamName").EqualTo("retryCount"),
                () => new SimpleRetryStrategy(-1, TimeSpan.FromSeconds(-1)));

            Assert.Throws(Is.InstanceOf<ArgumentOutOfRangeException>().And.Property("ParamName").EqualTo("waitTime"),
             () => new SimpleRetryStrategy(0, TimeSpan.FromSeconds(-1)));
        }

        [Test]
        public void LifeCycle()
        {
            TimeSpan waitTime = TimeSpan.FromSeconds(123);
            SimpleRetryStrategy strategy = new SimpleRetryStrategy(10, waitTime);

            for (int i = -100; i < 100; i++)
            {
                if (i > 0 && i <= 10)
                {
                    Assert.That(strategy.ShouldRetry(i), Is.True);
                }
                else
                {
                    Assert.That(strategy.ShouldRetry(i), Is.False);
                }

                Assert.That(strategy.GetWaitTime(i), Is.EqualTo(waitTime)); 
            }
        }
    }
}
