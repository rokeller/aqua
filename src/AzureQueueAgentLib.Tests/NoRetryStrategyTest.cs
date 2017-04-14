using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class NoRetryStrategyTest
    {
        [Test]
        public void LifeCycle()
        {
            for (int i = -100; i < 100; i++)
            {
                Assert.That(NoRetryStrategy.Default.ShouldRetry(i), Is.False);

                Assert.Throws<NotSupportedException>(() => NoRetryStrategy.Default.GetWaitTime(i));
            }
        }
    }
}
