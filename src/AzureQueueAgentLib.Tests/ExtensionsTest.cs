using NUnit.Framework;
using System;

namespace Aqua.Tests
{
    [TestFixture]
    public sealed class ExtensionsTest
    {
        [Test]
        public void GetService_InputValidation()
        {
            TestServiceProvider serviceProvider = null;

            Assert.Throws(Is.InstanceOf<ArgumentNullException>().And.Property("ParamName").EqualTo("serviceProvider"),
                () => Extensions.GetService<string>(serviceProvider));

            serviceProvider = new ExtensionsTest.TestServiceProvider();

            Assert.That(serviceProvider.GetService<string>(), Is.EqualTo("TestServiceProvider"));
        }

        private sealed class TestServiceProvider : IServiceProvider
        {
            public object GetService(Type serviceType)
            {
                if (serviceType == typeof(string))
                {
                    return "TestServiceProvider";
                }

                return null;
            }
        }
    }
}
