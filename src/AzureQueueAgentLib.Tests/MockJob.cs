using System;

namespace Aqua.Tests
{
    internal sealed class MockJob : IJob
    {
        public static Func<Guid, bool> Callback;

        public Guid Id { get; set; }

        public bool Execute()
        {
            if (null != Callback)
            {
                return Callback(Id);
            }

            return true;
        }
    }
}
