using System;

namespace Aqua.Tests
{
    internal sealed class MockJob : IJob
    {
        public static Func<MockJob, bool> Callback;

        public Guid Id { get; set; }

        public int Int32 { get; set; }

        public bool Execute()
        {
            if (null != Callback)
            {
                return Callback(this);
            }

            return true;
        }
    }
}
