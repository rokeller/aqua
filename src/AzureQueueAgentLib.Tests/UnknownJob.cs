using System;

namespace Aqua.Tests
{
    public sealed class UnknownJob : IJob
    {
        public bool Execute()
        {
            throw new NotSupportedException();
        }
    }
}
