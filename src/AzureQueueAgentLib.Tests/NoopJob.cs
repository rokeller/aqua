using System;

namespace Aqua.Tests
{
    internal sealed class NoopJob : IJob
    {
        private readonly Guid id = Guid.NewGuid();

        public Guid Id { get { return id; } }

        public object Null
        {
            set
            {
                Console.WriteLine("Set Null to {0}", value);
            }
        }

        public bool Execute()
        {
            return true;
        }
    }
}
