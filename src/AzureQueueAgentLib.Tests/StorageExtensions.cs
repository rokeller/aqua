using Microsoft.WindowsAzure.Storage.Queue;

namespace Aqua.Tests
{
    internal static class StorageExtensions
    {
        public static bool Exists(this CloudQueue queue)
        {
            return queue.ExistsAsync().GetAwaiter().GetResult();
        }

        public static bool CreateIfNotExists(this CloudQueue queue)
        {
            return queue.CreateIfNotExistsAsync().GetAwaiter().GetResult();
        }

        public static bool DeleteIfExists(this CloudQueue queue)
        {
            return queue.DeleteIfExistsAsync().GetAwaiter().GetResult();
        }

        public static void AddMessage(this CloudQueue queue, CloudQueueMessage message)
        {
            queue.AddMessageAsync(message).GetAwaiter().GetResult();
        }

        public static CloudQueueMessage GetMessage(this CloudQueue queue)
        {
            return queue.GetMessageAsync().GetAwaiter().GetResult();
        }

        public static CloudQueueMessage PeekMessage(this CloudQueue queue)
        {
            return queue.PeekMessageAsync().GetAwaiter().GetResult();
        }

        public static void Clear(this CloudQueue queue)
        {
            queue.ClearAsync().GetAwaiter().GetResult();
        }

        public static void DeleteMessage(this CloudQueue queue, CloudQueueMessage message)
        {
            queue.DeleteMessageAsync(message).GetAwaiter().GetResult();
        }
    }
}