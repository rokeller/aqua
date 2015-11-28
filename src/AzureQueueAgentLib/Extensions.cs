using System;

namespace Aqua
{
    /// <summary>
    /// Defines useful extension methods.
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// Gets the service of a specific type from the given IServiceProvider.
        /// </summary>
        /// <typeparam name="T">
        /// The type of the service to get.
        /// </typeparam>
        /// <param name="serviceProvider">
        /// The IServiceProvider to get the service from.
        /// </param>
        /// <returns>
        /// An instance of T if the service was found or null otherwise.
        /// </returns>
        public static T GetService<T>(this IServiceProvider serviceProvider)
        {
            if (null == serviceProvider)
            {
                throw new ArgumentNullException("serviceProvider");
            }

            return (T)serviceProvider.GetService(typeof(T));
        }
    }
}
