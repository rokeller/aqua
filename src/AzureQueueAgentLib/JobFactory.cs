using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;

namespace Aqua
{
    /// <summary>
    /// A factory to create instance of IJob from JobDescriptor objects.
    /// </summary>
    /// <remarks>
    /// This class is not thread-safe.
    /// </remarks>
    public class JobFactory
    {
        #region Fields

        /// <summary>
        /// The Dictionary which tracks the registered job types by their names.
        /// </summary>
        private readonly Dictionary<string, JobSpec> specMap = new Dictionary<string, JobSpec>();

        #endregion

        #region Public Methods

        /// <summary>
        /// Registers the given jobType.
        /// </summary>
        /// <param name="jobType">
        /// The Type of to class to register which implements IJob.
        /// </param>
        public void RegisterJobType(Type jobType)
        {
            if (null == jobType)
            {
                throw new ArgumentNullException("jobType");
            }
            else if (jobType.GetInterface("IJob") != typeof(IJob))
            {
                throw new ArgumentException("The jobType must implement the IJob interface.");
            }

            specMap.Add(jobType.Name, new JobSpec(jobType));
        }

        /// <summary>
        /// Tries to create a new instance of IJob for the given JobDescriptor.
        /// </summary>
        /// <param name="descriptor">
        /// The JobDescriptor to create an instance of IJob for.
        /// </param>
        /// <returns>
        /// An instance of IJob that was created for the given JobDescriptor or null, if no matching job was found.
        /// </returns>
        public IJob CreateJob(JobDescriptor descriptor)
        {
            if (null == descriptor)
            {
                throw new ArgumentNullException("descriptor");
            }

            JobSpec spec;

            if (!specMap.TryGetValue(descriptor.Job, out spec))
            {
                throw new UnknownJobException(descriptor.QueueMessageId, descriptor.Job);
            }

            IJob job = spec.CreateAndBind(descriptor.Properties);

            return job;
        }

        #endregion

        /// <summary>
        /// Tracks the logic to create an instance of an IJob implementation and bind it to the properties of a
        /// JobDescriptor.
        /// </summary>
        private sealed class JobSpec
        {
            /// <summary>
            /// Initializes a new instance of JobSpec for the given jobType.
            /// </summary>
            /// <param name="jobType">
            /// The Type of the job this instance is tracking.
            /// </param>
            public JobSpec(Type jobType)
            {
                Debug.Assert(null != jobType, "The job type must not be null.");
                Debug.Assert(jobType.GetInterface("IJob") == typeof(IJob), "The job type must implement IJob.");

                Type = jobType;
                Properties = new Dictionary<string, PropertyInfo>(StringComparer.CurrentCulture);

                InitProperties();
            }

            /// <summary>
            /// Creates a new instance of the tracked job and binds it to the given properties.
            /// </summary>
            /// <param name="properties">
            /// An IDictionary that maps strings (property names) to JToken objects (the property values) to bind.
            /// </param>
            /// <returns>
            /// An instance of IJob that defines the newly created and bound job.
            /// </returns>
            public IJob CreateAndBind(IDictionary<string, JToken> properties)
            {
                IJob job = (IJob)Activator.CreateInstance(Type);

                if (null != properties)
                {
                    foreach (KeyValuePair<string, JToken> prop in properties)
                    {
                        PropertyInfo propInfo;

                        if (Properties.TryGetValue(prop.Key, out propInfo))
                        {
                            propInfo.SetValue(job, prop.Value.ToObject(propInfo.PropertyType));
                        }
                    }
                }

                return job;
            }

            /// <summary>
            /// Gets or sets the Type of the job this instance can create and bind.
            /// </summary>
            public Type Type { get; private set; }

            /// <summary>
            /// Gets or sets a Dictionary which maps property names to the corresponding PropertyInfo objects.
            /// </summary>
            public Dictionary<string, PropertyInfo> Properties { get; private set; }

            /// <summary>
            /// Initializes the properties that can be bound to in the job type this instance is tracking.
            /// </summary>
            private void InitProperties()
            {
                PropertyInfo[] props = Type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy);

                foreach (PropertyInfo prop in props)
                {
                    if (prop.CanWrite)
                    {
                        Properties.Add(prop.Name, prop);
                    }
                }
            }
        }
    }
}
