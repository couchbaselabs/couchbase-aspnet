using System;
using System.Collections.Specialized;
using System.Configuration;
using Couchbase.Configuration.Client;
using Couchbase.Configuration.Client.Providers;
using Couchbase.Core;

namespace Couchbase.AspNet
{
    internal static class ProviderHelper
    {
        /// <summary>
        /// Gets and removes an value from the configuration section
        /// </summary>
        /// <param name="config">Name value collection to examine</param>
        /// <param name="name">Name of the value to retrieve</param>
        /// <param name="required">True if the value is required, false if optional</param>
        /// <returns>Value returned from the configuration section, null if not found</returns>
        public static string GetAndRemove(
            NameValueCollection config,
            string name,
            bool required)
        {
            var value = config[name];
            if (value == null) {
                if (required)
                    throw new System.Configuration.ConfigurationErrorsException("Missing parameter: " + name);
            } else {
                config.Remove(name);
            }

            return value;
        }

        /// <summary>
        /// Helper to make sure there are no unknown attributes left over in the config section
        /// </summary>
        /// <param name="config">Name value collection to examine</param>
        public static void CheckForUnknownAttributes(
            NameValueCollection config)
        {
            if (config.Count > 0)
                throw new System.Configuration.ConfigurationErrorsException("Unknown parameter: " + config.Keys[0]);
        }

        /// <summary>
        /// Gets a Couchbase bucket using the couchbase factory configured in the Web.config file. If the factory is
        /// not defined in the Web.config file section, then the default factory is used.
        /// </summary>
        /// <param name="name">Name of the bucket WHICH EVIDENTLY IS UNUSED</param>
        /// <param name="config">Name value collection from config file</param>
        /// <param name="cluster">The <see cref="ICluster"/> instance.</param>
        /// <returns>Instance of the couchbase bucket to use</returns>
        public static IBucket GetBucket(
            string name,
            NameValueCollection config, ICluster cluster)
        {
            var bucketName = GetAndRemove(config, "bucket", false);
            if (!string.IsNullOrEmpty(bucketName))
            {
                return cluster.OpenBucket(bucketName);
            }

            //if no bucket is provide use the default bucket
            return cluster.OpenBucket();
        }

        public static ICluster GetCluster(
            string name,
            NameValueCollection config)
        {
            var section = (CouchbaseClientSection)ConfigurationManager.GetSection(name);
            var clientConfig = new ClientConfiguration(section);
            return new Cluster(clientConfig);
        }

        /// <summary>
        /// Internal function to get the couchbase factory 
        /// </summary>
        /// <param name="typeName">Name of the factory type extracted from the config file</param>
        /// <returns>Instance of the couchbase factory to use</returns>
        private static ICouchbaseBucketFactory GetFactoryInstance(
            string typeName)
        {
            // If the factory type is not provided, use the default one
            if (string.IsNullOrEmpty(typeName))
                return new CouchbaseBucketFactory();

            // Attempt to create the custom factory instance
            var type = Type.GetType(typeName, false);
            if (type == null)
                throw new System.Configuration.ConfigurationErrorsException("Could not load type: " + typeName);
            if (!typeof(ICouchbaseBucketFactory).IsAssignableFrom(type))
                throw new System.Configuration.ConfigurationErrorsException("Type '" + typeName + "' must implement ICouchbaseBucketFactory");
            return (ICouchbaseBucketFactory)Activator.CreateInstance(type);
        }
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2012 Couchbase, Inc.
 *    @copyright 2012 Attila Kiskó, enyim.com
 *    @copyright 2015 AMain.com, Inc.
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion