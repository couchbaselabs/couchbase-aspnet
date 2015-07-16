using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Linq;
using Couchbase.Configuration.Client;
using Couchbase.Configuration.Client.Providers;
using Couchbase.Core;

namespace Couchbase.AspNet
{
	public sealed class CouchbaseClientFactory : ICouchbaseClientFactory
    {
        public static Cluster cluster = null;
        public static Dictionary<string, IBucket> buckets = new Dictionary<string,IBucket>();
        
        public IBucket Create(string name, NameValueCollection config, out bool disposeClient)
        {
            // This client should be disposed of as it is not shared
            disposeClient = false;

            
            // Get the section name from the configuration file. If not found, create a default Couchbase client.
            var sectionName = ProviderHelper.GetAndRemove(config, "section", false);
            if (String.IsNullOrEmpty(sectionName))
            {
                return getDefaultClient();
            }

            // If a custom section name is passed in, get the section information and use it to construct the Couchbase client
            var sectionObj = ConfigurationManager.GetSection(sectionName);
            if (sectionObj == null)
                throw new InvalidOperationException("Invalid config section: " + sectionName);
            var section = sectionObj as CouchbaseClientSection;
            if(section == null)
            {
                throw new ConfigurationErrorsException(string.Format("CreateClient requires a CouchbaseConfigSection.  Found a {0} instead.", sectionObj.GetType().Name));
            }
            if (cluster == null)
            {
                cluster = new Cluster(new ClientConfiguration(section));
            }
            BucketElement bucketElement = null;
            // Strange way to get the first element but, there you go
            foreach (var item in section.Buckets)
            {
                bucketElement = (BucketElement)item;
                break;
            }
            if (!buckets.ContainsKey(bucketElement.Name) || !cluster.IsOpen(bucketElement.Name) )
            {
                if (buckets.ContainsKey(bucketElement.Name))
                    buckets[bucketElement.Name].Dispose();
                buckets[bucketElement.Name] = cluster.OpenBucket(bucketElement.Name, bucketElement.Password);
            }
            return buckets[bucketElement.Name];
        }

        private IBucket getDefaultClient()
        {
            IBucket bucket = null;
            ClusterHelper.Initialize();
            return ClusterHelper.GetBucket("default");
        }
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    @author Couchbase <info@couchbase.com>
 *    @copyright 2015 NetVoyage Corporation
 *    @copyright 2012 Couchbase, Inc.
 *    @copyright 2012 Attila Kiskó, enyim.com
 *    @copyright 2012 Good Time Hobbies, Inc.
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
