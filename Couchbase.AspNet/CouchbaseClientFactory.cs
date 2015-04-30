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
        private bool initialized = false;
        public IBucket Create(string name, NameValueCollection config, out bool disposeClient)
        {
            // This client should be disposed of as it is not shared
            disposeClient = false;

            // Get the section name from the configuration file. If not found, create a default Couchbase client which 
            // will get the configuration information from the default Couchbase client section in the Web.config file
            var sectionName = ProviderHelper.GetAndRemove(config, "section", false);
	        if (String.IsNullOrEmpty(sectionName))
				return this.CreateClient(null);

            // If a custom section name is passed in, get the section information and use it to construct the Couchbase client
            var section = ConfigurationManager.GetSection(sectionName);
            if (!initialized)
            {
                ClusterHelper.Initialize(sectionName);
                initialized = true;
            }
            if (section == null)
				throw new InvalidOperationException("Invalid config section: " + sectionName);
			return CreateClient(section);
        }

		private IBucket CreateClient(object config)
		{
		    IBucket bucket;
		    var section = config as CouchbaseClientSection;

		    BucketElement bucketElement = null;
		    foreach (var item in section.Buckets)
		    {
		        bucketElement = (BucketElement) item;
                break;
		    }
		    if (section != null)
		    {
		        bucket = ClusterHelper.GetBucket(bucketElement.Name);
		    }
		    else
		    {
		        const string message = "CreateClient requires a CouchbaseConfigSection. Found a {0} instead.";
		        throw new ConfigurationErrorsException(string.Format(message, config.GetType().Name));
		    }
		    return bucket;
		}
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    @author Couchbase <info@couchbase.com>
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
