using System;
using System.Collections.Specialized;
using System.Configuration;
using Enyim.Caching;
using Couchbase.Configuration;

namespace Couchbase.AspNet
{
    public sealed class CouchbaseClientFactory : ICouchbaseClientFactory
    {
        public IMemcachedClient Create(string name, NameValueCollection config, out bool disposeClient)
        {
            // This client should be disposed of as it is not shared
            disposeClient = true;

            // Get the section name from the configuration file. If not found, create a default Couchbase client which 
            // will get the configuration information from the default Couchbase client section in the Web.config file
            var sectionName = ProviderHelper.GetAndRemove(config, "section", false);
            if (String.IsNullOrEmpty(sectionName))
                return new CouchbaseClient();

            // If a custom section name is passed in, get the section information and use it to construct the Couchbase client
            var section = ConfigurationManager.GetSection(sectionName) as ICouchbaseClientConfiguration;
            if (section == null)
                throw new InvalidOperationException("Invalid config section: " + section);
            return new CouchbaseClient(section);
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
