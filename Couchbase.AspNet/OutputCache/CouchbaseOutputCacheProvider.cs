using System;
using System.Text;
using System.Collections.Specialized;
using System.Web.Caching;
using Enyim.Caching;
using Enyim.Caching.Memcached;

namespace Couchbase.AspNet.OutputCache
{
    public class CouchbaseOutputCacheProvider : OutputCacheProvider
    {
        private IMemcachedClient client;
        private bool disposeClient;
        private static readonly string Prefix = (System.Web.Hosting.HostingEnvironment.SiteName ?? String.Empty).Replace(" ", "-") + "+" + System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath + "cache-";

        /// <summary>
        /// Function to initialize the provider
        /// </summary>
        /// <param name="name">Name of the element in the configuration file</param>
        /// <param name="config">Configuration values for the provider from the Web.config file</param>
        public override void Initialize(
            string name,
            NameValueCollection config)
        {
            base.Initialize(name, config);
            client = ProviderHelper.GetClient(name, config, () => (ICouchbaseClientFactory)new CouchbaseClientFactory(), out disposeClient);

            ProviderHelper.CheckForUnknownAttributes(config);
        }

        /// <summary>
        /// Function to sanitize the key for use with Couchbase. We simply convert it to a Base 64 representation so that it will be unique and will allow
        /// encoding of any URL
        /// </summary>
        /// <param name="key">Key to sanitize</param>
        /// <returns>Sanitized key</returns>
        private string SanitizeKey(
            string key)
        {
            return Prefix + Convert.ToBase64String(Encoding.UTF8.GetBytes(key), Base64FormattingOptions.None);
        }

        /// <summary>
        /// Function to add a new item to the output cache. If there is already a value in the cache for the 
        /// specified key, the provider must return that value and must not store the data passed by using the Add method 
        /// parameters. The Add method stores the data if it is not already in the cache and returns the value
        /// read from the cache if it already exists.
        /// </summary>
        /// <param name="key">A unique identifier for entry</param>
        /// <param name="entry">The content to add to the output cache</param>
        /// <param name="utcExpiry">The time and date on which the cached entry expires</param>
        /// <returns>
        /// The value that identifies what was in the cache, or the value that was just added if it was not
        /// </returns>
        public override object Add(
            string key,
            object entry,
            DateTime utcExpiry)
        {
            // Fix the key
            key = SanitizeKey(key);

            // Make sure that the expiration date is flagged as UTC. The client converts the expiration to 
            // UTC to calculate the UNIX time and this way we can skip the UTC -> ToLocal -> ToUTC chain
            utcExpiry = DateTime.SpecifyKind(utcExpiry, DateTimeKind.Utc);

            // We should only store the item if it's not in the cache. So try to add it and if it 
            // succeeds, return the value we just stored
            if (client.Store(StoreMode.Add, key, entry, utcExpiry))
                return entry;

            // If it's in the cache we should return it
            var retval = client.Get(key);

            // If the item got evicted between the Add and the Get (very rare) we store it anyway, 
            // but this time with Set to make sure it always gets into the cache
            if (retval == null) {
                client.Store(StoreMode.Set, key, entry, utcExpiry);
                retval = entry;
            }

            // Return the value read from the cache if it was present
            return retval;
        }

        /// <summary>
        /// Function to read an item from the output cache and returns it
        /// </summary>
        /// <param name="key">A unique identifier for entry</param>
        /// <returns>
        /// The value that identifies the specified entry in the cache, or null if the specified entry is not in the cache.
        /// </returns>
        public override object Get(
            string key)
        {
            return client.Get(SanitizeKey(key));
        }

        /// <summary>
        /// Function to remove an item from the output cache
        /// </summary>
        /// <param name="key">The unique identifier for the entry to remove from the output cache</param>
        public override void Remove(
            string key)
        {
            client.Remove(SanitizeKey(key));
        }

        /// <summary>
        /// Function to set an item in the output cache
        /// </summary>
        /// <param name="key">A unique identifier for entry</param>
        /// <param name="entry">The content to add to the output cache</param>
        /// <param name="utcExpiry">The time and date on which the cached entry expires</param>
        public override void Set(
            string key,
            object entry,
            DateTime utcExpiry)
        {
            client.Store(StoreMode.Set, SanitizeKey(key), entry, DateTime.SpecifyKind(utcExpiry, DateTimeKind.Utc));
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