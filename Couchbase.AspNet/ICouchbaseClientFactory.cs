using System.Collections.Specialized;
using Enyim.Caching;

namespace Couchbase.AspNet
{
    public interface ICouchbaseClientFactory
    {
        /// <summary>
        /// Returns a Couchbase client. This will be called by the provider's Initialize method. Note
        /// that the instance of the client returned will be owned by the called, and will be disposed.
        /// So make sure you don't return a shared instance, but create a new one.
        /// </summary>
        /// <param name="name">Name of the section from the configuration file</param>
        /// <param name="config">Configuration section information from the config file</param>
        /// <param name="disposeClient">True if the client should be disposed of or not</param>
        /// <returns>Instance of the couchbase client to use</returns>
        IMemcachedClient Create(string name, NameValueCollection config, out bool disposeClient);
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
