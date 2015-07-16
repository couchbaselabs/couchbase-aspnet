using System;
using System.Collections.Specialized;
using Couchbase.Core;

namespace Couchbase.AspNet
{
    internal static class ProviderHelper
    {
        public static string GetAndRemove(NameValueCollection nvc, string name, bool required)
        {
            var tmp = nvc[name];
            if (tmp == null) {
                if (required)
                    throw new System.Configuration.ConfigurationErrorsException("Missing parameter: " + name);
            } else {
                nvc.Remove(name);
            }

            return tmp;
        }

        public static void CheckForUnknownAttributes(NameValueCollection nvc)
        {
            if (nvc.Count > 0)
                throw new System.Configuration.ConfigurationErrorsException("Unknown parameter: " + nvc.Keys[0]);
        }

        public static IBucket GetClient(string name, NameValueCollection config, Func<ICouchbaseClientFactory> createDefault, out bool disposeClient)
        {
            var factory = GetFactoryInstance(ProviderHelper.GetAndRemove(config, "factory", false), createDefault);
            System.Diagnostics.Debug.Assert(factory != null, "factory == null");

            return factory.Create(name, config, out disposeClient);
        }

        private static ICouchbaseClientFactory GetFactoryInstance(string typeName, Func<ICouchbaseClientFactory> createDefault)
        {
            if (String.IsNullOrEmpty(typeName))
                return createDefault();

            var type = Type.GetType(typeName, false);
            if (type == null)
                throw new System.Configuration.ConfigurationErrorsException("Could not load type: " + typeName);

            if (!typeof(ICouchbaseClientFactory).IsAssignableFrom(type))
                throw new System.Configuration.ConfigurationErrorsException("Type '" + typeName + "' must implement ICouchbaseClientFactory");

            return (ICouchbaseClientFactory )Activator.CreateInstance(type);
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