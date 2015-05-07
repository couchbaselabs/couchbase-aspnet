using System;
using System.Web.SessionState;
using System.Web;
using System.IO;
using System.Web.UI;
using Couchbase.Core;
using Couchbase.IO;

namespace Couchbase.AspNet.SessionState
{
	public class CouchbaseSessionStateProvider : SessionStateStoreProviderBase
	{
		private IBucket client;
        private bool disposeClient;
        private static bool exclusiveAccess;

		public override void Initialize(string name, System.Collections.Specialized.NameValueCollection config)
		{
            // Initialize the base class
			base.Initialize(name, config);

            // Create our Couchbase client instance
            client = ProviderHelper.GetClient(name, config, () => (ICouchbaseClientFactory)new CouchbaseClientFactory(), out disposeClient);

            // By default use exclusive session access. But allow it to be overridden in the config file
            var exclusive = ProviderHelper.GetAndRemove(config, "exclusiveAccess", false) ?? "true";
            exclusiveAccess = (String.Compare(exclusive, "true", true) == 0);

            // Make sure no extra attributes are included
			ProviderHelper.CheckForUnknownAttributes(config);
		}

		public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
		{
			return new SessionStateStoreData(new SessionStateItemCollection(), SessionStateUtility.GetSessionStaticObjects(context), timeout);
		}

        public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
        {
            var e = new SessionStateItem {
                Data = new SessionStateItemCollection(),
                Flag = SessionStateActions.InitializeItem,
                LockId = 0,
                Timeout = timeout
            };

            e.Save(client, id, false, false);
        }

		public override void Dispose()
		{
            if (disposeClient) {
                client.Dispose();
            }
		}

        public override void EndRequest(HttpContext context) { }

        public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
        {
            var e = Get(client, context, false, id, out locked, out lockAge, out lockId, out actions);

            return (e == null)
                    ? null
                    : e.ToStoreData(context);
        }

        public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
        {
            var e = Get(client, context, true, id, out locked, out lockAge, out lockId, out actions);

            return (e == null)
                    ? null
                    : e.ToStoreData(context);
        }

        public static SessionStateItem Get(IBucket client, HttpContext context, bool acquireLock, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
        {
            locked = false;
            lockId = null;
            lockAge = TimeSpan.Zero;
            actions = SessionStateActions.None;

            var e = SessionStateItem.Load(client, id, false);
            if (e == null)
                return null;

            if (acquireLock) {
                // repeat until we can update the retrieved 
                // item (i.e. nobody changes it between the 
                // time we get it from the store and updates it s attributes)
                // Save() will return false if Cas() fails
                while (true) {
                    if (e.LockId > 0)
                        break;

                    actions = e.Flag;

                    e.LockId = exclusiveAccess ? e.HeadCas : 0;
                    e.LockTime = DateTime.UtcNow;
                    e.Flag = SessionStateActions.None;

                    // try to update the item in the store
                    if (e.Save(client, id, true, exclusiveAccess)) {
                        locked = true;
                        lockId = e.LockId;

                        return e;
                    }

                    // it has been modified between we loaded and tried to save it
                    e = SessionStateItem.Load(client, id, false);
                    if (e == null)
                        return null;
                }
            }

            locked = true;
            lockAge = DateTime.UtcNow - e.LockTime;
            lockId = e.LockId;
            actions = SessionStateActions.None;

            return acquireLock ? null : e;
        }

        public override void InitializeRequest(HttpContext context)
        {
        }

        public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
        {
            var tmp = (ulong)lockId;
            SessionStateItem e;
            do {
                // Load the header for the item with CAS
                e = SessionStateItem.Load(client, id, true);

                // Bail if the entry does not exist, or the lock ID does not match our lock ID
                if (e == null || e.LockId != tmp) {
                    break;
                }

                // Attempt to clear the lock for this item and loop around until we succeed
                e.LockId = 0;
                e.LockTime = DateTime.MinValue;
            } while (!e.Save(client, id, true, exclusiveAccess));
        }

        public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
        {
            var tmp = (ulong)lockId;
            var e = SessionStateItem.Load(client, id, true);

            if (e != null && e.LockId == tmp) {
                SessionStateItem.Remove(client, id);
            }
        }

        public override void ResetItemTimeout(HttpContext context, string id)
        {
            SessionStateItem e;
            do {
                // Load the item with CAS
                e = SessionStateItem.Load(client, id, false);
                if (e == null) {
                    break;
                }

                // Try to save with CAS, and loop around until we succeed
            } while (!e.Save(client, id, false, exclusiveAccess));
        }

        public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item, object lockId, bool newItem)
        {
            SessionStateItem e = null;
            do {
                if (!newItem) {
                    var tmp = (ulong)lockId;

                    // Load the entire item with CAS (need the DataCas value also for the save)
                    e = SessionStateItem.Load(client, id, false);

                    // if we're expecting an existing item, but
                    // it's not in the cache
                    // or it's locked by someone else, then quit
                    if (e == null || e.LockId != tmp) {
                        return;
                    }
                } else {
                    // Create a new item if it requested
                    e = new SessionStateItem();
                }

                // Set the new data and reset the locks
                e.Timeout = item.Timeout;
                e.Data = (SessionStateItemCollection)item.Items;
                e.Flag = SessionStateActions.None;
                e.LockId = 0;
                e.LockTime = DateTime.MinValue;

                // Attempt to save with CAS and loop around if it fails
            } while (!e.Save(client, id, false, exclusiveAccess && !newItem));
        }

        public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
        {
            return false;
        }

		#region [ SessionStateItem             ]

        public class SessionStateItem
        {
            private static readonly string HeaderPrefix = (System.Web.Hosting.HostingEnvironment.SiteName ?? String.Empty).Replace(" ", "-") + "+" + System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath + "info-";
            private static readonly string DataPrefix = (System.Web.Hosting.HostingEnvironment.SiteName ?? String.Empty).Replace(" ", "-") + "+" + System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath + "data-";

            public SessionStateItemCollection Data;
            public SessionStateActions Flag;
            public ulong LockId;
            public DateTime LockTime;

            // this is in minutes
            public int Timeout;

            public ulong HeadCas;
            public ulong DataCas;

            private void SaveHeader(MemoryStream ms)
            {
                var p = new Pair(
                                    (byte)1,
                                    new Triplet(
                                                    (byte)Flag,
                                                    Timeout,
                                                    new Pair(
                                                                LockId,
                                                                LockTime.ToBinary()
                                                            )
                                                )
                                );

                new ObjectStateFormatter().Serialize(ms, p);
            }

            public bool Save(IBucket client, string id, bool metaOnly, bool useCas)
            {
                using (var ms = new MemoryStream()) {
                    // Save the header first
                    SaveHeader(ms);
                    var ts = TimeSpan.FromMinutes(Timeout);

                    // Attempt to save the header and fail if the CAS fails
                    var retval = useCas
                        ? client.Upsert(HeaderPrefix + id, new ArraySegment<byte>(ms.GetBuffer(), 0, (int) ms.Length), HeadCas, ts)
                        : client.Upsert(HeaderPrefix + id, new ArraySegment<byte>(ms.GetBuffer(), 0, (int) ms.Length), ts);

                    if (!retval.Success) {
                        return false;
                    }

                    // Save the data
                    if (!metaOnly) {
                        ms.Position = 0;

                        // Serialize the data
                        using (var bw = new BinaryWriter(ms)) {
                            Data.Serialize(bw);

                            // Attempt to save the data and fail if the CAS fails
                            retval = useCas
                                ? client.Upsert(DataPrefix + id, new ArraySegment<byte>(ms.GetBuffer(), 0, (int)ms.Length), DataCas, ts)
                                : client.Upsert(DataPrefix + id, new ArraySegment<byte>(ms.GetBuffer(), 0, (int)ms.Length), ts);
                        }
                    }

                    // Return the success of the operation
                    return retval.Success;
                }
            }

            private static SessionStateItem LoadItem(MemoryStream ms)
            {
                var graph = new ObjectStateFormatter().Deserialize(ms) as Pair;
                if (graph == null)
                    return null;

                if (((byte)graph.First) != 1)
                    return null;

                var t = (Triplet)graph.Second;
                var retval = new SessionStateItem();

                retval.Flag = (SessionStateActions)((byte)t.First);
                retval.Timeout = (int)t.Second;

                var lockInfo = (Pair)t.Third;

                retval.LockId = (ulong)lockInfo.First;
                retval.LockTime = DateTime.FromBinary((long)lockInfo.Second);

                return retval;
            }

            public static SessionStateItem Load(IBucket client, string id, bool metaOnly)
            {
                return Load(HeaderPrefix, DataPrefix, client, id, metaOnly);
            }

            public static SessionStateItem Load(string headerPrefix, string dataPrefix, IBucket client, string id, bool metaOnly)
            {
                // Load the header for the item
                IOperationResult<byte[]> header = null;
                try
                {
                    header = client.Get<byte[]>(headerPrefix + id);
                }
                catch
                {
                    //TODO: Log the exception
                }
                if(header == null) return null;
                if (header.Status == ResponseStatus.KeyNotFound) {
                    return null;
                }

                int retry = 0;
                while (header.Status == ResponseStatus.TemporaryFailure && retry++ < 12)
                {
                    System.Threading.Thread.Sleep(3000);
                    header = client.Get<byte[]>(headerPrefix + id);
                }
                // Deserialize the header values
                SessionStateItem entry = null;
                try
                {
                    using (var ms = new MemoryStream(header.Value))
                    {
                        entry = SessionStateItem.LoadItem(ms);
                    }
                }
                catch
                {
                    //TODO: log error
                }
                if (entry == null) return null;
                entry.HeadCas = header.Cas;

                // Bail early if we are only loading the meta data
                if (metaOnly) {
                    return entry;
                }

                // Load the data for the item
                var data = client.Get<byte[]>(dataPrefix + id);
                if (data.Value == null) {
                    return null;
                }
                entry.DataCas = data.Cas;

                // Deserialize the data
                using (var ms = new MemoryStream(data.Value)) {
                    using (var br = new BinaryReader(ms)) {
                        entry.Data = SessionStateItemCollection.Deserialize(br);
                    }
                }

                // Return the session entry
                return entry;
            }

            public SessionStateStoreData ToStoreData(HttpContext context)
            {
                return new SessionStateStoreData(Data, SessionStateUtility.GetSessionStaticObjects(context), Timeout);
            }

            public static void Remove(IBucket client, string id)
            {
                client.Remove(DataPrefix + id);
                client.Remove(HeaderPrefix + id);
            }
        }

		#endregion
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