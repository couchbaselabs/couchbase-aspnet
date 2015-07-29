using System;
using System.Web.SessionState;
using System.Collections.Generic;
using System.Web;
using System.IO;
using System.Web.UI;
using Couchbase.Core;
using Couchbase.IO;
using System.Linq;

namespace Couchbase.AspNet.SessionState
{
	public class CouchbaseSessionStateProvider : SessionStateStoreProviderBase
	{
		private IBucket client;
        private bool disposeClient;
        internal static TimeSpan SessionExpires = TimeSpan.FromMinutes(90);
        internal static int maxLock = 5;


        public override string Name
        {
            get
            {
                return "Couchbase Session State Provider";
            }
        }

        public override string Description
        {
            get
            {
                return "Implementation of SessionStateStoreProvider using Couchbase as the backend store.";
            }
        }

		public override void Initialize(string name, System.Collections.Specialized.NameValueCollection config)
		{
            // Initialize the base class
			base.Initialize(name, config);

            // Create our Couchbase client instance
            client = ProviderHelper.GetClient(name, config, () => (ICouchbaseClientFactory)new CouchbaseClientFactory(), out disposeClient);
            if (config.AllKeys.Contains("timeout"))
            {
                int sessionTimeout = 90;
				if (int.TryParse(ProviderHelper.GetAndRemove(config, "timeout", false), out sessionTimeout))
                    SessionExpires = TimeSpan.FromMinutes(sessionTimeout);
            }
            if(config.AllKeys.Contains("maxLockTime"))
            {
				int.TryParse(ProviderHelper.GetAndRemove(config, "maxLockTime", false), out maxLock);
            }

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
            ResponseStatus status;
            e.Save(client, id, false, out status, true);
        }

		public override void Dispose()
		{
            //if (disposeClient) {
            //    client.Dispose();
            //}
		}

        public override void EndRequest(HttpContext context) { }

        public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
        {
            locked = false;
            lockId = null;
            lockAge = TimeSpan.Zero;
            actions = SessionStateActions.None;

            var e = SessionStateItem.Load(client, id, false);
            if (e == null)
            {
                actions = SessionStateActions.InitializeItem;
                return null;
            }

            locked = e.LockId > 0; // If the lockID is greater than 0 then it is locked
            lockAge = DateTime.UtcNow - e.LockTime;
            lockId = e.LockId;

            // If it is locked then return null
            return locked ? null : e.ToStoreData(context);
        }

        public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
        {
            locked = false;
            lockId = null;
            lockAge = TimeSpan.Zero;
            actions = SessionStateActions.None;

            // Load only the header
            var e = SessionStateItem.Load(client, id, true);
            if (e == null)
            {
                actions = SessionStateActions.InitializeItem;
                return null;
            }

            // repeat until we save or see it is locked
            // item (i.e. nobody changes it between the 
            // time we get it from the store and updates it s attributes)
            // Save() will return false if Cas() fails
            while (true)
            {
                // It is locked so break out, but only if the lock is less than 5 minutes old
                if (e.LockId > 0 && DateTime.UtcNow - e.LockTime < TimeSpan.FromMinutes(maxLock))
                    break;

                e.LockId = e.HeadCas;
                e.LockTime = DateTime.UtcNow;
                e.Flag = SessionStateActions.None;

                // try to update the item in the store
                ResponseStatus status;
                if (e.Save(client, id, true, out status, false))
                {
                    locked = true;
                    lockId = e.LockId;
                    // We got the lock so load the body
                    if (e.LoadBody(client, id))
                        return e.ToStoreData(context);
                    // we failed to load the body, this could be for any number of reasons but 
                    // it retried many times so return null, this causes them to get a
                    // new session.
                    else
                    {
                        locked = false;
                        lockId = null;
                        actions = SessionStateActions.InitializeItem;
                        e.LockId = 0;
                        e.LockTime = DateTime.MinValue;
                        e.Save(client, id, true, out status, false);
                        return null;
                    }
                }
                // Couldn't save so repeat unless
                // the session seems to have been lost somehow
                if(status == ResponseStatus.KeyNotFound)
                {
                    locked = false;
                    lockId = 0;
                    return null;
                }
                // it has been modified between we loaded and tried to save it
                e = SessionStateItem.Load(client, id, true);
                if (e == null)
                    return null;
            }
            // It was locked so we hit this
            actions = e.Flag;
            locked = e.LockId > 0;
            lockAge = DateTime.UtcNow - e.LockTime;
            lockId = e.LockId;
            actions = e.Flag;

            return null;
        }

        public override void InitializeRequest(HttpContext context)
        {
        }

        public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
        {
            var tmp = (ulong)lockId;
            SessionStateItem e;
            ResponseStatus status = default(ResponseStatus);
            uint retry = 0;
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
            } while (!e.Save(client, id, true, out status, false) && retry++ < 20 && status != ResponseStatus.KeyNotFound);
            if(retry >= 20)
                throw new System.Web.HttpUnhandledException("Failed to release exclusive lock on session item. Status: " + status.ToString());
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
            SessionStateItem.Touch(client, id, SessionExpires);
        }

        public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item, object lockId, bool newItem)
        {
            SessionStateItem e = null;
            ResponseStatus status = default(ResponseStatus);
            uint retry = 0;
            do {
                if (!newItem) {
                    var tmp = (ulong)lockId;

                    // Load the entire item with CAS (need the DataCas value also for the save)
                    e = SessionStateItem.Load(client, id, false);

                    // if we're expecting an existing item, but
                    // it's not in the cache
                    // or it's locked by someone else, then quit
                    // TODO: We should log this
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
            } while (!e.Save(client, id, false, out status, newItem) && retry++ < 20 && status != ResponseStatus.KeyNotFound);
            if (retry >= 20)
                throw new System.Web.HttpUnhandledException("Failed to set and release exclusive lock on session item. Status: " + status.ToString());
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

            [Serializable]
            private class SessionStateItemHeader
            {
                public byte start;
                public SessionStateActions Flag;
                public int Timeout;
                public ulong LockId;
                public long LockTime;
            }

            private void SaveHeader(MemoryStream ms)
            {
                var p = new SessionStateItemHeader()
                {
                    Flag = Flag,
                    Timeout = Timeout,
                    LockId = LockId,
                    LockTime = LockTime.ToBinary()
                };
                new ObjectStateFormatter().Serialize(ms, p);
            }

            public bool Save(IBucket client, string id, bool metaOnly, out ResponseStatus status, bool newItem)
            {
                status = ResponseStatus.ClientFailure;
                using (var ms = new MemoryStream()) {
                    // Save the header first
                    SaveHeader(ms);
                    var ts = TimeSpan.FromMinutes(Timeout);

                    IOperationResult<byte[]> retval;
                    if(newItem)
                    {
                        // If new item then insert rather than upsert
                        retval = client.Insert<byte[]>(HeaderPrefix + id, ms.GetBuffer(), ts);
                        HeadCas = retval.Cas;
                    }
                    else
                    {
                        // Attempt to save the header and fail if the CAS fails
                        retval = client.Replace<byte[]>(HeaderPrefix + id, ms.GetBuffer(), HeadCas, ts);
                        HeadCas = retval.Cas;
                    }

                    status = retval.Status;
                    if (!retval.Success) {
                        return false;
                    }

                    // Save the data
                    if (!metaOnly) {
                        ms.Position = 0;

                        // Serialize the data
                        using (var bw = new BinaryWriter(ms)) {
                            Data.Serialize(bw);
                            if (newItem)
                            {
                                // If new item then insert rather than upsert
                                retval = client.Insert<byte[]>(DataPrefix + id, ms.GetBuffer(), ts);
                                DataCas = retval.Cas;
                            }
                            else
                            { 
                                // Attempt to save the data and fail if the CAS fails
                                retval = client.Replace<byte[]>(DataPrefix + id, ms.GetBuffer(), DataCas, ts);
                                DataCas = retval.Cas;
                            }
                        }
                    }
                    status = retval.Status;
                    // Return the success of the operation
                    return retval.Success;
                }
            }

            private static SessionStateItem LoadItem(MemoryStream ms)
            {
                var header = new ObjectStateFormatter().Deserialize(ms) as SessionStateItemHeader;
                if (header == null)
                    return null;

                var retval = new SessionStateItem()
                {
                    Flag = header.Flag,
                    Timeout = header.Timeout,
                    LockId = header.LockId,
                    LockTime = DateTime.FromBinary((long)header.LockTime)
                };

                return retval;
            }

            public static SessionStateItem Load(IBucket client, string id, bool metaOnly, int? timeout = null)
            {
                return Load(HeaderPrefix, DataPrefix, client, id, metaOnly, timeout);
            }

            public static SessionStateItem Load(string headerPrefix, string dataPrefix, IBucket client, string id, bool metaOnly, int? timeout = null)
            {
                // Load the header for the item
                IOperationResult<byte[]> header = null;
                int retry = 0;
                TimeSpan ts = SessionExpires;
                if(timeout.HasValue) ts = TimeSpan.FromMinutes(timeout.Value);
                header = client.GetAndTouch<byte[]>(headerPrefix + id, ts);

                // We will loop and retry when it might be failing over or having
                // some other temporary failure.
                while ((
                    header.Status == ResponseStatus.TemporaryFailure ||
                    header.Status == ResponseStatus.ClientFailure ||
                    header.Status == ResponseStatus.InternalError ||
                    header.Status == ResponseStatus.VBucketBelongsToAnotherServer
                    ) && retry++ < 11)
                {
                    System.Threading.Thread.Sleep(3000);
                    header = client.GetAndTouch<byte[]>(headerPrefix + id, ts);
                }
                // If we just failed or it wasn't found return null.
                if (header == null || header.Success == false || header.Status == ResponseStatus.KeyNotFound)
                {
                    return null;
                }

                // Deserialize the header values
                SessionStateItem entry = null;
                using (var ms = new MemoryStream(header.Value))
                {
                    entry = SessionStateItem.LoadItem(ms);
                }
                if (entry == null) return null;
                entry.HeadCas = header.Cas;

                // Skip loading the body if we only want the header/lock object
                if (metaOnly) {
                    return entry;
                }

                // Return the session entry
                if (entry.LoadBody(dataPrefix, client, id, timeout))
                    return entry;
                else // Couldn't load the body for some reason
                    return null;
            }

            public bool LoadBody(IBucket client, string id, int? timeout = null)
            {
                return LoadBody(DataPrefix, client, id, timeout);
            }

            public bool LoadBody(string dataPrefix, IBucket client, string id, int? timeout = null)
            {
                TimeSpan ts = SessionExpires;
                if (timeout.HasValue) ts = TimeSpan.FromMinutes(timeout.Value);
                // Load the data for the item
                var data = client.GetAndTouch<byte[]>(dataPrefix + id, ts);
                int retry = 0;
                while ((
                        data.Status == ResponseStatus.TemporaryFailure ||
                        data.Status == ResponseStatus.ClientFailure ||
                        data.Status == ResponseStatus.InternalError ||
                        data.Status == ResponseStatus.VBucketBelongsToAnotherServer) && retry++ < 11)
                {
                    System.Threading.Thread.Sleep(3000);
                    data = client.GetAndTouch<byte[]>(dataPrefix + id, ts);
                }
                if (data == null || data.Success == false || data.Status == ResponseStatus.KeyNotFound || data.Value == null)
                {
                    return false;
                }
                DataCas = data.Cas;

                // Deserialize the data
                using (var ms = new MemoryStream(data.Value))
                {
                    using (var br = new BinaryReader(ms))
                    {
                        Data = SessionStateItemCollection.Deserialize(br);
                    }
                }
                return Data != null;
            }
            

            public SessionStateStoreData ToStoreData(HttpContext context)
            {
                return new SessionStateStoreData(Data, SessionStateUtility.GetSessionStaticObjects(context), Timeout);
            }

            public static void Remove(IBucket client, string id)
            {
                // Do a bulk remove
                client.Remove(new string[] { DataPrefix + id, HeaderPrefix + id });
            }

            internal static void Touch(IBucket client, string id, TimeSpan timeout)
            {
                Touch(HeaderPrefix, DataPrefix, client, id, timeout);
            }

            internal static void Touch(string headerPrefix, string dataPrefix, IBucket client, string id, TimeSpan timeout)
            {
                var result = client.Touch(headerPrefix + id, timeout);
                result = client.Touch(dataPrefix + id, timeout);

            }
        }

		#endregion
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