using System;
using System.Collections.Specialized;
using System.Configuration;
using System.Web.SessionState;
using System.Web;
using System.Web.Configuration;
using Common.Logging;
using Couchbase.Core;
using Couchbase.IO;
using Newtonsoft.Json;

namespace Couchbase.AspNet.SessionState
{
    /// <summary>
    /// A <see cref="SessionStateStoreProviderBase"/> implementation which uses Couchbase Server as the backing store.
    /// </summary>
    public class CouchbaseSessionStateProvider : SessionStateStoreProviderBase
    {
        private IBucket _bucket;
        private static bool _exclusiveAccess;
        private int _maxRetryCount = 5;
        private string _configName;
        private string _bucketName;
        private static TimeSpan _executionTimeout;

        private static readonly object _syncObj = new object();

        private static readonly ILog _log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// Required default ctor for ASP.NET
        /// </summary>
        public CouchbaseSessionStateProvider()
        {
        }

        /// <summary>
        /// Gets the name of the provider
        /// </summary>
        public override string Name { get { return GetType().Name; } }

        /// <summary>
        /// Gets a description of the provider.
        /// </summary>
        public override string Description { get { return "Implementation of SessionStateStoreProvider using Couchbase Server as the backend store."; } }

        /// <summary>
        /// For unit testing only.
        /// </summary>
        /// <param name="bucket"></param>
        public CouchbaseSessionStateProvider(IBucket bucket)
        {
            _bucket = bucket;
        }

        /// <summary>
        /// Defines the prefix for header data in the Couchbase bucket. Must be unique to ensure it does not conflict with 
        /// other applications that might be using the Couchbase bucket.
        /// </summary>
        public static string HeaderPrefix =
            (System.Web.Hosting.HostingEnvironment.SiteName ?? string.Empty).Replace(" ", "-") + "+" +
            System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath + "info-";

        /// <summary>
        /// Defines the prefix for the actual session store data stored in the Couchbase bucket. Must also be unique for
        /// the same reasons outlined above.
        /// </summary>
        public static string DataPrefix =
            (System.Web.Hosting.HostingEnvironment.SiteName ?? string.Empty).Replace(" ", "-") + "+" +
            System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath + "data-";

        /// <summary>
        /// Function to initialize the provider
        /// </summary>
        /// <param name="name">Name of the element in the configuration file</param>
        /// <param name="config">Configuration values for the provider from the Web.config file</param>
        public override void Initialize(
            string name,
            NameValueCollection config)
        {
            // Initialize the base class
            base.Initialize(name, config);

            _configName = name;
            AppDomain.CurrentDomain.DomainUnload += Application_End;
            ClusterClient.Configure(name, config);

            lock (_syncObj) {
                // Create the bucket based off the name provided in the
                _bucketName = ProviderHelper.GetAndRemove(config, "bucket", false);
                _bucket = ClusterClient.GetBucket(name, _bucketName);

                var section = (HttpRuntimeSection)ConfigurationManager.GetSection("system.web/httpRuntime");
                _executionTimeout = section.ExecutionTimeout;
            }

            // By default use exclusive session access. But allow it to be overridden in the config file
            var exclusive = ProviderHelper.GetAndRemove(config, "exclusiveAccess", false) ?? "true";
            _exclusiveAccess = (string.Compare(exclusive, "true", StringComparison.OrdinalIgnoreCase) == 0);

            // Allow optional header and data prefixes to be used for this application
            var headerPrefix = ProviderHelper.GetAndRemove(config, "headerPrefix", false);
            if (headerPrefix != null) {
                HeaderPrefix = headerPrefix;
            }
            var dataPrefix = ProviderHelper.GetAndRemove(config, "dataPrefix", false);
            if (dataPrefix != null) {
                DataPrefix = dataPrefix;
            }
            var maxRetryCount = ProviderHelper.GetAndRemove(config, "maxRetryCount", false);
            var temp = 0;
            if (int.TryParse(maxRetryCount, out temp)) {
                _maxRetryCount = temp;
            }

            // Make sure no extra attributes are included
            ProviderHelper.CheckForUnknownAttributes(config);
        }

        /// <summary>
        /// Handle disposing of the session-state store object
        /// </summary>
        public override void Dispose()
        {
        }

        /// <summary>
        /// Handles the End event of the Application control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="EventArgs"/> instance containing the event data.</param>
        public void Application_End(
            object sender,
            EventArgs e)
        {
            try {
                ClusterClient.CloseOne(_configName, _bucketName);
            } catch (Exception) {
                //the app domain has already shutdown
            }
        }

        /// <summary>
        /// Takes as input the HttpContext instance for the current request and performs any 
        /// initialization required by the session-state store provider. 
        /// </summary>
        /// <param name="context">HttpContext for the current request</param>
        public override void InitializeRequest(
            HttpContext context)
        {
        }

        /// <summary>
        /// Takes as input the HttpContext instance for the current request and performs any 
        /// cleanup required by the session-state store provider.
        /// </summary>
        /// <param name="context">HttpContext for the current request</param>
        public override void EndRequest(
            HttpContext context)
        {
        }

        /// <summary>
        /// Takes as input the HttpContext instance for the current request and the Timeout value for the 
        /// current session, and returns a new SessionStateStoreData object with an empty 
        /// ISessionStateItemCollection object, an HttpStaticObjectsCollection collection, and the 
        /// specified Timeout value. The HttpStaticObjectsCollection instance for the ASP.NET application 
        /// can be retrieved using the GetSessionStaticObjects method.
        /// </summary>
        /// <param name="context">HttpContext for the current request</param>
        /// <param name="timeout">Timeout value for the session</param>
        /// <returns>New SessionStateStoreData object for storing the session state data</returns>
        public override SessionStateStoreData CreateNewStoreData(
            HttpContext context,
            int timeout)
        {
            return new SessionStateStoreData(new SessionStateItemCollection(),
                SessionStateUtility.GetSessionStaticObjects(context),
                timeout);
        }

        /// <summary>
        /// Creates an uninitialized item in the database. This is only used for cookieless sessions
        /// regenerateExpiredSessionId attribute is set to true, which causes SessionStateModule to 
        /// generate a new SessionID value when an expired session ID is encountered.
        /// </summary>
        /// <param name="context">HttpContext for the current request</param>
        /// <param name="id">Session ID for the new session</param>
        /// <param name="timeout">Timeout value for the session</param>
        public override void CreateUninitializedItem(
            HttpContext context,
            string id,
            int timeout)
        {
            var e = new SessionStateItem {
                Data = new SessionStateItemCollection(),
                Flag = SessionStateActions.InitializeItem,
                LockId = 0,
                Timeout = timeout
            };

            bool keyNotFound;
            e.SaveAll(_bucket, id, false, out keyNotFound);
        }

        /// <summary>
        /// Returns read-only session-state data from the session data store
        /// </summary>
        /// <param name="context">HttpContext for the current request</param>
        /// <param name="id">Session ID for the session</param>
        /// <param name="locked">Returns true if the session item is locked, otherwise false</param>
        /// <param name="lockAge">Returns the amount of time that the item has been locked</param>
        /// <param name="lockId">Returns lock identifier for the current request</param>
        /// <param name="actions">Indicates whether the current sessions is an uninitialized, cookieless session</param>
        /// <returns>SessionStateStoreData object containing the session state data</returns>
        public override SessionStateStoreData GetItem(
            HttpContext context,
            string id,
            out bool locked,
            out TimeSpan lockAge,
            out object lockId,
            out SessionStateActions actions)
        {
            // this override is busted; according to the spec it should not return the sessionstate object if it's currently locked, yet it does
            var e = GetSessionStoreItem(_bucket, context, false, id, out locked, out lockAge, out lockId, out actions);
            return (e == null) ? null : e.ToStoreData(context);
        }

        /// <summary>
        /// Returns read-write session-state data from the session data store
        /// </summary>
        /// <param name="context">HttpContext for the current request</param>
        /// <param name="id">Session ID for the session</param>
        /// <param name="locked">Returns true if the session item is locked, otherwise false</param>
        /// <param name="lockAge">Returns the amount of time that the item has been locked</param>
        /// <param name="lockId">Returns lock identifier for the current request</param>
        /// <param name="actions">Indicates whether the current sessions is an uninitialized, cookieless session</param>
        /// <returns>SessionStateStoreData object containing the session state data</returns>
        public override SessionStateStoreData GetItemExclusive(
            HttpContext context,
            string id,
            out bool locked,
            out TimeSpan lockAge,
            out object lockId,
            out SessionStateActions actions)
        {
            var e = GetSessionStoreItem(_bucket, context, true, id, out locked, out lockAge, out lockId, out actions);
            return (e == null) ? null : e.ToStoreData(context);
        }

        /// <summary>
        /// Retrieves the session data from the data source. If the lockRecord parameter is true 
        /// (in the case of GetItemExclusive), then the record is locked and we return a new lockId 
        /// and lockAge.
        /// </summary>
        /// <param name="bucket">Reference to the couchbase bucket we are using</param>
        /// <param name="context">HttpContext for the current request</param>
        /// <param name="acquireLock">True to aquire the lock, false to not aquire it</param>
        /// <param name="id">Session ID for the session</param>
        /// <param name="locked">Returns true if the session item is locked, otherwise false</param>
        /// <param name="lockAge">Returns the amount of time that the item has been locked</param>
        /// <param name="lockId">Returns lock identifier for the current request</param>
        /// <param name="actions">Indicates whether the current sessions is an uninitialized, cookieless session</param>
        /// <returns>SessionStateItem object containing the session state data</returns>
        public static SessionStateItem GetSessionStoreItem(
            IBucket bucket,
            HttpContext context,
            bool acquireLock,
            string id,
            out bool locked,
            out TimeSpan lockAge,
            out object lockId,
            out SessionStateActions actions)
        {
            locked = false;
            lockId = null;
            lockAge = TimeSpan.Zero;
            actions = SessionStateActions.None;

            var e = SessionStateItem.Load(bucket, id, false);
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

                    e.LockId = _exclusiveAccess ? e.HeadCas : 0;
                    e.LockTime = DateTime.UtcNow;
                    e.Flag = SessionStateActions.None;

                    ResponseStatus status;
                    // try to update the item in the store
                    if (e.SaveHeader(bucket, id, _exclusiveAccess, out status)) {
                        locked = true;
                        lockId = e.LockId;

                        return e;
                    }
                    if (status == ResponseStatus.KeyNotFound) {
                        break;
                    }

                    // it has been modified between we loaded and tried to save it
                    e = SessionStateItem.Load(bucket, id, false);
                    if (e == null)
                        return null;

                    _log.Warn(JsonConvert.SerializeObject(
                        new {
                            message = "AcquireLock loop reached in GetSessionStoreItem",
                            id,
                            e.LockTime, // the time the lock was acquired (from a different request)
                            e.LockId,   // the CAS value (from the same different request)
                            status
                        }
                    ));
                }
            }

            locked = true;
            lockAge = DateTime.UtcNow - e.LockTime;
            lockId = e.LockId;
            actions = SessionStateActions.None;

            if (acquireLock && _log.IsInfoEnabled) {
                _log.Info(JsonConvert.SerializeObject(
                    new {
                        message = "SessionState object is locked",
                        id,
                        lockAge,
                        lockId,
                        ExecutionTimeout = _executionTimeout.TotalSeconds
                    }
                ));
            }

            return acquireLock ? null : e;
        }

        /// <summary>
        /// Updates the session-item information in the session-state data store with values 
        /// from the current request, and clears the lock on the data
        /// </summary>
        /// <param name="context">HttpContext for the current request</param>
        /// <param name="id">Session ID for the session</param>
        /// <param name="item">The session state item to be stored</param>
        /// <param name="lockId">The lock identifier for the current request</param>
        /// <param name="newItem">True if this is a new session item, false if it is an existing item</param>
        public override void SetAndReleaseItemExclusive(
            HttpContext context,
            string id,
            SessionStateStoreData item,
            object lockId,
            bool newItem)
        {
            bool keyNotFound;
            var retries = 0;
            SessionStateItem e;
            do {
                if (!newItem) {
                    var tmp = (ulong)lockId;

                    // Load the entire item with CAS (need the DataCas value also for the save)
                    e = SessionStateItem.Load(_bucket, id, false);

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
            } while (!e.SaveAll(_bucket, id, _exclusiveAccess && !newItem, out keyNotFound) && retries++ < _maxRetryCount && !keyNotFound);
        }

        /// <summary>
        /// Releases a lock on an item in the session data store
        /// </summary>
        /// <param name="context">HttpContext for the current request</param>
        /// <param name="id">Session ID for the session</param>
        /// <param name="lockId">The lock identifier for the current request</param>
        public override void ReleaseItemExclusive(
            HttpContext context,
            string id,
            object lockId)
        {
            ResponseStatus status;
            var retries = 0;
            var tmp = (ulong)lockId;
            SessionStateItem e;
            do {
                // Load the header for the item with CAS
                e = SessionStateItem.Load(_bucket, id, true);

                // Bail if the entry does not exist, or the lock ID does not match our lock ID
                if (e == null || e.LockId != tmp) {
                    break;
                }

                // Attempt to clear the lock for this item and loop around until we succeed
                e.LockId = 0;
                e.LockTime = DateTime.MinValue;
            } while (!e.SaveHeader(_bucket, id, _exclusiveAccess, out status) && retries++ < _maxRetryCount && status != ResponseStatus.KeyNotFound);
        }

        /// <summary>
        /// Deletes item data from the session data store
        /// </summary>
        /// <param name="context">HttpContext for the current request</param>
        /// <param name="id">Session ID for the session</param>
        /// <param name="lockId">The lock identifier for the current request</param>
        /// <param name="item">Item to be deleted (ignored)</param>
        public override void RemoveItem(
            HttpContext context,
            string id,
            object lockId,
            SessionStateStoreData item)
        {
            var tmp = (ulong)lockId;
            var e = SessionStateItem.Load(_bucket, id, true);

            if (e != null && e.LockId == tmp) {
                SessionStateItem.Remove(_bucket, id);
            }
        }

        /// <summary>
        /// Updates the expiration date and time of an item in the session data store
        /// </summary>
        /// <param name="context">HttpContext for the current request</param>
        /// <param name="id">Session ID for the session</param>
        public override void ResetItemTimeout(
            HttpContext context,
            string id)
        {
            bool keyNotFound;
            var retries = 0;
            SessionStateItem e;
            do {
                // Load the item with CAS
                e = SessionStateItem.Load(_bucket, id, false);
                if (e == null) {
                    break;
                }

                // Try to save with CAS, and loop around until we succeed
            } while (!e.SaveAll(_bucket, id, _exclusiveAccess, out keyNotFound) && retries++ < _maxRetryCount && !keyNotFound);
        }

        /// <summary>
        /// Function to set the session data expiration callback. Since this is not supported
        /// by our database, we simply return false here and do manual session cleanup.
        /// </summary>
        /// <param name="expireCallback">Session expiration callback to set</param>
        /// <returns>False, since we don't support this feature</returns>
        public override bool SetItemExpireCallback(
            SessionStateItemExpireCallback expireCallback)
        {
            return false;
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
