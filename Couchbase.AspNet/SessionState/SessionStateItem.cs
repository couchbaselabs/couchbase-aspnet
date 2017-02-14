using System;
using System.IO;
using System.Web;
using System.Web.SessionState;
using System.Web.UI;
using Common.Logging;
using Couchbase.Core;
using Couchbase.IO;
using Newtonsoft.Json;

namespace Couchbase.AspNet.SessionState
{
    /// <summary>
    /// Internal class for handling the storage of the session items in Couchbase
    /// </summary>
    public class SessionStateItem
    {
        public SessionStateItemCollection Data;
        public SessionStateActions Flag;
        public ulong LockId;
        public DateTime LockTime;

        // Timeout value for the session store item (defined in minutes)
        public int Timeout;

        public ulong HeadCas;
        public ulong DataCas;

        private static readonly ILog _log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// Writes the header to the stream
        /// </summary>
        /// <param name="s">Stream to write the header to</param>
        private void WriteHeader(
            Stream s)
        {
            var p = new Pair(
                (byte)1,
                new Triplet(
                    (byte)Flag,
                    Timeout,
                    new Pair(
                        LockId,
                        LockTime.ToBinary()))
            );
            new ObjectStateFormatter().Serialize(s, p);
        }

        /// <summary>
        /// Saves the session store header into Couchbase
        /// </summary>
        /// <param name="bucket">Couchbase bucket to save to</param>
        /// <param name="id">Session ID</param>
        /// <param name="useCas">True to use a check and set, false to simply store it</param>
        /// <returns>True if the value was saved, false if not</returns>
        public bool SaveHeader(
            IBucket bucket,
            string id,
            bool useCas,
            out ResponseStatus status)
        {
            using (var ms = new MemoryStream()) {
                WriteHeader(ms);
                var ts = TimeSpan.FromMinutes(Timeout);

                // Attempt to write the header and fail if the CAS fails
                var retval = useCas
                    ? bucket.Upsert(CouchbaseSessionStateProvider.HeaderPrefix + id, ms.ToArray(), HeadCas, ts)
                    : bucket.Upsert(CouchbaseSessionStateProvider.HeaderPrefix + id, ms.ToArray(), ts);

                status = retval.Status;
                return retval.Success;
            }
        }

        /// <summary>
        /// Saves the session store data into Couchbase
        /// </summary>
        /// <param name="bucket">Couchbase bucket to save to</param>
        /// <param name="id">Session ID</param>
        /// <param name="useCas">True to use a check and set, false to simply store it</param>
        /// <param name="status">The <see cref="ResponseStatus"/> from the server.</param>
        /// <returns>True if the value was saved, false if not</returns>
        public bool SaveData(
            IBucket bucket,
            string id,
            bool useCas,
            out ResponseStatus status)
        {
            var ts = TimeSpan.FromMinutes(Timeout);
            using (var ms = new MemoryStream())
                using (var bw = new BinaryWriter(ms)) {
                    Data.Serialize(bw);

                    // Attempt to save the data and fail if the CAS fails
                    var retval = useCas
                        ? bucket.Upsert(CouchbaseSessionStateProvider.DataPrefix + id, ms.ToArray(), DataCas, ts)
                        : bucket.Upsert(CouchbaseSessionStateProvider.DataPrefix + id, ms.ToArray(), ts);

                    status = retval.Status;
                    return retval.Success;
                }
        }

        /// <summary>
        /// Saves the session store into Couchbase
        /// </summary>
        /// <param name="bucket">Couchbase bucket to save to</param>
        /// <param name="id">Session ID</param>
        /// <param name="useCas">True to use a check and set, false to simply store it</param>
        /// <param name="keyNotFound">True if <see cref="ResponseStatus.KeyNotFound"/> is returned for the body or the header.</param>
        /// <returns>True if the value was saved, false if not</returns>
        public bool SaveAll(
            IBucket bucket,
            string id,
            bool useCas,
            out bool keyNotFound)
        {
            var dataStatus = ResponseStatus.None;
            var headerStatus = ResponseStatus.None;

            var failed = SaveData(bucket, id, useCas, out dataStatus) && SaveHeader(bucket, id, useCas, out headerStatus);
            keyNotFound = dataStatus == ResponseStatus.KeyNotFound || headerStatus == ResponseStatus.KeyNotFound;
            return failed;
        }

        /// <summary>
        /// Loads a sessions store header data from the passed in stream
        /// </summary>
        /// <param name="s">Stream to load the item from</param>
        /// <returns>Value read from the stream, null on failure</returns>
        private static SessionStateItem LoadHeader(
            Stream s)
        {
            var graph = new ObjectStateFormatter().Deserialize(s) as Pair;
            if (graph == null)
                return null;

            if (((byte)graph.First) != 1)
                return null;

            var t = (Triplet)graph.Second;
            var retval = new SessionStateItem {
                Flag = (SessionStateActions)((byte)t.First),
                Timeout = (int)t.Second
            };

            var lockInfo = (Pair)t.Third;

            retval.LockId = (ulong)lockInfo.First;
            retval.LockTime = DateTime.FromBinary((long)lockInfo.Second);

            return retval;
        }

        /// <summary>
        /// Loads a session state item from the bucket
        /// </summary>
        /// <param name="bucket">Couchbase bucket to load from</param>
        /// <param name="id">Session ID</param>
        /// <param name="metaOnly">True to load only meta data</param>
        /// <returns>Session store item read, null on failure</returns>
        public static SessionStateItem Load(
            IBucket bucket,
            string id,
            bool metaOnly)
        {
            return Load(CouchbaseSessionStateProvider.HeaderPrefix, CouchbaseSessionStateProvider.DataPrefix, bucket, id, metaOnly);
        }

        /// <summary>
        /// Loads a session state item from the bucket. This function is publicly accessible
        /// so that you have direct access to session data from another application if necesssary.
        /// We use this so our front end code can determine if an employee is logged into our back
        /// end application to give them special permissions, without the session data being actually common
        /// between the two applications.
        /// </summary>
        /// <param name="headerPrefix">Prefix for the header data</param>
        /// <param name="dataPrefix">Prefix for the real data</param>
        /// <param name="bucket">Couchbase bucket to load from</param>
        /// <param name="id">Session ID</param>
        /// <param name="metaOnly">True to load only meta data</param>
        /// <returns>Session store item read, null on failure</returns>
        public static SessionStateItem Load(
            string headerPrefix,
            string dataPrefix,
            IBucket bucket,
            string id,
            bool metaOnly)
        {
            // Read the header value from Couchbase
            var header = bucket.Get<byte[]>(CouchbaseSessionStateProvider.HeaderPrefix + id);
            if (header.Status != ResponseStatus.Success) {
                if (header.Status != ResponseStatus.KeyNotFound) {
                    _log.Warn(JsonConvert.SerializeObject(
                        new {
                            Key = CouchbaseSessionStateProvider.HeaderPrefix + id,
                            Status = header.Status.ToString(),
                            header.Message,
                            header.Exception
                        }
                    ));
                }
                return null;
            }

            if (_log.IsDebugEnabled) {
                _log.Debug(JsonConvert.SerializeObject(
                    new {
                        Key = CouchbaseSessionStateProvider.HeaderPrefix + id,
                        Status = header.Status.ToString(),
                        header.Message,
                        header.Exception
                    }
                ));
            }
            // Deserialize the header values
            SessionStateItem entry;
            using (var ms = new MemoryStream(header.Value)) {
                entry = LoadHeader(ms);
            }
            entry.HeadCas = header.Cas;

            // Bail early if we are only loading the meta data
            if (metaOnly) {
                return entry;
            }

            // Read the data for the item from Couchbase
            var data = bucket.Get<byte[]>(CouchbaseSessionStateProvider.DataPrefix + id);
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

        /// <summary>
        /// Creates a session store data object from the session data
        /// </summary>
        /// <param name="context">HttpContext to use</param>
        /// <returns>Session store data for this session item</returns>
        public SessionStateStoreData ToStoreData(
            HttpContext context)
        {
            return new SessionStateStoreData(Data, SessionStateUtility.GetSessionStaticObjects(context), Timeout);
        }

        /// <summary>
        /// Removes a session store item from the bucket
        /// </summary>
        /// <param name="bucket">Bucket to remove from</param>
        /// <param name="id">Session ID</param>
        public static void Remove(
            IBucket bucket,
            string id)
        {
            bucket.Remove(CouchbaseSessionStateProvider.DataPrefix + id);
            bucket.Remove(CouchbaseSessionStateProvider.HeaderPrefix + id);
        }
    }
}