using System.Collections.Specialized;
using NUnit.Framework;

namespace Couchbase.AspNet.UnitTests
{
    [TestFixture]
    public class ProviderHelperTests
    {
        [Test]
        [Category("Integration")]
        public void GetCluster_CouchbaseSession_ReturnsCluster()
        {
            var cluster = ProviderHelper.GetCluster("couchbase-session", null);

            Assert.IsNotNull(cluster);
        }

        [Test]
        [Category("Integration")]
        public void GetBucket_CouchbaseSession_ReturnsBucket()
        {
            var config = new NameValueCollection {
                { "bucket", "transient1" }
            };
            var cluster = ProviderHelper.GetCluster("couchbase-session", null);
            var bucket = ProviderHelper.GetBucket("default", config, cluster);

            Assert.IsNotNull(bucket);
        }

        [Test]
        [Category("Integration")]
        public void GetBucket_CouchbaseSession_ReturnsMemcachedBucket()
        {
            var config = new NameValueCollection {
                { "bucket", "transient1" }
            };
            var cluster = ProviderHelper.GetCluster("couchbase-session", null);
            var bucket = ProviderHelper.GetBucket("default", config, cluster);

            Assert.AreEqual(typeof(MemcachedBucket), bucket.GetType());
        }

        [Test]
        [Category("Integration")]
        public void GetCluster_CouchbaseCache_ReturnsCluster()
        {
            var cluster = ProviderHelper.GetCluster("couchbase-cache", null);

            Assert.IsNotNull(cluster);
        }

        [Test]
        [Category("Integration")]
        public void GetBucket_CouchbaseCache_ReturnsBucket()
        {
            var config = new NameValueCollection {
                { "bucket", "data1" }
            };
            var cluster = ProviderHelper.GetCluster("couchbase-cache", null);
            var bucket = ProviderHelper.GetBucket("default", config, cluster);

            Assert.IsNotNull(bucket);
        }

        [Test]
        [Category("Integration")]
        public void GetBucket_CouchbaseCache_ReturnsMemcachedBucket()
        {
            var config = new NameValueCollection {
                { "bucket", "data1" }
            };
            var cluster = ProviderHelper.GetCluster("couchbase-cache", null);
            var bucket = ProviderHelper.GetBucket("default", config, cluster);

            Assert.AreEqual(typeof(MemcachedBucket), bucket.GetType());
        }
    }
}
