using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace EasyDB
{
    public class BaseDBTests
    {
        public EasyDB DB;
        public EasyDBSettings Settings;
        public string StorageRoot;

        [SetUp]
        public void Setup () {
            StorageRoot = Path.Combine(Path.GetTempPath(), "easydb_test_" + DateTime.UtcNow.Ticks);
            Assert.IsFalse(Directory.Exists(StorageRoot));

            Settings = new EasyDBSettings(StorageRoot, ".data");
            DB = new EasyDB(Settings);

            Assert.IsTrue(Directory.Exists(StorageRoot));
        }

        [TearDown]
        public void TearDown () {
            Assert.IsTrue(Directory.Exists(StorageRoot));

            DB.DestroyDatabase();
            Assert.IsFalse(Directory.Exists(StorageRoot));
        }
    }

    public class EasyDBTests : BaseDBTests
    {
        [Test]
        public void TestFileMapping () {
            // test root directory
            Test("baz", "", "baz");
            // test path containing ascii filenames that got kept verbatim
            Test("foo/bar/baz", "foo\\bar", "foo\\bar\\baz");
            // test that valid non-ascii filenames got encoded
            Test("gołąb/鳩", "go%C5%82%C4%85b", "go%C5%82%C4%85b\\%E9%B3%A9");
            // test that invalid ascii characters got encoded
            Test("foo(>!<)/hello/a+b=%\\",
                "foo%28%3E%21%3C%29\\hello",
                "foo%28%3E%21%3C%29\\hello\\a%2Bb%3D%25%5C");

            void Test (Key tk, string dirpath, string filepath) {
                var reldir = tk.MakeKeyPath().MakeRelativePath();
                var relfile = tk.MakeRelativePath();
                Assert.AreEqual(dirpath, reldir);
                Assert.AreEqual(filepath, relfile);
            }
        }

        [Test]
        public void TestCreateReadWrite () {
            // test creating, reading, and deleting a document

            var key = "a/b/foo";

            Assert.IsFalse(DB.Contains(key));
            Assert.IsFalse(DB.Get(key).value.IsValid);
            bool removed = DB.Remove(key);
            Assert.IsFalse(removed);

            Document doc = "aaaaa";
            DB.Set(key, doc);

            Assert.IsTrue(DB.Contains(key));
            Assert.IsTrue(DB.Get(key).value.IsValid);
            Assert.AreEqual(doc, DB.Get(key).value);

            removed = DB.Remove(key);

            Assert.IsTrue(removed);
            Assert.IsFalse(DB.Contains(key));
            Assert.IsFalse(DB.Get(key).value.IsValid);

            removed = DB.Remove(key);
            Assert.IsFalse(removed);
        }

        [Test]
        public void TestSetAndGet () {
            // test retrieving docs from valid and invalid keys

            // add a new key
            var table = nameof(TestSetAndGet);
            Key key = $"{table}/a";
            Key missing = $"{table}/b";
            Document value = "aaaaa";

            DB.Set(key, value);

            var result = DB.Get(key);
            Assert.IsTrue(result.etag.IsValid);
            Assert.IsTrue(result.value.IsValid);
            Assert.AreEqual(value, result.value);

            result = DB.Get(missing);
            Assert.IsFalse(result.etag.IsValid);
            Assert.IsFalse(result.value.IsValid);
        }

        [Test]
        public void TestCheckAndSet () {
            // test the check-and-set functionality using etags

            // add a new key
            var table = nameof(TestCheckAndSet);

            Key key = $"{table}/a";
            Document oldval = "aaaaa";
            Document newval = "bbbbb";

            var oldetag = DB.Set(key, oldval);

            Assert.AreEqual(oldval, DB.Get(key).value);
            Assert.AreEqual(oldetag, DB.Get(key).etag);

            // check a failed etag
            var brokenEtag = new Etag(DateTime.Now.AddSeconds(1));
            var (result, resultEtag) = DB.CheckAndSet(key, "broken", brokenEtag);
            Assert.IsFalse(result);
            Assert.IsFalse(resultEtag.IsValid);
            Assert.AreEqual(oldval, DB.Get(key).value);
            Assert.AreEqual(oldetag, DB.Get(key).etag);

            // check a successful etag
            var (newresult, newetag) = DB.CheckAndSet(key, newval, oldetag);
            Assert.IsTrue(newresult);
            Assert.AreEqual(newval, DB.Get(key).value);
            Assert.AreEqual(newetag, DB.Get(key).etag);
            Assert.AreNotEqual(oldetag, newetag);
        }

        [Test]
        public void TestSetAndContains () {
            // check contains is working before/after set

            // add a new key
            var table = nameof(TestSetAndContains);
            Key key = $"{table}/a";
            Key missing = $"{table}/b";

            DB.Set(key, "aaaaaa");

            var exists = DB.Contains(key);
            Assert.IsTrue(exists);

            exists = DB.Contains(missing);
            Assert.IsFalse(exists);

            DB.Remove(key);

            exists = DB.Contains(key);
            Assert.IsFalse(exists);
        }

        [Test]
        public void TestRemoveAndContains () {
            // check contains is working before/after removal

            string table = nameof(TestRemoveAndContains);

            Key key = $"{table}/a";
            DB.Set(key, "aaaaaa");

            var exists = DB.Contains(key);
            Assert.IsTrue(exists);

            var removed = DB.Remove(key);
            Assert.IsTrue(removed);

            exists = DB.Contains(key);
            Assert.IsFalse(exists);
        }

        [Test]
        public void TestEnumerate () {
            // test enumerating keys at specific points in the hierarchy as well as recursive

            var keys = new string[] {
                "a/b/1",
                "a/b/2",
                "a/b/x/y/1",
                "a/b/x/y/2"
            }.Select(item => new Key(item)).ToArray();

            foreach (var key in keys) { DB.Set(key, key.Raw); }

            // nonexistant keys (non-recursive)
            Assert.AreEqual(0, DB.Enumerate("", false).ToList().Count);
            Assert.AreEqual(0, DB.Enumerate("does/not/exist", false).ToList().Count);

            // raw enumerate at that specific point in hierarchy
            Assert.AreEqual(0, DB.Enumerate("a", false).ToList().Count);
            Assert.AreEqual(2, DB.Enumerate("a/b", false).ToList().Count);
            Assert.AreEqual(0, DB.Enumerate("a/b/x", false).ToList().Count);
            Assert.AreEqual(2, DB.Enumerate("a/b/x/y", false).ToList().Count);

            // recursive enumerate at that point and everywhere below
            Assert.AreEqual(2, DB.Enumerate("a/b/x/y", true).ToList().Count);
            Assert.AreEqual(2, DB.Enumerate("a/b/x", true).ToList().Count);
            Assert.AreEqual(4, DB.Enumerate("a/b", true).ToList().Count);
            Assert.AreEqual(4, DB.Enumerate("a", true).ToList().Count);

            // root enum, recursive, should return everything
            Assert.AreEqual(4, DB.Enumerate("", true).ToList().Count);

            // add a root item
            DB.Set(new Key("foo"), "foo");
            Assert.AreEqual(1, DB.Enumerate("", false).ToList().Count);
            Assert.AreEqual(5, DB.Enumerate("", true).ToList().Count);

            // make sure it's as expected
            Assert.AreEqual("foo", DB.Enumerate("", false).FirstOrDefault().Raw);

            // delete all, one by one
            foreach (var key in keys) { DB.Remove(key); }
            DB.Remove("foo");

            Assert.AreEqual(0, DB.Enumerate("", true).ToList().Count);
        }

        [Test]
        public void TestEnumerateValues () {
            // test enumerating documents 

            var keys = new string[] {
                "a/b/1",
                "a/b/2",
                "a/b/x/y/1",
                "a/b/x/y/2"
            }.Select(item => new Key(item)).ToArray();

            foreach (var key in keys) { DB.Set(key, key.Raw); }

            // pull all keys into a dictionary
            var allkeys = new Dictionary<string, Key>();
            foreach (var key in DB.Enumerate("", true)) {
                allkeys[key.Raw] = key;
            }

            // put all values in another dictionary
            var allvals = new Dictionary<string, (Key, string)>();
            foreach (var (key, _, value) in DB.EnumerateValues("", true)) {
                allvals[key.Raw] = (key, value);
            }

            Assert.AreEqual(keys.Length, allkeys.Count);
            Assert.AreEqual(keys.Length, allvals.Count);

            foreach (var key in allkeys.Values) {
                var val = allvals[key.Raw];
                Assert.AreEqual(key.Raw, val.Item2);
            }
        }

        [Test]
        public void TestEquivalenceOfStringsAndBytes () {
            // test helpers for serializing/deserializing strings

            var key = $"{nameof(TestEquivalenceOfStringsAndBytes)}/foo";
            var valstring = key;
            var valbytes = Document.StringToBytes(valstring);
            var valstr2 = Document.BytesToString(valbytes);

            DB.Set(key, valstring);

            var resultstr = DB.Get(key).value.AsString;
            Assert.AreEqual(valstr2, resultstr);

            var resultbytes = DB.Get(key).value.Bytes;
            Assert.IsTrue(Document.BytewiseEqual(resultbytes, valbytes));
        }

        [Test]
        public void TestRemoveCleaningUpDirectories () {
            // test directory removal when all keys in a hierarchy get removed

            var key1 = "foo/aaa/123"; DB.Set(key1, key1);
            var key2 = "foo/bbb/456"; DB.Set(key2, key2);

            var rootdir = new DirectoryInfo(DB.Settings.StorageRoot);

            Assert.IsTrue(DB.Contains(key1));
            Assert.IsTrue(DB.Contains(key2));
            Assert.AreEqual(1, rootdir.GetDirectories().Length);

            DB.Remove(key1);
            Assert.AreEqual(1, rootdir.GetDirectories().Length);

            DB.Remove(key2);
            Assert.AreEqual(0, rootdir.GetDirectories().Length);
        }

        [Test]
        public void TestSetNullThrowsException () {
            try {
                DB.Set(null, "null key");
                Assert.Fail();
            } catch (Exception) { }

            try {
                DB.Set("null bytes value", default);
                Assert.Fail();
            } catch (Exception) { }
        }

        [Test]
        public void TestEnumerateInvalidPath () {
            try {
                var _ = DB.Enumerate("/");
                Assert.Fail();
            } catch (Exception) { }

            try {
                var _ = DB.Enumerate("foo/");
                Assert.Fail();
            } catch (Exception) { }

            try {
                var _ = DB.Enumerate("/foo");
                Assert.Fail();
            } catch (Exception) { }
        }
    }

    public class DocumentTests
    {
        [Test]
        public void TestEquality () {
            var a = new Document("abc");
            var b = new Document(new byte[] { 97, 98, 99 });

            Assert.AreEqual((string)a, (string)b);
            Assert.AreEqual((byte[])a, (byte[])b);
            Assert.AreEqual(a, b);
        }

        [Test]
        public void TestConstructorExceptions () {
            try {
                var key = new Document((byte[])null);
                Assert.Fail();
            } catch (Exception) { }

            try {
                var key = new Document((string)null);
                Assert.Fail();
            } catch (Exception) { }
        }
    }

    public class KeyPathTests : BaseDBTests
    {
        [Test]
        public void TestPathSegments () {
            var path = new KeyPath("");
            var segments = path.MakePathSegments();
            Assert.AreEqual(0, segments.Length);

            path = new KeyPath("foo");
            segments = path.MakePathSegments();
            Assert.AreEqual(1, segments.Length);
            Assert.AreEqual("foo", segments[0]);

            path = new KeyPath("foo/bar/baz");
            segments = path.MakePathSegments();
            Assert.AreEqual(3, segments.Length);
            Assert.AreEqual("foo", segments[0]);
            Assert.AreEqual("bar", segments[1]);
            Assert.AreEqual("baz", segments[2]);
        }

        [Test]
        public void TestRelativePath () {
            // good old ascii
            Test("", "");
            Test("foo", "foo");
            Test("foo/bar", "foo\\bar");
            Test("foo/bar/baz", "foo\\bar\\baz");

            // test non-ascii ones
            Test("gołąb/鳩", "go%C5%82%C4%85b\\%E9%B3%A9");

            void Test (string raw, string exp) {
                var path = new KeyPath(raw);
                var rel = path.MakeRelativePath();
                Assert.AreEqual(exp, rel);
            }
        }

        [Test]
        public void TestAbsolutePath () {
            Test("", StorageRoot);
            Test("foo", StorageRoot + "\\foo");
            Test("foo/bar/baz", StorageRoot + "\\foo\\bar\\baz");

            void Test (string raw, string exp) {
                var path = new KeyPath(raw);
                var rel = path.MakeAbsolutePath(Settings);
                Assert.AreEqual(exp, rel);
            }
        }

        [Test]
        public void TestConstructorExceptions () {
            try {
                var key = new KeyPath(null);
                Assert.Fail();
            } catch (Exception) { }

            try {
                var key = new KeyPath("/");
                Assert.Fail();
            } catch (Exception) { }

            try {
                var key = new KeyPath("/foo");
                Assert.Fail();
            } catch (Exception) { }

            try {
                var key = new KeyPath("foo/");
                Assert.Fail();
            } catch (Exception) { }

            try {
                var key = new KeyPath("foo//bar");
                Assert.Fail();
            } catch (Exception) { }
        }
    }

    public class KeyTests : BaseDBTests
    {
        [Test]
        public void TestRelativePath () {
            // good old ascii
            Test("foo", "foo");
            Test("foo/bar", "foo\\bar");
            Test("foo/bar/baz", "foo\\bar\\baz");

            // test non-ascii ones
            Test("gołąb/鳩", "go%C5%82%C4%85b\\%E9%B3%A9");

            void Test (string raw, string exp) {
                var key = new Key(raw);
                var rel = key.MakeRelativePath();
                Assert.AreEqual(exp, rel);
            }
        }

        [Test]
        public void TestAbsolutePath () {
            Test("foo", StorageRoot + "\\foo" + Settings.DataFileExtension);
            Test("foo/bar/baz", StorageRoot + "\\foo\\bar\\baz" + Settings.DataFileExtension);

            void Test (string raw, string exp) {
                var key = new Key(raw);
                var rel = key.MakeAbsolutePath(Settings);
                Assert.AreEqual(exp, rel);
            }
        }

        [Test]
        public void TestConstructorExceptions () {
            try {
                var key = new Key(null);
                Assert.Fail();
            } catch (Exception) { }

            try {
                var key = new Key("");
                Assert.Fail();
            } catch (Exception) { }

            try {
                var key = new Key("/");
                Assert.Fail();
            } catch (Exception) { }

            try {
                var key = new Key("/foo");
                Assert.Fail();
            } catch (Exception) { }

            try {
                var key = new Key("foo/");
                Assert.Fail();
            } catch (Exception) { }

            try {
                var key = new Key("foo//bar");
                Assert.Fail();
            } catch (Exception) { }
        }
    }

    public class EncodingUtilsTests
    {
        [Test]
        public void TestEncodeDecode () {
            // valid chars
            Test("a-z_A-Z_0-9");

            // valid url chars that get encoded here
            Test("az+.!*()", "az%2B%2E%21%2A%28%29");

            // chars that are never safe
            Test("鳩/\\#żź", "%E9%B3%A9%2F%5C%23%C5%BC%C5%BA");

            // finally let's make sure encoding isn't case sensitive
            var decoded = EncodingUtils.UrlDecode("az%2b%2e%2B%2E");
            Assert.AreEqual("az+.+.", decoded);


            void Test (string value, string encoded = null) {
                // test straight up string functions first

                var en = EncodingUtils.UrlEncode(value);
                if (encoded != null) {
                    Assert.IsTrue(string.Equals(encoded, en, StringComparison.InvariantCulture));
                }

                var de = EncodingUtils.UrlDecode(en);
                Assert.IsTrue(string.Equals(value, de, StringComparison.InvariantCulture));

                // now test the binary interface as well

                var valbytes = Encoding.UTF8.GetBytes(value);
                var enbytes = EncodingUtils.UrlEncode(valbytes);

                if (encoded != null) {
                    var encodedbytes = Encoding.UTF8.GetBytes(encoded);
                    Assert.IsTrue(ContentEquals(enbytes, encodedbytes));
                }

                var debytes = EncodingUtils.UrlDecode(enbytes);
                Assert.IsTrue(ContentEquals(valbytes, debytes));
            }

            bool ContentEquals (byte[] a, byte[] b) {
                if (a == null || b == null || a.Length != b.Length) { return false; }
                for (int i = 0, count = a.Length; i < count; i++) {
                    if (a[i] != b[i]) { return false; }
                }
                return true;
            }
        }
    }

}