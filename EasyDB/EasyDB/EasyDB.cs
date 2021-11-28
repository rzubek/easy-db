using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace EasyDB
{
    /// <summary>
    /// Database settings
    /// </summary>
    public class EasyDBSettings
    {
        /// <summary>
        /// Path to the root directory where all files will be stored.
        /// </summary>
        public readonly string StorageRoot = "";

        /// <summary>
        /// Filename extension for data files corresponding to each value,
        /// including any separators, e.g. ".txt". May be empty, but may not be null.
        /// </summary>
        public readonly string DataFileExtension = "";

        /// <summary>
        /// Constructor. Storage root is required, others are optional.
        /// </summary>
        public EasyDBSettings (string storageRoot, string dataFileExtension = "") {
            StorageRoot = storageRoot;
            DataFileExtension = dataFileExtension;
        }
    }

    /// <summary>
    /// EasyDB is a thin key/value API on top of filesystem storage. 
    ///
    /// Client code can query and set entries as if they were documents with keys;
    /// these are in turn translated directly into filesystem accesses,
    /// where each key is a path and each value is a file.
    ///
    /// File naming: key names are used verbatim as directory and file names,
    /// unless they contain invalid characters, in which case those are url encoded.
    /// Please note this wrapper is unaware of case insensitivity -
    /// since names are used verbatim, users on Windows and other case-insensitive
    /// filesystems should avoid using keys that differ only in case.
    ///
    /// Threading model: the store is safe for multithreaded use from a single process.
    /// Filesystem access happens directly on the calling thread.
    /// All filesystem access is guarded by a reader/writer lock, so that all read operations
    /// (get, contains, enumerate) can occur simultaneously and without blocking on each other,
    /// while all write operations (set, check and set, remove) will wait for any read operations
    /// to finish, and will be mutually exclusive from each other and from any kind of a read operation.
    ///
    /// </summary>
    public class EasyDB
    {
        /// <summary>
        /// Global lock used to make sure mutable operations (set, check and set, remove)
        /// are mutually exclusive from each other and any retrieval operations (get, contains, enumerate)
        /// </summary>
        private readonly ReaderWriterLockSlim GlobalLock;

        /// <summary>
        /// Database settings.
        /// </summary>
        public readonly EasyDBSettings Settings;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="storageRoot">Root directory where all files will be persisted</param>
        public EasyDB (EasyDBSettings settings) {
            Settings = settings;
            GlobalLock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

            if (!Directory.Exists(Settings.StorageRoot)) { Directory.CreateDirectory(Settings.StorageRoot); }
        }

        //
        // mutex helpers

        private T WithReadLock<T> (Func<T> fn) {
            GlobalLock.EnterReadLock();
            try {
                return fn();
            } finally {
                GlobalLock.ExitReadLock();
            }
        }

        private T WithWriteLock<T> (Func<T> fn) {
            GlobalLock.EnterWriteLock();
            try {
                return fn();
            } finally {
                GlobalLock.ExitWriteLock();
            }
        }

        //
        // public api

        /// <summary>
        /// Returns true if the key exists in storage, false if missing.
        /// Throws an exception if filesystem access failed.
        /// </summary>
        public bool Contains (Key key) =>
            WithReadLock(() => {
                var file = MakeFileInfo(key);
                return file.Exists;
            });

        /// <summary>
        /// Retrieves value at the given table/key, and returns it with its etag if found,
        /// or an invalid document and default etag if not found.
        /// Throws an exception if filesystem access failed.
        /// </summary>
        public (Document value, Etag etag) Get (Key key) =>
            WithReadLock(() => {
                var file = MakeFileInfo(key);
                if (!file.Exists) { return (default, default); }

                var value = File.ReadAllBytes(file.FullName);
                var etag = Etag.FromLastFileWrite(file);
                return (new Document(value), etag);
            });


        /// <summary>
        /// Saves the key/value tuple using a blind write (i.e. creates a new entry,
        /// or blindly overwrites existing entry, without providing feedback),
        /// and returns a new etag for the newly saved version of the document.
        /// Throws an exception if filesystem access failed.
        /// </summary>
        public Etag Set (Key key, Document value) =>
            WithWriteLock(() => {
                if (value == null) { throw new ArgumentException("Invalid null value", nameof(value)); }

                var dir = EnsureDirectoryExists(key);
                if (dir == null) { throw new InvalidOperationException($"Failed to create directory for key {key}"); }

                var file = MakeFileInfo(key);
                File.WriteAllBytes(file.FullName, value.Bytes);

                return Etag.FromLastFileWrite(file);
            });

        /// <summary>
        /// If the current document has a matching etag, it overwrites the document
        /// and returns true and a valid new etag; otherwise does not overwrite
        /// and returns false an an invalid etag.
        /// Throws an exception if file access failed.
        /// </summary>
        public (bool success, Etag etag) CheckAndSet (Key key, Document value, Etag etag) =>
            WithWriteLock(() => {
                if (value == null) { throw new ArgumentException("Invalid null value", nameof(value)); }

                // check etag first
                var file = MakeFileInfo(key);
                if (!file.Exists) { return (false, default); }

                var currentEtag = Etag.FromLastFileWrite(file);
                if (!Etag.Equals(currentEtag, etag)) { return (false, default); }

                // now set as needed
                File.WriteAllText(file.FullName, value);

                return (true, Etag.FromLastFileWrite(file));
            });


        /// <summary>
        /// Attempts to remove the given key/value from storage, and returns true if successful, or false if not found.
        /// Throws exception if file access failed.
        /// </summary>
        public bool Remove (Key key) {
            return WithWriteLock(() => {
                var file = MakeFileInfo(key);
                if (!file.Exists) { return false; }

                file.Delete();
                DeleteDirectoriesIfEmpty();
                return true;
            });

            void DeleteDirectoriesIfEmpty () {
                var segments = key.MakeKeyPath().MakePathSegments();
                if (segments.Length == 0) { return; } // already at root

                DeleteDirectoriesHelper(segments.ToArray());
            }

            void DeleteDirectoriesHelper (string[] segments) {
                var dir = TryMakeDirectoryFromSegments(segments);
                if (dir == null) { return; }

                // if there's anything at all in there, bail
                if (dir.GetFiles().Length != 0) { return; }
                if (dir.GetDirectories().Length != 0) { return; }

                // delete the directory and see if we can delete the parent
                dir.Delete();

                if (segments.Length > 1) {
                    var parents = segments.Take(segments.Length - 1).ToArray();
                    DeleteDirectoriesHelper(parents);
                }
            }
        }


        /// <summary>
        /// Given a key path, enumerates all keys under that key hierarchy, optionally recursively.
        /// For example, a query with key path of the form "a/b" might return keys like
        /// "a/b/entry1" and "a/b/entry2" and, if recursion is enabled, "a/b/x/y/1" or "a/b/x/y/2", etc.;
        /// however it will not return keys like "a/c" or "a/d/e/f".
        ///
        /// Note that the path must not start or end with "/".
        /// To enumerate all keys starting from the root hierarchy, use the "" path (empty string).
        /// 
        /// If the path is valid but there are no key/value pairs corresponding to it,
        /// this operation will return an empty enumeration.
        ///
        /// Result ordering is not guaranteed.
        /// </summary>
        public IEnumerable<Key> Enumerate (KeyPath path, bool recursive = true) {
            return WithReadLock(() => {
                var dir = MakeDirectoryInfo(path);
                var all = EnumerateAllFiles(dir);
                return all.Select(file => MakeKeyFromFile(file));
            });

            IEnumerable<FileInfo> EnumerateAllFiles (DirectoryInfo dir) {
                if (!dir.Exists) { yield break; }

                foreach (var file in dir.EnumerateFiles()) { yield return file; }

                if (recursive) {
                    foreach (var subdir in dir.EnumerateDirectories()) {
                        var subfiles = EnumerateAllFiles(subdir);
                        foreach (var file in subfiles) { yield return file; }
                    }
                }
            }
        }

        /// <summary>
        /// Given a key path, enumerates all entries under that key hierarchy, optionally recursively.
        /// This is similar to Enumerate(), but additionally it reads all documents and their contents.
        /// 
        /// Note that the path must not start or end with "/".
        /// To enumerate all keys starting from the root hierarchy, use the "" path (empty string).
        /// 
        /// If the path is valid but there are no key/value pairs corresponding to it,
        /// this operation will return an empty enumeration.
        ///
        /// Result ordering is not guaranteed.
        /// </summary>
        public IEnumerable<(Key key, Etag etag, Document value)> EnumerateValues (KeyPath path, bool recursive = true) {
            (Key key, Etag etag, Document value) Getter (Key key) {
                var (value, etag) = Get(key);
                return (key, etag, value);
            }

            var keys = Enumerate(path, recursive).ToList();
            return WithReadLock(() => keys.Select(Getter));
        }


        /// <summary>
        /// Unconditionally and immediately removes all data associated with all tables.
        /// </summary>
        public bool DestroyDatabase () =>
            WithWriteLock(() => {
                if (Directory.Exists(Settings.StorageRoot)) {
                    Directory.Delete(Settings.StorageRoot, true);
                    return true;
                } else {
                    return false;
                }
            });


        //
        // filesystem access helpers

        private FileInfo MakeFileInfo (Key key) =>
            new FileInfo(key.MakeAbsolutePath(Settings));

        private DirectoryInfo MakeDirectoryInfo (Key key) =>
            new DirectoryInfo(key.MakeKeyPath().MakeAbsolutePath(Settings));

        private DirectoryInfo MakeDirectoryInfo (KeyPath keypath) =>
            new DirectoryInfo(keypath.MakeAbsolutePath(Settings));

        private DirectoryInfo EnsureDirectoryExists (Key key) =>
            Directory.CreateDirectory(MakeDirectoryInfo(key).FullName);

        private DirectoryInfo TryMakeDirectoryFromSegments (string[] segments) {
            if (segments == null || segments.Length == 0) { return null; }

            var path = Path.Combine(Settings.StorageRoot, Path.Combine(segments));
            return new DirectoryInfo(path);
        }

        private Key MakeKeyFromFile (FileInfo file) {
            // find path relative to storage root
            var rootPath = new DirectoryInfo(Settings.StorageRoot).FullName;
            var relativePath = file.FullName.Replace(rootPath, "");
            if (relativePath.Length == 0) { return default; }

            // above replacement typically leaves a separator at the start - remove that too
            if (relativePath[0] == Path.DirectorySeparatorChar) {
                relativePath = relativePath.Substring(1);
            }

            // replace os-specific separators
            var keyPath = relativePath.Replace(Path.DirectorySeparatorChar, KeyPath.Separator);

            // remove file extension
            var key = keyPath.Substring(0, keyPath.Length - Settings.DataFileExtension.Length);

            return new Key(key);
        }
    }

    /// <summary>
    /// This struct holds on to the document body as a byte array,
    /// and provides helper functions for converting from/to UTF-8 strings.
    /// </summary>
    [DebuggerDisplay("Document, length = {Bytes.Length} bytes, text = {AsString}")]
    public struct Document
    {
        /// <summary> This document's contents as a byte array </summary>
        public readonly byte[] Bytes;

        /// <summary> Constructor from a raw byte array </summary>
        public Document (byte[] bytes) {
            if (bytes == null) { throw new ArgumentNullException(nameof(bytes)); }
            Bytes = new byte[bytes.Length];
            Array.Copy(bytes, Bytes, Bytes.Length);
        }

        /// <summary> Constructor from a unicode string </summary>
        public Document (string str) : this(StringToBytes(str)) { }

        public static implicit operator Document (string val) => new Document(val);
        public static implicit operator Document (byte[] val) => new Document(val);
        public static implicit operator string (Document doc) => doc.AsString;
        public static implicit operator byte[] (Document doc) => doc.Bytes;

        public bool IsValid => Bytes != null;

        /// <summary>
        /// Treats document bytes as UTF-8 and converts them to a unicode string.
        /// </summary>
        public string AsString => BytesToString(Bytes);

        /// <summary> Performs document string to bytes conversion </summary>
        public static byte[] StringToBytes (string value) => Encoding.UTF8.GetBytes(value);

        /// <summary> Performs document bytes to string conversion </summary>
        public static string BytesToString (byte[] value) => Encoding.UTF8.GetString(value);

        /// <summary> Returns true if two arrays are byte-wise equivalent </summary>
        public static bool BytewiseEqual (byte[] abytes, byte[] bbytes) {
            if (abytes == bbytes) { return true; } // short-circuit if they're both the same refs, or null

            if (abytes == null || bbytes == null) { return false; }
            if (abytes.Length != bbytes.Length) { return false; }
            for (int i = 0, count = abytes.Length; i < count; i++) {
                if (abytes[i] != bbytes[i]) { return false; }
            }

            return true;
        }


        public static bool Equals (Document a, Document b) => BytewiseEqual(a.Bytes, b.Bytes);
        public bool Equals (Document other) => Equals(this, other);

        public override bool Equals (object obj) => obj is Document other && Equals(this, other);
        public override int GetHashCode () {
            int sum = 0;
            foreach (var b in Bytes) { sum += b; }
            return sum;
        }
    }

    /// <summary>
    /// This struct holds hierarchy path for some key or set of keys.
    /// Paths do not include the initial or terminal '/' slash separator character.
    /// Keys like "a" or "foo" have no position in the hierarchy, so their path is "".
    /// Key like "a/b" will have the path "a", while key "a/b/c" will have the path "a\\b".
    /// Each unique path corresponds to a directory on the filesystem.
    /// </summary>
    [DebuggerDisplay("KeyPath: {Raw}")]
    public struct KeyPath : IEquatable<KeyPath>
    {
        public static readonly char Separator = '/';
        public static readonly string SeparatorString = "/";

        internal static readonly string InvalidSeparatorSequence = SeparatorString + SeparatorString;

        /// <summary> Raw path provided by the user, including final slash </summary>
        public readonly string Raw;

        public KeyPath (string raw) {
            if (raw == null) {
                throw new ArgumentNullException(nameof(raw));
            }

            if (raw.Contains(InvalidSeparatorSequence)) {
                throw new ArgumentException("Path must not contain a double slash", nameof(raw));
            }

            if (raw.StartsWith(SeparatorString)) {
                throw new ArgumentException("Path must not start with a slash", nameof(raw));
            }

            if (raw.EndsWith(SeparatorString)) {
                throw new ArgumentException("Path must not end with a slash", nameof(raw));
            }

            Raw = raw;
        }

        public static implicit operator KeyPath (string raw) => new KeyPath(raw);
        public static implicit operator string (KeyPath path) => path.Raw;

        /// <summary>
        /// Converts this path into a series of filesystem path segments.
        /// </summary>
        public string[] MakePathSegments () =>
            string.IsNullOrEmpty(Raw) ?
                Array.Empty<string>() :
                Raw.Split(Separator).Select(EncodingUtils.UrlEncode).ToArray();

        /// <summary>
        /// Relative file path to the location on the filesystem where this path's keys are stored.
        /// For a path like "foo/bar/" the result would be the directory "foo\bar\".
        /// Path segments that are not filesystem safe will be url-encoded.
        /// </summary>
        public string MakeRelativePath () =>
            Path.Combine(MakePathSegments());

        /// <summary>
        /// File path to the location on the filesystem where this path's keys are stored.
        /// For a path like "foo/bar/" for a database stored on "c:\data" the result would be the file "c:\data\foo\bar\".
        /// Path segments that are not filesystem safe will be url-encoded.
        /// </summary>
        public string MakeAbsolutePath (EasyDBSettings settings) =>
            Path.Combine(settings.StorageRoot, Path.Combine(MakePathSegments()));

        // helper function for keys

        internal static KeyPath MakeFromKey (string raw) {
            // get everything up to but excluding the last slash
            var index = raw.LastIndexOf(Separator);
            var path = index <= 0 ? "" : raw.Substring(0, index);
            return new KeyPath(path);
        }

        public static bool Equals (KeyPath a, KeyPath b) => a.Raw == b.Raw;
        public bool Equals (KeyPath other) => Equals(this, other);

        public override bool Equals (object obj) => obj is KeyPath other && Equals(this, other);
        public override int GetHashCode () => Raw?.GetHashCode() ?? 0;
    }

    /// <summary>
    /// This struct represents the string key for an item in the database.
    /// Keys can be hierarchical, for example keys like "a" or "foo" are in
    /// the root position, while keys like "a/b/c" and "a/b/d" share the
    /// same path "a/b" in the hierarchy, which can help with enumerating
    /// keys at the same location.
    /// 
    /// Each key corresponds to a file on the filesystem, and its location
    /// is determined by the key path.
    /// 
    /// Keys can contain the hierarchical path separator '/' but must not
    /// begin or end with that character, and they must not be null.
    /// </summary>
    [DebuggerDisplay("Key: {Raw}")]
    public struct Key : IEquatable<Key>
    {
        /// <summary> Key provided by the user </summary>
        public readonly string Raw;

        /// <summary>
        /// Constructor. Takes raw key string, which can be a simple string like "name" or a
        /// hierarchical key like "category/subcategory/item" etc.
        /// </summary>
        public Key (string raw) {
            if (raw == null) {
                throw new ArgumentNullException(nameof(raw));
            }

            if (string.IsNullOrWhiteSpace(raw)) {
                throw new ArgumentException("Invalid argument", nameof(raw));
            }

            if (raw.EndsWith(KeyPath.SeparatorString)) {
                throw new ArgumentException("Key must not end with a slash", nameof(raw));
            }

            if (raw.StartsWith(KeyPath.SeparatorString)) {
                throw new ArgumentException("Key must not start with a slash", nameof(raw));
            }

            if (raw.Contains(KeyPath.InvalidSeparatorSequence)) {
                throw new ArgumentException("Path must not contain a double slash", nameof(raw));
            }


            Raw = raw;
        }

        public static implicit operator Key (string raw) => new Key(raw);
        public static implicit operator string (Key key) => key.Raw;

        /// <summary> Makes a path data structure for this key </summary>
        public KeyPath MakeKeyPath () => KeyPath.MakeFromKey(Raw);

        /// <summary> Extracts the last, non-hierarchical element of the key.
        /// For example, the last element of "foo" is "foo", while the last
        /// element of "a/b/c" is "c". </summary>
        public string ExtractLastElement () {
            var index = Raw.LastIndexOf(KeyPath.Separator);
            return index < 0 ? Raw : Raw.Substring(index + 1);
        }

        /// <summary> Makes filesystem name for this key. This is the last segment of the path.
        /// For a key like "foo/bar/baz" the filesystem name would be "baz".
        /// This does not include file extension. </summary>
        public string MakeFilesystemFilename () =>
            EncodingUtils.UrlEncode(ExtractLastElement());

        /// <summary>
        /// Relative path to the location on the filesystem where this key is stored.
        /// For a key like "foo/bar/baz" the result would be the relative file path
        /// "foo\bar\baz". For a key like "a", the result would be the file "a".
        /// Path segments that are not filesystem safe will be url-encoded.
        /// </summary>
        public string MakeRelativePath () =>
            Path.Combine(MakeKeyPath().MakeRelativePath(), MakeFilesystemFilename());

        /// <summary>
        /// File path to the location on the filesystem where this key is stored.
        /// For a key like "foo/bar/baz" and a database stored on "c:\data"
        /// the result would be the file "c:\data\foo\bar\baz.txt". For a key
        /// like "a", the result would be the file "c:\data\a.txt".
        /// Path segments that are not filesystem safe will be url-encoded.
        /// </summary>
        public string MakeAbsolutePath (EasyDBSettings settings) =>
            Path.Combine(settings.StorageRoot, MakeRelativePath() + settings.DataFileExtension);


        public static bool Equals (Key a, Key b) => a.Raw == b.Raw;
        public bool Equals (Key other) => Equals(this, other);

        public override bool Equals (object obj) => obj is Key other && Equals(this, other);
        public override int GetHashCode () => Raw?.GetHashCode() ?? 0;
    }


    /// <summary>
    /// Etag token represents the last update time of some specific key,
    /// and can be used in check-and-set operations, to make sure that the value
    /// is only updated if they key was last updated at an expected time.
    /// </summary>
    public struct Etag : IEquatable<Etag>
    {
        public readonly DateTime Value;

        public Etag (DateTime value) {
            Value = value;
        }

        public bool IsValid => Value != default;

        public static bool Equals (Etag a, Etag b) => a.Value == b.Value;
        public bool Equals (Etag other) => Equals(this, other);

        public override bool Equals (object obj) => obj is Etag other && Equals(this, other);
        public override int GetHashCode () => Value.GetHashCode();

        public static bool operator == (Etag a, Etag b) => Equals(a, b);
        public static bool operator != (Etag a, Etag b) => !Equals(a, b);

        public static Etag FromLastFileWrite (FileInfo file) =>
            // generate a new fileinfo, because the old one might be stale
            new Etag(new FileInfo(file.FullName).LastWriteTimeUtc);
    }

    /// <summary>
    /// EncodingUtils provide a variant of URLEncode and URLDecode from .Net WebUtility class,
    /// but with a much more restricted class of allowed characters. Specifically, several characters
    /// deemed safe by traditional URL encoding, like `*`, are disallowed by this library,
    /// and will be encoded. The only safe characters exempt from encoding are ASCII letters
    /// (a-z, A-Z), digits (0-9), dash (-) and underscore (_), all others will be encoded. 
    /// </summary>
    public class EncodingUtils
    {
        // this implementation is based on WebUtility by Microsoft
        // https://github.com/microsoft/referencesource/blob/master/System/net/System/Net/WebUtility.cs

        public static string UrlEncode (string value) {
            if (value == null || value.Length == 0) { return value; }

            byte[] bytes = Encoding.UTF8.GetBytes(value);
            return Encoding.UTF8.GetString(UrlEncodeInternal(bytes));
        }

        public static byte[] UrlEncode (byte[] value) {
            if (value == null || value.Length == 0) { return value; }

            byte[] encoded = UrlEncodeInternal(value);
            return encoded == value ? (byte[])encoded.Clone() : encoded;
        }

        private static byte[] UrlEncodeInternal (byte[] bytes) {
            if (bytes == null) { throw new ArgumentNullException(nameof(bytes)); }

            int cSpaces = 0;
            int cUnsafe = 0;

            // count them first
            int count = bytes.Length;
            for (int i = 0; i < count; i++) {
                char ch = (char)bytes[i];

                if (ch == ' ') {
                    cSpaces++;
                } else if (!IsUrlSafeChar(ch)) {
                    cUnsafe++;
                }
            }

            // nothing to expand?
            if (cSpaces == 0 && cUnsafe == 0) {
                return bytes;
            }

            // expand not 'safe' characters into %XX, spaces to +s
            byte[] expandedBytes = new byte[count + cUnsafe * 2];
            int pos = 0;

            for (int i = 0; i < count; i++) {
                byte b = bytes[i];
                char ch = (char)b;

                if (IsUrlSafeChar(ch)) {
                    expandedBytes[pos++] = b;
                } else if (ch == ' ') {
                    expandedBytes[pos++] = (byte)'+';
                } else {
                    expandedBytes[pos++] = (byte)'%';
                    expandedBytes[pos++] = (byte)IntToHex((b >> 4) & 0xf);
                    expandedBytes[pos++] = (byte)IntToHex(b & 0x0f);
                }
            }

            return expandedBytes;
        }


        public static string UrlDecode (string value) =>
            value == null ? null : UrlDecodeInternal(value, Encoding.UTF8);

        public static byte[] UrlDecode (byte[] value) =>
            value == null ? null : UrlDecodeInternal(value);


        private static string UrlDecodeInternal (string value, Encoding encoding) {
            if (value == null) { throw new ArgumentNullException(nameof(value)); }

            int count = value.Length;
            UrlDecoder helper = new UrlDecoder(count, encoding);

            // go through the string's chars collapsing %XX and
            // appending each char as char, with exception of %XX constructs
            // that are appended as bytes

            for (int pos = 0; pos < count; pos++) {
                char ch = value[pos];

                if (ch == '+') {
                    ch = ' ';
                } else if (ch == '%' && pos < count - 2) {
                    int h1 = HexToInt(value[pos + 1]);
                    int h2 = HexToInt(value[pos + 2]);

                    if (h1 >= 0 && h2 >= 0) {     // valid 2 hex chars
                        byte b = (byte)((h1 << 4) | h2);
                        pos += 2;

                        // don't add as char
                        helper.AddByte(b);
                        continue;
                    }
                }

                if ((ch & 0xFF80) == 0) {
                    helper.AddByte((byte)ch); // 7 bit have to go as bytes because of Unicode
                } else {
                    helper.AddChar(ch);
                }
            }

            return helper.GetString();
        }

        private static byte[] UrlDecodeInternal (byte[] bytes) {
            if (bytes == null) { throw new ArgumentNullException(nameof(bytes)); }

            int decodedBytesCount = 0;
            int count = bytes.Length;
            byte[] decodedBytes = new byte[count];

            for (int i = 0; i < count; i++) {
                byte b = bytes[i];

                if (b == '+') {
                    b = (byte)' ';
                } else if (b == '%' && i < count - 2) {
                    int h1 = HexToInt((char)bytes[i + 1]);
                    int h2 = HexToInt((char)bytes[i + 2]);

                    if (h1 >= 0 && h2 >= 0) {     // valid 2 hex chars
                        b = (byte)((h1 << 4) | h2);
                        i += 2;
                    }
                }

                decodedBytes[decodedBytesCount++] = b;
            }

            if (decodedBytesCount < decodedBytes.Length) {
                byte[] newDecodedBytes = new byte[decodedBytesCount];
                Array.Copy(decodedBytes, newDecodedBytes, decodedBytesCount);
                decodedBytes = newDecodedBytes;
            }

            return decodedBytes;
        }

        private static int HexToInt (char h) =>
            (h >= '0' && h <= '9') ? h - '0' :
            (h >= 'a' && h <= 'f') ? h - 'a' + 10 :
            (h >= 'A' && h <= 'F') ? h - 'A' + 10 :
            -1;

        private static char IntToHex (int n) {
            Debug.Assert(n < 0x10);
            return n <= 9 ? (char)(n + '0') : (char)(n - 10 + 'A');
        }

        // Set of safe chars, far more restrictive than RFC 1738.4:
        // in this implementation it's just ASCII letters, numbers, dash, and underscore
        private static bool IsUrlSafeChar (char ch) =>
            (ch >= 'a' && ch <= 'z') ||
            (ch >= 'A' && ch <= 'Z') ||
            (ch >= '0' && ch <= '9') ||
            ch == '-' ||
            ch == '_';


        // Internal class to facilitate URL decoding -- keeps char buffer and byte buffer, allows appending of either chars or bytes
        private class UrlDecoder
        {
            private readonly int _bufferSize;

            // Accumulate characters in a special array
            private readonly char[] _charBuffer;
            private int _numChars;

            // Accumulate bytes for decoding into characters in a special array
            private int _numBytes;
            private byte[] _byteBuffer;

            // Encoding to convert chars to bytes
            private readonly Encoding _encoding;

            internal UrlDecoder (int bufferSize, Encoding encoding) {
                _bufferSize = bufferSize;
                _encoding = encoding;

                _charBuffer = new char[bufferSize];
                // byte buffer created on demand
            }

            internal void AddChar (char ch) {
                if (_numBytes > 0) { FlushBytes(); }

                _charBuffer[_numChars++] = ch;
            }

            internal void AddByte (byte b) {
                _byteBuffer = _byteBuffer ?? (new byte[_bufferSize]);
                _byteBuffer[_numBytes++] = b;
            }

            private void FlushBytes () {
                if (_numBytes > 0) {
                    _numChars += _encoding.GetChars(_byteBuffer, 0, _numBytes, _charBuffer, _numChars);
                    _numBytes = 0;
                }
            }

            internal string GetString () {
                if (_numBytes > 0) { FlushBytes(); }

                return _numChars > 0 ? new string(_charBuffer, 0, _numChars) : string.Empty;
            }
        }
    }

}
