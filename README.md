# EasyDB

**EasyDB is a tiny document store for C# applications. The API looks like a key/value database, 
while the implementation is a thin wrapper which saves each document as a 
separate plain file on the local filesystem, so they can be easily
found, inspected, modified, backed up, etc.**
 

Features:
  - Basic key-value store, where each document is stored in a separate file on the filesystem
    and retrieved by its key. Documents can be binary, or unicode text (e.g. JSON).
  - Key structure supports key hierarchies, e.g. not just `user123` but also hierarchical keys 
    like `users/uid123` or `documents/2021/01/id123`.
  - Supports common key-value operations: `contains`, `get`, `set`, `check and set`, `remove`.
  - Supports key and value enumerations: `enumerate`, `enumerate documents`, starting from root
    or specific part of the hierarchy (e.g. `documents/2021/*`).
  - Single-file C# library, designed for embedding, requires no external libraries, 
    and does not spawn any background processes or services.
  
The database is ideal for fast prototyping, tools, small websites, and other basic persistence 
that doesn't demand a full-blown relational database.


## Implementation

EasyDB is a document-oriented storage, where each key maps to a single document (text or binary),
which in turn is stored in a separate file on the filesystem. 

Client code can retrieve and update individual documents by key name, and
this is reflected directly by reading and writing to corresponding files. 

Some examples: 

```
var db = new EasyDB(new EasyDBSettings("c:\\storage", ".data"));

// creates or updates a file containing the given string
// this will be stored as "c:\\storage\\mykey.data"
db.Set("mykey", "some text value");

// creates or updates a file containing the given byte array
// the hierarchical key means it will be stored as "c:\\storage\\users\\uid001.data"
db.Set("users/uid001", new byte[] { ... });

// reads the contents of file "mykey.data" as a document (can be recast to binary or text)
var result = db.Get("mykey");
byte[] bytes = result.value.Bytes;
string text = result.value.AsString;

// updates mykey.data only if it hasn't changed since last read
db.CheckAndSet("mykey", newdocument, result.etag); 
 
// checks for existence of file "users/uid001.data" and deletes it (and its directory if empty)
bool exists = db.Contains("users/uid001");
if (exists) { db.Remove("users/uid001"); }

// enumerates keys of all documents in "users" and subdirectories
foreach (var key in db.Enumerate("users", true)) { ... }

// enumerates all documents in the entire database, and returns their full contents
foreach (var result in db.EnumerateValues("", true)) { ... }
```


### API

Standard key-value operations are supported:
  - Check if a key exists in the database
  - Get the document for a given key
  - Set the document for a given key (i.e. create or overwrite file if already exists)
  - Check and set a key (i.e. update only if entry matches expected etag value)
  - Remove entries
  - Enumerate keys or documents at a given path (optionally including subpaths)
  
This wrapper does not perform any caching, it relies on OS filesystem caching for performance.


### Design goals

  - Simplicity - Behavior needs to be understandable on first reading
  - Inspectability - User can inspect every stored document as a separate file on the hard drive
  - Minimal API - Simple C# API for standard key-value store commands
  - Multi-threading - Safe to use in multi-threaded environments


## Key space

The key/value store is keyed by strings, and stores arbitrary data blobs as values. 

The key space supports key hierarchies, with slash character `/` as separator, 
so that the user can enumerate all keys at any point in the hierarchy, for example
`users/*` or `users/europe/*` and so on.

Keys can be arbitrary UTF strings, and they will be URL-encoded as needed
to meet the requirements of the target filesystem.


## Storage implementation details

Each entry in the database corresponds to a single file on the filesystem.

Keys and hierarchy segments are implemented as paths, so that users can easily find
data files locally. A simple ASCII key name like `foo/bar/baz` gets turned
into the corresponding filesystem path, and any key with special characters will be
URL-encoded first before being used as filenames.

For atomic check-and-set operations, the database uses `etag` which is a 
unique identifier of the value being stored, which needs to match the value 
passed in from the client before an update is made. 
This implementation uses last file update times as etag.


### Threading model

The database is safe for multithreaded access from a single process. 

All filesystem access is guarded by a reader/writer lock, so that all read operations 
(get, contains, enumerate) can occur simultaneously and without blocking on each other,
while all write operations (set, check and set, remove) will wait for any read operations
to finish, and will be mutually exclusive from each other and from any kind of a read operation. 


## Limitations

Because the database is a direct reflection of the filesystem, some limitations apply:
  - On case-insensitive filesystems (like Windows), keys with different casing 
    will behave like multiple copies of the same key (return the same data, etc)
    even though they're not technically equal.
  - Having a document with the same name as a hierarchy segment is not supported,
    since filesystems typically don't support having a file and a directory with the same name.
    For example, when calling `Set("users/2021/uid123", ...)` and then `Set("users/2021", ...)` 
    (or vice versa), the latter operation will fail.
  - The library is safe for multithreaded use from the same process, but simultaneous
    access from multiple processes is not supported and may fail in unexpected ways.
  - The database performs no indexing beyond key access by name; values are treated
    as opaque binary blobs and are not indexed or otherwise processed.

