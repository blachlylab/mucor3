module libmucor.varquery.invertedindex.binaryindex;
import std.algorithm.setops;
import std.regex;
import std.algorithm : sort, uniq, map, std_filter = filter, joiner, each;
import std.range : iota, takeExactly, zip;
import std.array : array, replace;
import std.conv : to;
import std.format : format;
import std.string : indexOf;
import std.bitmanip: nativeToLittleEndian, littleEndianToNative;
import std.stdio;
import std.exception : enforce;
import htslib.hfile: off_t;
import std.traits: isSomeString;
import std.file: exists;

import asdf: deserializeAsdf = deserialize, Asdf, AsdfNode, parseJson, serializeToAsdf;
import libmucor.wideint : uint128;
import libmucor.varquery.invertedindex.invertedindex;
import libmucor.varquery.invertedindex.metadata;
import libmucor.varquery.invertedindex.jsonvalue;
import libmucor.varquery.invertedindex.store;
import libmucor.khashl;
import htslib.hts_log;
import std.format: format;

/** 
 * Represent inverted index as it exists on disk:
 *  Constants:                      bytes
 *      VQ_INDEX constant:              8
 *      MD5 array length:               8
 *      key metadata length:            8
 *      json key metadata length:       8
 *      id data length:                 8
 *      json key data length:           8
 *
 *      variable length data checksum: 16
 *      
 *  Variable length data
 *      md5 checksums:                 16*n
 *      key metadata:                  32*n
 *      json key metadata:             48*n
 *      id data:                       8*n
 *      json key data:                 n
 *      string key data:               n
 * 
 * NOTE: string key data array's first 8 bytes are length of that data
 */
struct BinaryIndexWriter {
    string prefix;
    /// hashset of observed keys
    khashlSet!(uint128) seenKeys;
    /// Json store
    JsonStoreWriter * jsonStore;

    /// Id file cache
    IdFileCacheWriter * idCache;

    /// store json value meta data
    KeyMetaStore * metadata;
    /// store json value hashes
    MD5Store * hashes;

    ulong numSums;

    /// store md5 sums
    MD5Store * sums;
    /// store md5 sums
    StringStore * keys;

    this(string prefix) {
        this.prefix = prefix;
        this.hashes = new MD5Store(prefix ~ ".keys.md5", "wbu");
        this.metadata = new KeyMetaStore(prefix ~  ".keys.meta", "wbu");
        this.sums = new MD5Store(prefix ~  ".record.sums", "wbu");
        this.keys = new StringStore(prefix ~  ".keys", "wbu");
        this.jsonStore = new JsonStoreWriter(prefix);
        this.idCache = new IdFileCacheWriter(prefix);
    }

    void close() {
        this.idCache.close;
        this.jsonStore.close;
        this.hashes.close;
        this.metadata.close;
        this.sums.close;
        this.keys.close;
    }

    void insert(T)(T key, JSONValue item) 
    if(isSomeString!T)
    {
        hts_log_debug(__FUNCTION__, format("inserting json value %s for key %s", item, key));
        auto keyhash = getKeyHash(key);
        auto p = keyhash in seenKeys;
        if(!p) {
            this.seenKeys.insert(keyhash);

            KeyMetaData meta;
            meta.keyHash = keyhash;
            meta.keyOffset = this.keys.tell;
            this.keys.write(key);
            meta.keyLength = this.keys.tell - meta.keyOffset;
            metadata.write(meta);
        } 
        
        auto valHash = this.jsonStore.insert(item);

        auto newHash = combineHash(keyhash, valHash);

        this.idCache.insert(newHash, this.numSums);
    }
}

/// Stores json data and ids
/// for a given field
struct BinaryIndexReader {
    string prefix;
    /// hashset of observed keys
    khashlSet!(uint128) seenKeys;

    /// json store
    JsonStoreReader * jsonStore;

    /// Id file cache
    IdFileCacheReader * idCache;

    /// Key metadata
    KeyMetaData[] metadata;
    /// store md5 sums
    uint128[] sums;
    /// store json value hashes
    MD5Store * hashes;
    /// store md5 sums
    StringStore * keys;

    this(string prefix) {
        this.prefix = prefix;
        this.hashes = new MD5Store(prefix ~ ".keys.md5", "rb");
        auto md = new KeyMetaStore(prefix ~  ".keys.meta", "rb");
        this.metadata = md.getAll.array;
        md.close;
        hts_log_debug(__FUNCTION__, format("loading key index %s with %d keys", prefix, this.metadata.length));
        this.sums = new MD5Store(prefix ~  ".record.sums", "rb").getAll.array;
        this.keys = new StringStore(prefix ~  ".keys", "rb");
        this.jsonStore = new JsonStoreReader(prefix);
        this.idCache = new IdFileCacheReader(prefix);
        foreach (KeyMetaData key; metadata)
        {
            this.seenKeys.insert(key.keyHash);
        }
    }

    void close() {
        this.jsonStore.close;
        this.idCache.close;
        this.hashes.close;
        this.keys.close;
    }

    auto getKeysWithId() {
        auto strkeys = this.metadata
            .map!(meta => this.keys.readFromPosition(meta.keyOffset));
        auto hashes = this.metadata
            .map!(meta => meta.keyHash);
        return zip(strkeys, hashes);
    }
}

unittest
{
    import htslib.hts_log;
    hts_set_log_level(htsLogLevel.HTS_LOG_DEBUG);
    {
        auto bidx = BinaryIndexWriter("/tmp/test_bidx");
        bidx.insert("testkey", JSONValue("testval"));
        bidx.sums.write(uint128(0));
        bidx.insert("testkey", JSONValue("testval2"));
        bidx.sums.write(uint128(1));
        bidx.insert("testkey2", JSONValue(0));
        bidx.sums.write(uint128(2));
        bidx.insert("testkey2", JSONValue(2));
        bidx.sums.write(uint128(3));
        bidx.insert("testkey2", JSONValue(3));
        bidx.sums.write(uint128(4));
        bidx.insert("testkey2", JSONValue(5));
        bidx.sums.write(uint128(5));
        bidx.insert("testkey3", JSONValue(1.2));
        bidx.sums.write(uint128(6));
        bidx.close;
    }

    {
        auto bidx = BinaryIndexReader("/tmp/test_bidx");
        assert(bidx.sums.length == 7);
        assert(bidx.metadata.length == 3);
    }
    
}