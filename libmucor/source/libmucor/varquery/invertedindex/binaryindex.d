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

import asdf: deserializeAsdf = deserialize, Asdf, AsdfNode, parseJson, serializeToAsdf;
import libmucor.wideint : uint128;
import libmucor.varquery.invertedindex.invertedindex;
import libmucor.varquery.invertedindex.metadata;
import libmucor.varquery.invertedindex.jsonvalue;
import libmucor.varquery.invertedindex.store;
import libmucor.khashl;

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
    /// hashmap for writing
    /// json value hash maps to a set of ids
    khashl!(uint128, JsonStoreWriter) hashmap;
    /// store json value meta data
    KeyMetaStore metadata;
    /// store json value hashes
    MD5Store hashes;

    ulong numSums;

    /// store md5 sums
    MD5Store sums;
    /// store md5 sums
    StringStore keys;

    this(string prefix) {
        this.prefix = prefix;
        this.hashes = MD5Store(prefix ~ ".keys.md5", "wb");
        this.metadata = KeyMetaStore(prefix ~  ".keys.meta", "wb");
        this.sums = MD5Store(prefix ~  ".record.sums", "wb");
        this.keys = StringStore(prefix ~  ".keys", "wb");
    }

    void insert(T)(T key, JSONValue item) 
    if(isSomeString!T)
    {
        auto keyhash = getKeyHash(key);
        auto p = keyhash in hashmap;
        if(p) {
            p.insert(item, this.numSums++);
        } else {
            KeyMetaData meta;
            meta.keyHash = keyhash;
            auto s = JsonStoreWriter(prefix ~ "_" ~ format("%x",keyhash));
            this.hashmap[keyhash] = s;
            s.insert(item, this.numSums++);
            meta.keyOffset = this.keys.tell;
            this.keys.write(key);
            meta.keyLength = this.keys.tell - meta.keyOffset;
            metadata.write(meta);
        }
    }
}

/// Stores json data and ids
/// for a given field
struct BinaryIndexReader {
    string prefix;
    /// hashmap for writing
    /// json value hash maps to a set of ids
    khashl!(uint128, JsonStoreReader) hashmap;
    /// Key metadata
    KeyMetaData[] metadata;
    /// store md5 sums
    uint128[] sums;
    /// store json value hashes
    MD5Store hashes;
    /// store md5 sums
    StringStore keys;

    this(string prefix) {
        this.prefix = prefix;
        this.hashes = MD5Store(prefix ~ ".keys.md5", "rb");
        this.metadata = KeyMetaStore(prefix ~  ".keys.meta", "rb").getAll;
        this.sums = MD5Store(prefix ~  ".record.sums", "rb").getAll;
        this.keys = StringStore(prefix ~  ".keys", "rb");
        foreach(meta; this.metadata) {
            hashmap[meta.keyHash] = JsonStoreReader(prefix ~ "_" ~ format("%x", meta.keyHash));
        }
        this.metadata = KeyMetaStore(prefix ~  ".json.meta", "rb").getAll;
        this.sums = MD5Store(prefix ~  ".record.sums", "rb").getAll;
    }

    auto getKeysWithJsonStore() {
        auto strkeys = this.metadata
            .map!(meta => this.keys.readFromPosition(meta.keyOffset));
        auto stores = this.metadata
            .map!(meta => meta.keyHash in this.hashmap);
        return zip(strkeys, stores);
    }
}

// unittest
// {
//     {
//         auto bidx = BinaryIndexWriter("/tmp/test");
//         bidx.insert("testkey", JSONValue("testval"));
//         bidx.sums.write(uint128(0));
//         bidx.insert("testkey", JSONValue("testval2"));
//         bidx.sums.write(uint128(1));
//         bidx.insert("testkey2", JSONValue(0));
//         bidx.sums.write(uint128(2));
//         bidx.insert("testkey2", JSONValue(2));
//         bidx.sums.write(uint128(3));
//         bidx.insert("testkey2", JSONValue(3));
//         bidx.sums.write(uint128(4));
//         bidx.insert("testkey2", JSONValue(5));
//         bidx.sums.write(uint128(5));
//         bidx.insert("testkey3", JSONValue(1.2));
//         bidx.sums.write(uint128(6));
//     }

//     {
//         auto bidx = BinaryIndexReader("/tmp/test");
//         assert(bidx.sums.length == 7);
//         assert(bidx.metadata.length == 7);

//     }
    
// }


// unittest{
//     import asdf;
//     import libmucor.jsonlops.basic: md5sumObject;
//     import std.array: array;
//     import libmucor.varquery.invertedindex.jsonvalue;
//     InvertedIndex idx;
//     idx.addJsonObject(`{"test":"hello", "test2":"foo","test3":1}`.parseJson.md5sumObject);
//     idx.addJsonObject(`{"test":"world", "test2":"bar","test3":2}`.parseJson.md5sumObject);
//     idx.addJsonObject(`{"test":"world", "test4":"baz","test3":3}`.parseJson.md5sumObject);

//     assert(idx.recordMd5s.length == 3);
//     assert(idx.fields.byKey.array == ["/test", "/test4", "/test3", "/test2"]);

//     assert(idx.fields["/test2"].filter(["foo"]) == [0]);
//     assert(idx.fields["/test3"].filterRange([1, 3]) == [1, 0]);

//     ulong[] exp_constants = [6360565151759814998, 3, 4, 8, 9, 43, 1132386765224132344, 13579944864346696974];
//     auto exp_sums = ["4BC5E16362F7052C4C90249CDE512C9D", "60E2DB9459D8C80620A8C2156FCAB161", "51D72970FC05BF560F7A0545A9A36687"];
//     auto exp_keys = [KeyMetaData(0, 5, 0, 2), KeyMetaData(5, 6, 2, 1), KeyMetaData(11, 6, 3, 3), KeyMetaData(17, 6, 6, 2)];
//     auto exp_fields = [JsonKeyMetaData(3, 0, 0, 5, 0, 2), JsonKeyMetaData(3, 0, 5, 5, 2, 1), JsonKeyMetaData(3, 0, 10, 3, 3, 1), JsonKeyMetaData(1, 0, 13, 8, 4, 1), JsonKeyMetaData(1, 0, 21, 8, 5, 1), JsonKeyMetaData(1, 0, 29, 8, 6, 1), JsonKeyMetaData(3, 0, 37, 3, 7, 1), JsonKeyMetaData(3, 0, 40, 3, 8, 1)];
//     {
//         auto bidx = BinaryIndex(idx);
//         assert(cast(ulong[])bidx.serializeConstants == exp_constants);
//         assert((cast(uint128[])bidx.serializeSums).map!(x => format("%x",x)).array == exp_sums);
//         assert(cast(KeyMetaData[])bidx.serializeKeys == exp_keys);
//         assert(cast(JsonKeyMetaData[])bidx.serializeFieldKeys == exp_fields);

//         auto kv = bidx.keyMetaData[0].deserialize_to_tuple(bidx.jsonKeyMetaData, bidx.keyData);
//         assert(kv.key == "/test");
//         auto kv2 = kv.value[0].deserialize_to_tuple(bidx.data, bidx.fieldKeyData);
//         assert(kv2.key == JSONValue("world"));
//         assert(kv2.value == [1,2]);
//         writeln(bidx.data);
//         writeln(bidx.fieldKeyData);
//         writeln(bidx.keyData);
//         auto f = File("/tmp/test.bidx", "w");
//         bidx.sums.writeToFile(f);
//         f.close;
//     }

//     import std.file: read;
//     auto data = cast(ubyte[])read("/tmp/test.bidx");

//     {
        
//         BinaryIndex bidx;

//         auto p = data.ptr;
//         bidx.loadConstants(p);
//         enforce((cast(char*)&bidx.constant)[0..8] == "VQ_INDEX", "File doesn't contain VQ_INDEX sequence");
//         assert(cast(ulong[])bidx.serializeConstants == exp_constants);
//         // this.validateChecksum()
//         bidx.loadSums(p);
//         assert((cast(uint128[])bidx.serializeSums).map!(x => format("%x",x)).array == exp_sums);
//         bidx.loadKeyMeta(p);
//         assert(cast(KeyMetaData[])bidx.serializeKeys == exp_keys);
//         bidx.loadJsonKeyMeta(p);
//         assert(cast(JsonKeyMetaData[])bidx.serializeFieldKeys == exp_fields);
//         bidx.loadIdData(p);
//         bidx.loadFieldKeyData(p);
//         bidx.loadKeyData(p);
//         writeln(bidx.data);
//         writeln(bidx.fieldKeyData);
//         writeln(bidx.keyData);
        
//         auto kv = bidx.keyMetaData[0].deserialize_to_tuple(bidx.jsonKeyMetaData, bidx.keyData);
//         writeln(exp_fields);
//         writeln(bidx.jsonKeyMetaData);
//         writeln(kv);
//         assert(kv.key == "/test");
//         auto kv2 = kv.value[0].deserialize_to_tuple(bidx.data, bidx.fieldKeyData);
//         assert(kv2.key == JSONValue("world"));
//         assert(kv2.value == [1,2]);
//     }
//     {
//         auto bidx = BinaryIndex(data);

//         assert(cast(ulong[])bidx.serializeConstants == exp_constants);
//         assert((cast(uint128[])bidx.serializeSums).map!(x => format("%x",x)).array == exp_sums);
//         assert(cast(KeyMetaData[])bidx.serializeKeys == exp_keys);
//         assert(cast(JsonKeyMetaData[])bidx.serializeFieldKeys == exp_fields);
        
//         auto kv = bidx.keyMetaData[0].deserialize_to_tuple(bidx.jsonKeyMetaData, bidx.keyData);
//         assert(kv.key == "/test");
//         auto kv2 = kv.value[0].deserialize_to_tuple(bidx.data, bidx.fieldKeyData);
//         assert(kv2.key == JSONValue("world"));
//         assert(kv2.value == [1,2]);
//     }

// }

