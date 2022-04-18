module libmucor.varquery.invertedindex.binaryindex;
import std.algorithm.setops;
import std.regex;
import std.algorithm : sort, uniq, map, std_filter = filter, joiner, each;
import std.range : iota, takeExactly;
import std.array : array, replace;
import std.conv : to;
import std.format : format;
import std.string : indexOf;
import std.bitmanip: nativeToLittleEndian, littleEndianToNative;
import std.stdio;
import std.exception : enforce;

import asdf: deserializeAsdf = deserialize, Asdf, AsdfNode, parseJson, serializeToAsdf;
import libmucor.wideint : uint128;
import libmucor.varquery.invertedindex.invertedindex;
import libmucor.varquery.invertedindex.singleindex;
import libmucor.varquery.invertedindex.metadata;
import libmucor.khashl: khashl;
import htslib.hts_endian;
import std.digest.md : MD5Digest, toHexString;

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
struct BinaryJsonInvertedIndex {
    align:
    ulong constant = 0x5845444e495f5156; // VQ_INDEX
    ulong md5ArrLen;                 // # of md5 sums
    ulong keyMetaDataLen;            // # of vcf data fields
    ulong jsonKeyMetaDataLen;        // # of json data fields
    ulong idDataLen;                 // # of data ids
    ulong jsonKeyDataLen;            // len of json keys data 
    uint128 dataChecksum;            // checksum of below data fields

    uint128[] sums;                  // md5 sums
    KeyMetaData[] keyMetaData;          // first set of keys metadata 
    FieldKeyMetaData[] jsonKeyMetaData; // second set of keys metadata
    ulong[] data;                    // ulong ids
    ubyte[] fieldKeyData;            // second set of keys: JsonValue
    ubyte[] keyData;                 // first set of keys: String
    File file;

    this(JSONInvertedIndex idx) {
        // add md5sums
        this.md5ArrLen = idx.recordMd5s.length;
        this.sums = idx.recordMd5s;
        foreach(kv; idx.fields.byKeyValue) {
            KeyMetaData k;
            // add key
            k.keyOffset = this.keyData.length;
            this.keyData ~= cast(ubyte[]) kv.key;
            k.keyLength = this.keyData.length - k.keyOffset;
            // add fields
            k.fieldOffset = this.jsonKeyMetaData.length;
            foreach (kv2; kv.value.hashmap.byKeyValue)
            {
                FieldKeyMetaData fk;
                fk.type = kv2.key.getType;
                // add field key
                fk.keyOffset = this.fieldKeyData.length;
                fieldKeyData ~= kv2.key.toBytes;
                fk.keyLength = this.fieldKeyData.length - fk.keyOffset;
                // add data
                fk.dataOffset = this.data.length;
                data ~= kv2.value;
                fk.dataLength = this.data.length - fk.dataOffset;
                // add field key meta
                this.jsonKeyMetaData ~= fk;
            }
            k.fieldLength = this.jsonKeyMetaData.length - k.fieldOffset;
            this.keyMetaData ~= k;
        }
        this.keyMetaDataLen = this.keyMetaData.length; // # of vcf data fieldshis.keys.length;
        this.jsonKeyMetaDataLen = this.jsonKeyMetaData.length; // # of json data fields
        this.idDataLen = this.data.length; // # of data ids
        this.jsonKeyDataLen = this.fieldKeyData.length;
        this.calculateChecksum; 
    }

    this(string fn){
        this.file = File(fn, "rb");
        import std.file : read;
        ubyte[] bytes;
        auto buf = this.file.rawRead(new ubyte[4096]);
        do {
            bytes ~= buf;
            buf = this.file.rawRead(new ubyte[4096]);
        } while(buf.length == 4096);
        this(bytes);
    }
    /** 
    * load from file in this order:
    *  Constants
    *  md5 checksums
    *  key metadata
    *  json key metadata
    *  id data
    *  json key data
    *  string key data
    * 
    * NOTE: string key data array's first 8 bytes are length of that data
    */
    this(ubyte[] data) {
        auto p = data.ptr;
        load_constants(p);
        enforce((cast(char*)&this.constant)[0..8] == "VQ_INDEX", "File doesn't contain VQ_INDEX sequence");
        // this.validateChecksum()
        load_sums(p);
        load_key_meta(p);
        load_json_key_meta(p);
        load_id_data(p);
        load_field_key_data(p);
        load_key_data(p);
    }

    void load_constants(ref ubyte * p){
        // load constants
        this.constant = le_to_u64(p);
        p += 8;
        this.md5ArrLen = le_to_u64(p);
        p += 8;
        this.keyMetaDataLen = le_to_u64(p);
        p += 8;
        this.jsonKeyMetaDataLen = le_to_u64(p);
        p += 8;
        this.idDataLen = le_to_u64(p);
        p += 8;
        this.jsonKeyDataLen = le_to_u64(p);
        p += 8;
        this.dataChecksum.hi = le_to_u64(p);
        p += 8;
        this.dataChecksum.lo = le_to_u64(p);
        p += 8;
    }

    void load_sums(ref ubyte * p) {
        
        // load md5 sums
        this.sums = new uint128[this.md5ArrLen];
        foreach(i; 0..this.md5ArrLen) {
            this.sums[i].hi = le_to_u64(p);
            p += 8;
            this.sums[i].lo = le_to_u64(p);
            p += 8;
        }
    }

    void load_key_meta(ref ubyte * p){
        // load key metadata
        this.keyMetaData = new KeyMetaData[this.keyMetaDataLen];
        foreach(i; 0..this.keyMetaDataLen) {
            this.keyMetaData[i] = KeyMetaData(p[0..32]);
            p += 32;
        }
    }

    void load_json_key_meta(ref ubyte * p) {
        // load key metadata
        this.jsonKeyMetaData = new FieldKeyMetaData[this.jsonKeyMetaDataLen];
        foreach(i; 0..this.jsonKeyMetaDataLen) {
            this.jsonKeyMetaData[i] = FieldKeyMetaData(p[0..48]);
            p += 48;
        }
    }

    void load_id_data(ref ubyte * p) {
        this.data = new ulong[this.idDataLen];
        // load data
        foreach(i; 0..this.idDataLen) {
            this.data[i] = le_to_u64(p);
            p += 8;
        }
    }

    void load_key_data(ref ubyte * p) {
        auto len = le_to_u64(p);
        p += 8;
        // load key data
        this.keyData = p[0..len];
    }

    void load_field_key_data(ref ubyte * p){
        this.fieldKeyData = p[0..jsonKeyDataLen];
        p += jsonKeyDataLen;
        
    }

    void calculateChecksum() {
        auto md5 = new MD5Digest();
        md5.put(cast(ubyte[])this.sums);
        md5.put(cast(ubyte[])this.keyMetaData);
        md5.put(cast(ubyte[])this.jsonKeyMetaData);
        md5.put(cast(ubyte[])this.data);
        md5.put(this.fieldKeyData);
        md5.put(this.keyData);
        ubyte[16] buf;
        md5.finish(buf);
        uint128 sum;
        sum.fromHexString(buf.toHexString);
        this.dataChecksum = sum;
    }

    // void validateChecksum(ref ubyte * p) {
    //     auto md5 = new MD5Digest();
    //     uint128 sum;
    //     sum.fromHexString(md5.digest(p[0..&data[$-1] - p]).toHexString);
    //     enforce(this.dataChecksum == sum, "Data checksum doesn't match");
    // }

    ubyte[64] serialize_constants() {
        ubyte[64] ret;
        u64_to_le(this.constant, ret.ptr);
        u64_to_le(this.md5ArrLen, ret.ptr + 8);
        u64_to_le(this.keyMetaDataLen, ret.ptr + 16);
        u64_to_le(this.jsonKeyMetaDataLen, ret.ptr + 24);
        u64_to_le(this.idDataLen, ret.ptr + 32);
        u64_to_le(this.jsonKeyDataLen, ret.ptr + 40);
        u64_to_le(this.dataChecksum.hi, ret.ptr + 48);
        u64_to_le(this.dataChecksum.lo, ret.ptr + 56);
        return ret;
    }

    ubyte[] serialize_sums() {
        ubyte[] ret = new ubyte[this.md5ArrLen * 16];
        foreach(i; 0..this.md5ArrLen){
            u64_to_le(this.sums[i].hi, ret.ptr + (16*i));
            u64_to_le(this.sums[i].lo, ret.ptr + (16*i) + 8);
        }
        return ret;
    }

    ubyte[] serialize_keys() {
        ubyte[] ret = new ubyte[this.keyMetaDataLen * 32];
        foreach(i; 0..this.keyMetaDataLen){
            (ret.ptr + (32*i))[0..32] = this.keyMetaData[i].serialize();
        }
        return ret;
    }

    ubyte[] serialize_field_keys() {
        ubyte[] ret = new ubyte[this.jsonKeyMetaDataLen*48];
        foreach(i; 0..this.jsonKeyMetaDataLen){
            (ret.ptr + (48*i))[0..48] = this.jsonKeyMetaData[i].serialize();
        }
        return ret;
    }

    ubyte[] serialize_data() {
        ubyte[] ret = new ubyte[this.idDataLen * 8];
        foreach(i; 0..this.idDataLen){
            u64_to_le(this.data[i], ret.ptr + (i*8));
        }
        return ret;
    }
    /** 
    * Write to file in this order:
    *  Constants
    *  md5 checksums
    *  key metadata
    *  json key metadata
    *  id data
    *  json key data
    *  string key data
    * 
    * NOTE: string key data array's first 8 bytes are length of that data
    */
    void writeToFile(File f){
        
        // write constants
        f.rawWrite(this.serialize_constants);

        f.rawWrite(this.serialize_sums);

        f.rawWrite(this.serialize_keys);
        f.rawWrite(this.serialize_field_keys);

        f.rawWrite(this.serialize_data);
        f.rawWrite(this.fieldKeyData);
        auto keyDataLen = this.keyData.length;
        ubyte[8] lenSpace = [0,0,0,0,0,0,0,0];
        auto bytes =  lenSpace ~ this.keyData;
        u64_to_le(keyDataLen, bytes.ptr);
        f.rawWrite(bytes);
    }
}

unittest{
    import asdf;
    import libmucor.jsonlops.basic: md5sumObject;
    import std.array: array;
    import libmucor.varquery.invertedindex.jsonvalue;
    JSONInvertedIndex idx;
    idx.addJsonObject(`{"test":"hello", "test2":"foo","test3":1}`.parseJson.md5sumObject);
    idx.addJsonObject(`{"test":"world", "test2":"bar","test3":2}`.parseJson.md5sumObject);
    idx.addJsonObject(`{"test":"world", "test4":"baz","test3":3}`.parseJson.md5sumObject);

    assert(idx.recordMd5s.length == 3);
    assert(idx.fields.byKey.array == ["/test", "/test4", "/test3", "/test2"]);

    assert(idx.fields["/test2"].filter(["foo"]) == [0]);
    assert(idx.fields["/test3"].filterRange([1, 3]) == [1, 0]);

    ulong[] exp_constants = [6360565151759814998, 3, 4, 8, 9, 43, 1132386765224132344, 13579944864346696974];
    auto exp_sums = ["4BC5E16362F7052C4C90249CDE512C9D", "60E2DB9459D8C80620A8C2156FCAB161", "51D72970FC05BF560F7A0545A9A36687"];
    auto exp_keys = [KeyMetaData(0, 5, 0, 2), KeyMetaData(5, 6, 2, 1), KeyMetaData(11, 6, 3, 3), KeyMetaData(17, 6, 6, 2)];
    auto exp_fields = [FieldKeyMetaData(3, 0, 0, 5, 0, 2), FieldKeyMetaData(3, 0, 5, 5, 2, 1), FieldKeyMetaData(3, 0, 10, 3, 3, 1), FieldKeyMetaData(1, 0, 13, 8, 4, 1), FieldKeyMetaData(1, 0, 21, 8, 5, 1), FieldKeyMetaData(1, 0, 29, 8, 6, 1), FieldKeyMetaData(3, 0, 37, 3, 7, 1), FieldKeyMetaData(3, 0, 40, 3, 8, 1)];
    {
        auto bidx = BinaryJsonInvertedIndex(idx);
        assert(cast(ulong[])bidx.serialize_constants == exp_constants);
        assert((cast(uint128[])bidx.serialize_sums).map!(x => format("%x",x)).array == exp_sums);
        assert(cast(KeyMetaData[])bidx.serialize_keys == exp_keys);
        assert(cast(FieldKeyMetaData[])bidx.serialize_field_keys == exp_fields);

        auto kv = bidx.keyMetaData[0].deserialize_to_tuple(bidx.jsonKeyMetaData, bidx.keyData);
        assert(kv.key == "/test");
        auto kv2 = kv.value[0].deserialize_to_tuple(bidx.data, bidx.fieldKeyData);
        assert(kv2.key == JSONValue("world"));
        assert(kv2.value == [1,2]);
        writeln(bidx.data);
        writeln(bidx.fieldKeyData);
        writeln(bidx.keyData);
        auto f = File("/tmp/test.bidx", "w");
        bidx.writeToFile(f);
        f.close;
    }

    import std.file: read;
    auto data = cast(ubyte[])read("/tmp/test.bidx");

    {
        
        BinaryJsonInvertedIndex bidx;

        auto p = data.ptr;
        bidx.load_constants(p);
        enforce((cast(char*)&bidx.constant)[0..8] == "VQ_INDEX", "File doesn't contain VQ_INDEX sequence");
        assert(cast(ulong[])bidx.serialize_constants == exp_constants);
        // this.validateChecksum()
        bidx.load_sums(p);
        assert((cast(uint128[])bidx.serialize_sums).map!(x => format("%x",x)).array == exp_sums);
        bidx.load_key_meta(p);
        assert(cast(KeyMetaData[])bidx.serialize_keys == exp_keys);
        bidx.load_json_key_meta(p);
        assert(cast(FieldKeyMetaData[])bidx.serialize_field_keys == exp_fields);
        bidx.load_id_data(p);
        bidx.load_field_key_data(p);
        bidx.load_key_data(p);
        writeln(bidx.data);
        writeln(bidx.fieldKeyData);
        writeln(bidx.keyData);
        
        auto kv = bidx.keyMetaData[0].deserialize_to_tuple(bidx.jsonKeyMetaData, bidx.keyData);
        writeln(exp_fields);
        writeln(bidx.jsonKeyMetaData);
        writeln(kv);
        assert(kv.key == "/test");
        auto kv2 = kv.value[0].deserialize_to_tuple(bidx.data, bidx.fieldKeyData);
        assert(kv2.key == JSONValue("world"));
        assert(kv2.value == [1,2]);
    }
    {
        auto bidx = BinaryJsonInvertedIndex(data);

        assert(cast(ulong[])bidx.serialize_constants == exp_constants);
        assert((cast(uint128[])bidx.serialize_sums).map!(x => format("%x",x)).array == exp_sums);
        assert(cast(KeyMetaData[])bidx.serialize_keys == exp_keys);
        assert(cast(FieldKeyMetaData[])bidx.serialize_field_keys == exp_fields);
        
        auto kv = bidx.keyMetaData[0].deserialize_to_tuple(bidx.jsonKeyMetaData, bidx.keyData);
        assert(kv.key == "/test");
        auto kv2 = kv.value[0].deserialize_to_tuple(bidx.data, bidx.fieldKeyData);
        assert(kv2.key == JSONValue("world"));
        assert(kv2.value == [1,2]);
    }

}