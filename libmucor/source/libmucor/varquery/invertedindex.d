module libmucor.varquery.invertedindex;
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
import libmucor.varquery.singleindex;
import libmucor.khashl: khashl;
import htslib.hts_endian;

char sep = '/';

struct JSONInvertedIndex{
    uint128[] recordMd5s;
    khashl!(const(char)[], InvertedIndex) fields;
    this(string f){
        fromFile(f);
    }
    void addJsonObject(Asdf root, const(char)[] path = ""){
        if(path == ""){
            uint128 a;
            debug if(root["md5"] == Asdf.init) stderr.writeln("record with no md5");
            auto md5 = root["md5"].deserializeAsdf!string;
            root["md5"].remove;
            a.fromHexString(md5);
            this.recordMd5s ~= a;
        }
        foreach (key,value; root.byKeyValue)
        {
            JSONValue valkey;
            if(value.kind == Asdf.Kind.object){
                addJsonObject(value, path~sep~key);
                continue;
            }else if(value.kind == Asdf.Kind.array){
                addJsonArray(value, path~sep~key);
                continue;
            }else if(value.kind == Asdf.Kind.null_){
                continue;
            }else{
                valkey = JSONValue(value);
            }
            auto p = path~sep~key in fields;
            if(p){
                ulong[] arr = new ulong[0];
                auto val = (*p).hashmap.require(valkey,arr);
                (*val) ~= this.recordMd5s.length - 1;
            }else{
                ulong[] arr = new ulong[0];
                fields[path~sep~key] = InvertedIndex();
                InvertedIndex * hm= (path~sep~key) in fields;
                auto val = hm.hashmap.require(valkey,arr);
                (*val) ~= this.recordMd5s.length - 1;
            }
        }
        
    }
    void addJsonArray(Asdf root, const(char)[] path){
        assert(path != "");
        foreach (value; root.byElement)
        {
            JSONValue valkey;
            if(value.kind == Asdf.Kind.object){
                addJsonObject(value, path);
                continue;
            }else if(value.kind == Asdf.Kind.array){
                addJsonArray(value, path);
                continue;
            }else if(value.kind == Asdf.Kind.null_){
                continue;
            }else{
                valkey = JSONValue(value);
            }
            auto p = path in fields;
            if(p){
                ulong[] arr = new ulong[0];
                auto val = (*p).hashmap.require(valkey,arr);
                (*val) ~= this.recordMd5s.length - 1; 
            }else{
                ulong[] arr = new ulong[0];
                fields[path] = InvertedIndex();
                InvertedIndex * hm= path in fields;
                auto val = hm.hashmap.require(valkey,arr);
                (*val) ~= this.recordMd5s.length - 1;
            }
        }
        
    }

    void fromFile(string f){
        import std.file : read;
        auto bytesRange = cast(ubyte[])(f.read());
        // read const sequence
        
        auto bidx = BinaryJsonInvertedIndex(bytesRange);
        this.recordMd5s = bidx.sums;
        foreach(k; bidx.keysMeta) {
            auto kv = k.deserialize_to_tuple(bidx.fieldKeyMeta, bidx.keyData);
            InvertedIndex idx;
            foreach(fkv; kv.value) {
                auto kv2 = fkv.deserialize_to_tuple(bidx.data, bidx.fieldKeyData);
                idx.hashmap[kv2.key] = kv2.value;
            }
            this.fields[kv.key] = idx;
        }
    }

    /** 
     * 
     * Format of data written (size/itemsize):
     * VQ_INDEX constant: 8
     * MD5 array length: 8
     * [md5 array]: 16*n
     * num of fields: 8
     * length of data: 8
     * [data matrix]: 8*n
     * for each field {
     *     key_type: 1, 15 padd
     *     key_offset: 8,
     *     key_length: 8,
     *     matrix_offset: 8,
     *     length: 8,
     * }
     * [key data]: n
     * 
     * 
     */
    void writeToFile(File f){
        
        auto bidx = BinaryJsonInvertedIndex(this);
        // write constants
        f.rawWrite(bidx.serialize_constants);

        f.rawWrite(bidx.serialize_sums);

        f.rawWrite(bidx.serialize_keys);

        f.rawWrite(bidx.serialize_data);
        f.rawWrite(bidx.fieldKeyData);
        f.rawWrite(bidx.keyData);
    }

    ulong[] allIds(){
        return iota(0, this.recordMd5s.length).array;
        // return this.recordMd5s;
    }

    const(InvertedIndex)*[] getFields(string key)
    {
        auto keycopy = key.idup;
        if(key[0] != '/') throw new Exception("key is missing leading /");
        const(InvertedIndex)*[] ret;
        auto wildcard = key.indexOf('*');
        if(wildcard == -1){
            auto p = key in fields;
            if(!p) throw new Exception(" key "~key~" is not found");
            ret = [p];
            if(ret.length == 0){
                stderr.writeln("Warning: Key"~ keycopy ~" was not found in index!");
            }
        }else{
            key = key.replace("*",".*");
            auto reg = regex("^" ~ key ~"$");
            ret = fields.byKey.std_filter!(x => !(x.matchFirst(reg).empty)).map!(x=> &fields[x]).array;
            if(ret.length == 0){
                stderr.writeln("Warning: Key wildcards sequence "~ keycopy ~" matched no keys in index!");
            }
        }
        debug stderr.writefln("Key %s matched %d keys",keycopy,ret.length);
        return ret;
    }

    ulong[] query(T)(string key,T value){
        auto matchingFields = getFields(key);
        return matchingFields
                .map!(x=> (*x).filter([value]))
                .joiner.array.sort.uniq.array;
    }
    ulong[] queryRange(T)(string key,T first,T second){
        auto matchingFields = getFields(key);
        return matchingFields
                .map!(x=> (*x).filterRange([first,second]))
                .joiner.array.sort.uniq.array;
    }
    template queryOp(string op)
    {
        ulong[] queryOp(T)(string key,T val){
            auto matchingFields = getFields(key);
            return matchingFields
                    .map!(x=> (*x).filterOp!op(val))
                    .joiner.array.sort.uniq.array;
        }
    }
    
    ulong[] queryAND(T)(string key,T[] values){
        auto matchingFields = getFields(key);
        return matchingFields.map!((x){
            auto results = values.map!(y=>(*x).filter([y]).sort.uniq.array).array;
            auto intersect = results[0];
            foreach (item; results)
                intersect = setIntersection(intersect,item).array;
            return intersect.array;
        }).joiner.array.sort.uniq.array;
    }
    ulong[] queryOR(T)(string key,T[] values){
        auto matchingFields = getFields(key);
        return matchingFields.map!(x=> (*x).filter(values)).joiner.array.sort.uniq.array;
    }
    ulong[] queryNOT(ulong[] values){
        return allIds.sort.uniq.setDifference(values).array;
    }

    uint128[] convertIds(ulong[] ids)
    {
        return ids.map!(x => this.recordMd5s[x]).array;
    }

    auto opBinaryRight(string op)(JSONInvertedIndex lhs)
    {
        static if(op == "+") {
            JSONInvertedIndex ret;
            ret.recordMd5s = this.recordMd5s.dup;
            ret.fields = this.fields.dup;
            foreach(kv; lhs.fields.byKeyValue()) {
                auto v = kv.key in ret.fields;
                if(v) {
                    foreach(kv2; kv.value.hashmap.byKeyValue){
                        auto v2 = kv2.key in v.hashmap;
                        if(v2) {
                            *v2 = *v2 ~ kv2.value;
                        } else {
                            v.hashmap[kv2.key] = kv2.value;
                        }
                    }
                } else {
                    ret.fields[kv.key] = kv.value;
                }
            }
            return ret;
        } else
            static assert(false, "Op not implemented");
    }
}

/** 
 * VQ_INDEX constant: 8
 * MD5 array length: 8
 * num of fields: 8
 * length of key metadata: 8
 * length of data: 8
 * length of key data: 8
 */
struct BinaryJsonInvertedIndex {
    align:
    ulong constant = 0x5845444e495f5156; // VQ_INDEX
    ulong md5ArrLen;                 // # of md5 sums
    ulong keysLen;                   // # of vcf data fields
    ulong fieldKeysLen;              // # of json data fields
    ulong dataLen;                   // # of data ids
    ulong fieldDataLen;              // len of json keys data 
    uint128[] sums;                  // md5 sums
    KeyMetaData[] keysMeta;          // first set of keys metadata 
    FieldKeyMetaData[] fieldKeyMeta; // second set of keys metadata
    ulong[] data;                    // ulong ids
    ubyte[] fieldKeyData;            // second set of keys: JsonValue
    ubyte[] keyData;                 // first set of keys: String

    this(JSONInvertedIndex idx) {
        this.md5ArrLen = idx.recordMd5s.length;
        this.sums = idx.recordMd5s;
        foreach(kv; idx.fields.byKeyValue) {
            KeyMetaData k;
            // add key
            k.keyOffset = this.keyData.length;
            this.keyData ~= cast(ubyte[]) kv.key;
            k.keyLength = this.keyData.length - k.keyOffset;
            // add fields
            k.fieldOffset = this.fieldKeyMeta.length;
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
                this.fieldKeyMeta ~= fk;
            }
            k.fieldLength = this.fieldKeyMeta.length - k.fieldOffset;
            this.keysMeta ~= k;
        }
        this.keysLen = this.keysMeta.length; // # of vcf data fieldshis.keys.length;
        this.fieldKeysLen = this.fieldKeyMeta.length; // # of json data fields
        this.dataLen = this.data.length; // # of data ids
        this.fieldDataLen = this.fieldKeyData.length; 
    }

    this(ubyte[] data) {
        auto p = data.ptr;
        // load constants
        this.constant = le_to_u64(p);
        p += 8;
        this.md5ArrLen = le_to_u64(p);
        p += 8;
        this.keysLen = le_to_u64(p);
        p += 8;
        this.fieldKeysLen = le_to_u64(p);
        p += 8;
        this.dataLen = le_to_u64(p);
        p += 8;
        this.fieldDataLen = le_to_u64(p);
        p += 8;

        // load md5 sums
        this.sums = new uint128[this.md5ArrLen];
        foreach(i; 0..this.md5ArrLen) {
            this.sums[i].hi = le_to_u64(p);
            p += 8;
            this.sums[i].lo = le_to_u64(p);
            p += 8;
        }

        // load key metadata
        this.keysMeta = new KeyMetaData[this.keysLen];
        foreach(i; 0..this.keysLen) {
            this.keysMeta[i] = KeyMetaData(p[0..32]);
            p += 32;
        }

        // load key metadata
        this.fieldKeyMeta = new FieldKeyMetaData[this.fieldKeysLen];
        foreach(i; 0..this.fieldKeysLen) {
            this.fieldKeyMeta[i] = FieldKeyMetaData(p[0..48]);
            p += 48;
        }

        this.data = new ulong[this.dataLen];
        // load data
        foreach(i; 0..this.dataLen) {
            this.data[i] = le_to_u64(p);
            p += 8;
        }

        this.fieldKeyData = p[0..fieldDataLen];
        p += fieldDataLen;
        // load key data
        this.keyData = p[0..&data[$-1] - p];

        writeln(cast(string)data);
        writeln(data);
        enforce((cast(char*)&this.constant)[0..8] == "VQ_INDEX", "File doesn't contain VQ_INDEX sequence");
    }

    ubyte[48] serialize_constants() {
        ubyte[48] ret;
        u64_to_le(this.constant, ret.ptr);
        u64_to_le(this.md5ArrLen, ret.ptr + 8);
        u64_to_le(this.keysLen, ret.ptr + 16);
        u64_to_le(this.fieldKeysLen, ret.ptr + 24);
        u64_to_le(this.dataLen, ret.ptr + 32);
        u64_to_le(this.fieldDataLen, ret.ptr + 40);
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
        ubyte[] ret = new ubyte[this.keysLen * 32];
        foreach(i; 0..this.keysLen){
            (ret.ptr + (32*i))[0..32] = this.keysMeta[i].serialize();
        }
        return ret;
    }

    ubyte[] serialize_field_keys() {
        ubyte[] ret = new ubyte[this.fieldKeysLen*48];
        foreach(i; 0..this.fieldKeysLen){
            (ret.ptr + (48*i))[0..48] = this.fieldKeyMeta[i].serialize();
        }
        return ret;
    }

    ubyte[] serialize_data() {
        ubyte[] ret = new ubyte[this.dataLen * 8];
        foreach(i; 0..this.dataLen){
            u64_to_le(this.data[i], ret.ptr + (i*8));
        }
        return ret;
    }
    
}

unittest{
    import asdf;
    import libmucor.jsonlops.basic: md5sumObject;
    import std.array: array;
    JSONInvertedIndex idx;
    idx.addJsonObject(`{"test":"hello", "test2":"foo","test3":1}`.parseJson.md5sumObject);
    idx.addJsonObject(`{"test":"world", "test2":"bar","test3":2}`.parseJson.md5sumObject);
    idx.addJsonObject(`{"test":"world", "test4":"baz","test3":3}`.parseJson.md5sumObject);

    assert(idx.recordMd5s.length == 3);
    assert(idx.fields.byKey.array == ["/test", "/test4", "/test3", "/test2"]);

    assert(idx.fields["/test2"].filter(["foo"]) == [0]);
    assert(idx.fields["/test3"].filterRange([1, 3]) == [1, 0]);

    auto bidx = BinaryJsonInvertedIndex(idx);
    writeln(bidx);
    auto f = File("/tmp/test.idx", "w");
    idx.writeToFile(f);
    f.close;

    import std.file: read;
    bidx = BinaryJsonInvertedIndex(cast(ubyte[])read("/tmp/test.idx"));
    writeln(bidx);
    assert(bidx.md5ArrLen == 3);
    assert(bidx.keysLen == 4);
    assert(bidx.fieldKeysLen == 8);
    assert(bidx.dataLen == 9);
    idx = JSONInvertedIndex("/tmp/test.idx");

    assert(idx.recordMd5s.length == 3);
    assert(idx.fields.byKey.array == ["/test", "/test4", "/test3", "/test2"]);

    assert(idx.fields["/test2"].filter(["foo"]) == [0]);
    assert(idx.fields["/test3"].filterRange([1, 3]) == [1, 0]);

    // root["INFO","ANN"] = AsdfNode(ann.parseJson);
    // idx.addJsonObject(cast(Asdf)parseAnnotationField(root,"ANN",ANN_FIELDS[],ANN_TYPES[]),"2");
    // writeln(idx.fields["/INFO/ANN/effect"]);
    // writeln(idx.fields["/INFO/ANN/effect"].hashmap.keys.map!(x=>deserialize!string(x)));
    // writeln(idx.fields["/INFO/ANN/effect"].filter(["missense_variant"]));
}
