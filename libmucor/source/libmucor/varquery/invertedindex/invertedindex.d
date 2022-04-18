module libmucor.varquery.invertedindex.invertedindex;
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
import libmucor.varquery.invertedindex.singleindex;
import libmucor.varquery.invertedindex.jsonvalue;
import libmucor.varquery.invertedindex.binaryindex;
import libmucor.khashl: khashl;
import htslib.hts_endian;
import std.digest.md : MD5Digest, toHexString;

char sep = '/';

struct JSONInvertedIndex
{
    BinaryJsonInvertedIndex * bidx;
    uint128[] recordMd5s;
    khashl!(const(char)[], InvertedIndex) fields;
    this(string f){
        // read const sequence
        
        this.bidx = new BinaryJsonInvertedIndex(f);
        this.recordMd5s = bidx.sums;
        foreach(k; bidx.keyMetaData) {
            auto kv = k.deserialize_to_tuple(bidx.jsonKeyMetaData, bidx.keyData);
            InvertedIndex idx;
            foreach(fkv; kv.value) {
                auto kv2 = fkv.deserialize_to_tuple(bidx.data, bidx.fieldKeyData);
                idx.hashmap[kv2.key] = kv2.value;
            }
            this.fields[kv.key] = idx;
        }
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

    void writeToFile(File f){
        
        auto bidx = BinaryJsonInvertedIndex(this);
        bidx.writeToFile(f);
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
    assert(bidx.keyMetaDataLen == 4);
    assert(bidx.jsonKeyMetaDataLen == 8);
    assert(bidx.idDataLen == 9);
    idx = JSONInvertedIndex("/tmp/test.idx");

    assert(idx.recordMd5s.length == 3);
    writeln(idx.fields.byKey.array);
    assert(idx.fields.byKey.array == ["/test2", "/test4", "/test3", "/test"]);

    assert(idx.fields["/test2"].filter(["foo"]) == [0]);
    assert(idx.fields["/test3"].filterRange([1, 3]) == [1, 0]);
}
