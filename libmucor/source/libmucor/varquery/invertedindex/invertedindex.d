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
import std.file: exists;
import std.exception : enforce;

import asdf: deserializeAsdf = deserialize, Asdf, AsdfNode, parseJson, serializeToAsdf;
import libmucor.wideint : uint128;
import libmucor.varquery.invertedindex.jsonvalue;
import libmucor.varquery.invertedindex.binaryindex;
import libmucor.varquery.invertedindex.store;
import libmucor.khashl: khashl;
import htslib.hts_endian;
import std.digest.md : MD5Digest, toHexString;
import htslib.hts_log;

char sep = '/';

struct InvertedIndex
{
    BinaryIndexReader bidxReader;
    BinaryIndexWriter bidxWriter;
    this(string prefix, bool write){
        // read const sequence
        if(!write){
            this.bidxReader = BinaryIndexReader(prefix);
        } else {
            this.bidxWriter = BinaryIndexWriter(prefix);
        }
    }

    void addJsonObject(Asdf root, const(char)[] path = ""){
        if(path == ""){
            uint128 a;
            debug if(root["md5"] == Asdf.init) stderr.writeln("record with no md5");
            auto md5 = root["md5"].deserializeAsdf!string;
            root["md5"].remove;
            a.fromHexString(md5);
            this.bidxWriter.sums.write(a);
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
            this.bidxWriter.insert(path~sep~key, valkey);
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
            this.bidxWriter.insert(path, valkey);
        }
        
    }

    ulong[] allIds(){
        return iota(0, this.bidxReader.sums.length).array;
        // return this.recordMd5s;
    }

    JsonStoreReader*[] getFields(const(char)[] key)
    {
        auto keycopy = key.idup;
        if(key[0] != '/') throw new Exception("key is missing leading /");
        JsonStoreReader*[] ret;
        auto wildcard = key.indexOf('*');
        if(wildcard == -1){
            auto hash = getKeyHash(key);
            auto p = hash in this.bidxReader.hashmap;
            if(!p) throw new Exception(" key "~key.idup~" is not found");
            ret = [p];
            if(ret.length == 0){
                hts_log_warning(__FUNCTION__,"Warning: Key"~ keycopy ~" was not found in index!");
            }
        }else{
            key = key.replace("*",".*");
            auto reg = regex("^" ~ key ~"$");
            ret = this.bidxReader.getKeysWithJsonStore.std_filter!(x => !(x[0].matchFirst(reg).empty)).map!(x=> x[1]).array;
            if(ret.length == 0){
                hts_log_warning(__FUNCTION__,"Warning: Key wildcards sequence "~ keycopy ~" matched no keys in index!");
            }
        }
        debug hts_log_debug(__FUNCTION__, format("Key %s matched %d keys",keycopy,ret.length));
        return ret;
    }

    ulong[] query(T)(const(char)[] key,T value) {
        auto matchingFields = getFields(key);
        return matchingFields
                .map!(x=> (*x).filter([value]))
                .joiner.array.sort.uniq.array;
    }
    ulong[] queryRange(T)(const(char)[] key,T first,T second){
        auto matchingFields = getFields(key);
        return matchingFields
                .map!(x=> (*x).filterRange([first,second]))
                .joiner.array.sort.uniq.array;
    }
    template queryOp(string op)
    {
        ulong[] queryOp(T)(const(char)[] key,T val){
            auto matchingFields = getFields(key);
            return matchingFields
                    .map!(x=> (*x).filterOp!(op, T)(val))
                    .joiner.array.sort.uniq.array;
        }
    }
    
    ulong[] queryAND(T)(const(char)[] key,T[] values){
        auto matchingFields = getFields(key);
        return matchingFields.map!((x){
            auto results = values.map!(y=>(*x).filter([y]).sort.uniq.array).array;
            auto intersect = results[0];
            foreach (item; results)
                intersect = setIntersection(intersect,item).array;
            return intersect.array;
        }).joiner.array.sort.uniq.array;
    }
    ulong[] queryOR(T)(const(char)[] key,T[] values){
        auto matchingFields = getFields(key);
        return matchingFields.map!(x=> (*x).filter(values)).joiner.array.sort.uniq.array;
    }
    ulong[] queryNOT(ulong[] values){
        return allIds.sort.uniq.setDifference(values).array;
    }

    uint128[] convertIds(ulong[] ids)
    {
        return ids.map!(x => this.bidxReader.sums[x]).array;
    }

    auto opBinaryRight(string op)(InvertedIndex lhs)
    {
        static if(op == "+") {
            InvertedIndex ret;
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
    import htslib.hts_log;
    hts_set_log_level(htsLogLevel.HTS_LOG_DEBUG);
    {
        auto idx = InvertedIndex("/tmp/test_index",true);
        idx.addJsonObject(`{"test":"hello", "test2":"foo","test3":1}`.parseJson.md5sumObject);
        idx.addJsonObject(`{"test":"world", "test2":"bar","test3":2}`.parseJson.md5sumObject);
        idx.addJsonObject(`{"test":"world", "test4":"baz","test3":3}`.parseJson.md5sumObject);
    }

    auto idx = InvertedIndex("/tmp/test_index",false);
    assert(idx.bidxReader.sums.length == 3);
    writeln(idx.getFields("/*").map!(x => x.getJsonValues));
    writeln(idx.query("/test2","foo"));
    assert(idx.query("/test2","foo") == [0]);
    assert(idx.queryRange("/test3", 1, 3) == [1, 0]);

}
