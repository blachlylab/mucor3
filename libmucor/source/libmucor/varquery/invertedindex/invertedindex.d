module libmucor.varquery.invertedindex.invertedindex;
import std.algorithm.setops;
import std.regex;
import std.algorithm : sort, uniq, map, std_filter = filter, joiner, each, cartesianProduct, reduce;
import std.range : iota, takeExactly;
import std.array : array, replace;
import std.conv : to;
import std.format : format;
import std.string : indexOf;
import std.stdio;
import std.file: exists;
import std.exception : enforce;

import asdf: deserializeAsdf = deserialize, Asdf, AsdfNode, parseJson, serializeToAsdf;
import libmucor.wideint : uint128;
import libmucor.varquery.invertedindex.jsonvalue;
import libmucor.varquery.invertedindex.binaryindex;
import libmucor.varquery.invertedindex.store;
import libmucor.khashl: khashl;
import std.digest.md : MD5Digest, toHexString;
import htslib.hts_log;

char sep = '/';

struct InvertedIndex
{
    BinaryIndexReader * bidxReader;
    BinaryIndexWriter * bidxWriter;
    this(string prefix, bool write){
        // read const sequence
        if(!write){
            this.bidxReader = new BinaryIndexReader(prefix);
        } else {
            this.bidxWriter = new BinaryIndexWriter(prefix);
        }
    }

    void close() {
        if(this.bidxReader) this.bidxReader.close;
        if(this.bidxWriter) this.bidxWriter.close;
    }

    auto recordMd5s() {
        return this.bidxReader.sums;
    }

    void addJsonObject(Asdf root, const(char)[] path = "", uint128 md5 = uint128(0)){
        if(path == ""){
            if(root["md5"] == Asdf.init) hts_log_error(__FUNCTION__, "record with no md5");
            auto m = root["md5"].deserializeAsdf!string;
            root["md5"].remove;
            md5.fromHexString(m);
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
        if(path == ""){
            assert(md5 != uint128(0));
            this.bidxWriter.sums.write(md5);
            this.bidxWriter.numSums++;
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

    uint128[] getFields(const(char)[] key)
    {
        auto keycopy = key.idup;
        if(key[0] != '/') throw new Exception("key is missing leading /");
        uint128[] ret;
        auto wildcard = key.indexOf('*');
        if(wildcard == -1){
            auto hash = getKeyHash(key);
            if(!(hash in this.bidxReader.seenKeys)) throw new Exception(" key "~key.idup~" is not found");
            ret = [hash];
            if(ret.length == 0){
                hts_log_warning(__FUNCTION__,"Warning: Key"~ keycopy ~" was not found in index!");
            }
        }else{
            key = key.replace("*",".*");
            auto reg = regex("^" ~ key ~"$");
            ret = this.bidxReader.getKeysWithId.std_filter!(x => !(x[0].matchFirst(reg).empty)).map!(x=> x[1]).array;
            if(ret.length == 0){
                hts_log_warning(__FUNCTION__,"Warning: Key wildcards sequence "~ keycopy ~" matched no keys in index!");
            }
        }
        debug hts_log_debug(__FUNCTION__, format("Key %s matched %d keys",keycopy,ret.length));
        return ret;
    }

    ulong[] query(T)(const(char)[] key,T value) {
        auto matchingFields = getFields(key);
        auto matchingValues = this.bidxReader.jsonStore.filter([value]);
        return cartesianProduct(matchingFields, matchingValues)
            .map!(x => combineHash(x[0], x[1]))
            .map!( x => this.bidxReader.idCache.getIds(x))
            .joiner.array.sort.uniq.array;
    }
    ulong[] queryRange(T)(const(char)[] key,T first,T second){
        auto matchingFields = getFields(key);
        auto matchingValues = this.bidxReader.jsonStore.filterRange([first, second]);
        return cartesianProduct(matchingFields, matchingValues)
            .map!(x => combineHash(x[0], x[1]))
            .map!( x => this.bidxReader.idCache.getIds(x))
            .joiner.array.sort.uniq.array;
    }
    template queryOp(string op)
    {
        ulong[] queryOp(T)(const(char)[] key,T val){
            auto matchingFields = getFields(key);
            auto matchingValues = this.bidxReader.jsonStore.filterOp!(op, T)(val);
            return cartesianProduct(matchingFields, matchingValues)
                .map!(x => combineHash(x[0], x[1]))
                .map!( x => this.bidxReader.idCache.getIds(x))
                .joiner.array.sort.uniq.array;
        }
    }
    
    ulong[] queryAND(T)(const(char)[] key,T[] values){
        auto matchingFields = getFields(key);
        auto matchingValues = this.bidxReader.jsonStore.filter(values);
        return reduce!((a, b) => setIntersection(a, b).array)(
                cartesianProduct(matchingFields, matchingValues)
                    .map!(x => combineHash(x[0], x[1]))
                    .map!( x => this.bidxReader.idCache.getIds(x).array.sort.array)
            ).array.sort.uniq.array;
    }
    ulong[] queryOR(T)(const(char)[] key,T[] values){
        auto matchingFields = getFields(key);
        auto matchingValues = this.bidxReader.jsonStore.filter(values);
        return cartesianProduct(matchingFields, matchingValues)
            .map!(x => combineHash(x[0], x[1]))
            .map!( x => this.bidxReader.idCache.getIds(x))
            .joiner.array.sort.uniq.array;
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
        auto idx = new InvertedIndex("/tmp/test_idx",true);
        idx.addJsonObject(`{"test":"hello", "test2":"foo","test3":1}`.parseJson.md5sumObject);
        idx.addJsonObject(`{"test":"world", "test2":"bar","test3":2}`.parseJson.md5sumObject);
        idx.addJsonObject(`{"test":"world", "test4":"baz","test3":3}`.parseJson.md5sumObject);
        idx.close;
    }
    {
        auto idx = InvertedIndex("/tmp/test_idx",false);
        assert(idx.bidxReader.sums.length == 3);
        assert(idx.query("/test2","foo") == [0]);
        assert(idx.queryRange("/test3", 1, 3) == [0, 1]);
        idx.close;
    }

}
