module libmucor.invertedindex;
public import libmucor.invertedindex.store;
public import libmucor.invertedindex.metadata;
public import libmucor.invertedindex.binaryindex;

import std.algorithm.setops;
import std.algorithm : sort, uniq, map, std_filter = filter, joiner, each,
    cartesianProduct, reduce;
import std.range : iota, takeExactly;
import std.array : array, replace;
import std.conv : to;
import std.format : format;
import std.string : indexOf;
import std.stdio;
import std.file : exists;
import std.exception : enforce;

import asdf : deserializeAsdf = deserialize, Asdf, AsdfNode, parseJson, serializeToAsdf;
import libmucor.wideint : uint128;
import libmucor.jsonlops.jsonvalue;
import libmucor.query.eval;

import libmucor.khashl;
import std.digest.md : MD5Digest, toHexString;
import libmucor.error;
import libmucor.query;
import std.sumtype;
import std.typecons: Tuple;
import std.parallelism;

char sep = '/';

struct InvertedIndex
{
    BinaryIndexReader* bidxReader;
    BinaryIndexWriter* bidxWriter;

    khashlSet!(const(char)[]) * fields;
    this(string prefix, bool write, ulong cacheSize = 8192, ulong smallsMax = 128)
    {
        if (!write)
        {
            this.bidxReader = new BinaryIndexReader(prefix);
        }
        else
        {
            import std.file: mkdirRecurse, exists;
            if(!prefix.exists)
                mkdirRecurse(prefix);
            this.bidxWriter = new BinaryIndexWriter(prefix, cacheSize, smallsMax);
        }
    }

    auto addQueryFilter(string queryStr) {
        import std.array: split;
        assert(this.bidxWriter);
        auto q = Query(queryStr);
        this.fields = getQueryFields(q.expr, null);
        log_info(__FUNCTION__, "Indexing only fields specified in query: %s", (*this.fields).byKey.array);
        foreach (key; this.fields.byKey.array)
        {
            auto arr = key.split(sep);
            const(char)[] field;
            foreach(v; arr){
                field ~= v;
                this.fields.insert(field);
                field ~= sep;
            }
        }
        log_info(__FUNCTION__, "Indexing only fields specified in query: %s", (*this.fields).byKey.array);
    }

    void close()
    {
        if (this.bidxReader)
            this.bidxReader.close;
        if (this.bidxWriter)
            this.bidxWriter.close;
    }

    auto recordMd5s()
    {
        return this.bidxReader.sums;
    }
    
    void addJsonObject(Asdf root)
    {
        if(fields) addJsonObjectFieldKeys(root);
        else addJsonObjectAllKeys(root);
    }

    void addJsonObjectAllKeys(Asdf root)
    {
        import std.container : DList;
        import libmucor.invertedindex.queue;
        alias AsdfKeyValue = Tuple!(const(char)[], Asdf);
        Queue!(AsdfKeyValue) queue; 
        uint128 md5;

        if (root["md5"] == Asdf.init)
            log_err(__FUNCTION__, "record with no md5");
        auto m = root["md5"].deserializeAsdf!string;
        root["md5"].remove;
        md5.fromHexString(m);
        
        if(root.kind != Asdf.Kind.object) log_err(__FUNCTION__,  "Expected JSON Object not: %s", root.kind);
        foreach (kv; root.byKeyValue)
        {
            queue.push(AsdfKeyValue(kv.key, kv.value));
        }
        while(!queue.empty) {
            AsdfKeyValue val = queue.pop;
            // writeln(val[0]," ", val[1]);
            final switch(val[1].kind) {
                case Asdf.Kind.object:
                    foreach (kv; val[1].byKeyValue)
                    {
                        queue.push(AsdfKeyValue(val[0] ~ sep ~ kv.key,kv.value));
                    }
                    break;
                case Asdf.Kind.array:
                    foreach (e; val[1].byElement)
                    {
                        queue.push(AsdfKeyValue(val[0],e));
                    }
                    break;
                case Asdf.Kind.string:
                    this.bidxWriter.insert(val[0], JSONValue(val[1]));
                    break;
                case Asdf.Kind.number:
                    this.bidxWriter.insert(val[0], JSONValue(val[1]));
                    break;
                case Asdf.Kind.null_:
                    continue;
                case Asdf.Kind.true_:
                    this.bidxWriter.insert(val[0], JSONValue(true));
                    break;
                case Asdf.Kind.false_:
                    this.bidxWriter.insert(val[0], JSONValue(false));
                    break;
            }
        }
        assert(md5 != uint128(0));
        this.bidxWriter.sums.write(md5);
        this.bidxWriter.numSums++;
    }
    
    void addJsonObjectFieldKeys(Asdf root)
    {
        import std.container : DList;
        import libmucor.invertedindex.queue;
        alias AsdfKeyValue = Tuple!(const(char)[], Asdf);
        Queue!(AsdfKeyValue) queue; 
        uint128 md5;

        if (root["md5"] == Asdf.init)
            log_err(__FUNCTION__, "record with no md5");
        auto m = root["md5"].deserializeAsdf!string;
        root["md5"].remove;
        md5.fromHexString(m);
        if(root.kind != Asdf.Kind.object) log_err(__FUNCTION__,  "Expected JSON Object not: %s", root.kind);
        foreach (kv; root.byKeyValue)
        {
            queue.push(AsdfKeyValue(kv.key, kv.value));
        }
        while(!queue.empty) {
            AsdfKeyValue val = queue.pop;
            // writeln(val[0]," ", val[1]);
            if(!(val[0] in *fields)) continue;
            final switch(val[1].kind) {
                case Asdf.Kind.object:
                    foreach (kv; val[1].byKeyValue)
                    {
                        queue.push(AsdfKeyValue(val[0] ~ sep ~ kv.key,kv.value));
                    }
                    break;
                case Asdf.Kind.array:
                    foreach (e; val[1].byElement)
                    {
                        queue.push(AsdfKeyValue(val[0],e));
                    }
                    break;
                case Asdf.Kind.string:
                    this.bidxWriter.insert(val[0], JSONValue(val[1]));
                    break;
                case Asdf.Kind.number:
                    this.bidxWriter.insert(val[0], JSONValue(val[1]));
                    break;
                case Asdf.Kind.null_:
                    continue;
                case Asdf.Kind.true_:
                    this.bidxWriter.insert(val[0], JSONValue(true));
                    break;
                case Asdf.Kind.false_:
                    this.bidxWriter.insert(val[0], JSONValue(false));
                    break;
            }
        }
        assert(md5 != uint128(0));
        this.bidxWriter.sums.write(md5);
        this.bidxWriter.numSums++;
    }

    khashlSet!(ulong) * allIds()
    {
        auto ret = new khashlSet!(ulong);
        foreach (k; iota(0, this.bidxReader.sums.length))
        {
            ret.insert(k);
        }
        return ret;
        // return this.recordMd5s;
    }

    uint128[] getFields(const(char)[] key)
    {
        import std.regex : matchFirst, regex;

        auto keycopy = key.idup;
        uint128[] ret;
        auto wildcard = key.indexOf('*');
        if (wildcard == -1)
        {
            auto hash = getKeyHash(key);
            // writefln("%x",hash);
            if (!(hash in this.bidxReader.seenKeys))
                log_err(__FUNCTION__, " key %s is not found", key);
            ret = [hash];
            if (ret.length == 0)
            {
                log_warn(__FUNCTION__, "Warning: Key %s was not found in index!", keycopy);
            }
        }
        else
        {
            key = key.replace("*", ".*");
            auto reg = regex("^" ~ key ~ "$");
            ret = this.bidxReader
                .getKeysWithId
                .std_filter!(x => !(x[0].matchFirst(reg).empty))
                .map!(x => x[1])
                .array;
            if (ret.length == 0)
            {
                log_warn(__FUNCTION__,
                        "Warning: Key wildcards sequence %s matched no keys in index!", keycopy);
            }
        }
        log_debug(__FUNCTION__, "Key %s matched %d keys", keycopy, ret.length);
        return ret;
    }

    khashlSet!(ulong) * query(T)(const(char)[] key, T value)
    {
        auto matchingFields = getFields(key);
        auto matchingValues = this.bidxReader.jsonStore.filter([value]);
        return reduce!unionIds(
                new khashlSet!ulong,
                cartesianProduct(matchingFields, matchingValues).map!(x => combineHash(x[0], x[1]))
                    .map!(x => this.bidxReader.idCache.getIds(x))
            );
    }

    khashlSet!(ulong) * queryRange(T)(const(char)[] key, T first, T second)
    {
        auto matchingFields = getFields(key);
        auto matchingValues = this.bidxReader.jsonStore.filterRange([
            first, second
        ]);
        return reduce!unionIds(
                new khashlSet!ulong,
                cartesianProduct(matchingFields, matchingValues).map!(x => combineHash(x[0], x[1]))
                .map!(x => this.bidxReader.idCache.getIds(x))
            );
    }

    auto getMatchingValues(T)(T val, string op)
    {
        switch (op)
        {
        case ">=":
            return this.bidxReader.jsonStore.filterOp!(">=", T)(val);
        case ">":
            return this.bidxReader.jsonStore.filterOp!(">", T)(val);
        case "<":
            return this.bidxReader.jsonStore.filterOp!("<", T)(val);
        case "<=":
            return this.bidxReader.jsonStore.filterOp!("<=", T)(val);
        default:
            log_err(__FUNCTION__, "%s operator is not valid here", op);
            throw new Exception("An error has occured");
        }
    }

    khashlSet!(ulong) * queryOp(T)(const(char)[] key, T val, string op)
    {
        import std.traits : ReturnType;
        import std.algorithm : mean;

        log_info(__FUNCTION__, "fetching ids for query: %s %s %s", key, op, val.to!string);
        auto matchingFields = getFields(key).array;
        auto matchingValues = getMatchingValues(val, op).array;
        auto combinations = cartesianProduct(matchingFields, matchingValues).array;
        log_debug(__FUNCTION__, "fields (%d) and values (%d) collected: %d combinations", matchingFields.length, matchingValues.length, combinations.length);
        khashlSet!(ulong)*[] ids = new khashlSet!(ulong) * [combinations.length];
        foreach (i, comb; parallel(combinations))
        {
            ids[i] = this.bidxReader.idCache.getIds(combineHash(comb[0], comb[1]));
        }
        auto ret = taskPool.reduce!unionIds(ids,1);
        log_info(__FUNCTION__, "%d ids fetched for query: %s %s %s", ret.count, key, op, val.to!string);
        return ret;
    }


    khashlSet!(ulong) * queryNOT(khashlSet!(ulong) * values)
    {
        auto r = this.allIds;
        (*r) -= (*values);
        return r;
    }

    uint128[] convertIds(khashlSet!(ulong) * ids)
    {
        return ids.byKey.map!(x => this.bidxReader.sums[x]).array;
    }

    auto opBinaryRight(string op)(InvertedIndex lhs)
    {
        static if (op == "+")
        {
            InvertedIndex ret;
            ret.recordMd5s = this.recordMd5s.dup;
            ret.fields = this.fields.dup;
            foreach (kv; lhs.fields.byKeyValue())
            {
                auto v = kv.key in ret.fields;
                if (v)
                {
                    foreach (kv2; kv.value.hashmap.byKeyValue)
                    {
                        auto v2 = kv2.key in v.hashmap;
                        if (v2)
                        {
                            *v2 = *v2 ~ kv2.value;
                        }
                        else
                        {
                            v.hashmap[kv2.key] = kv2.value;
                        }
                    }
                }
                else
                {
                    ret.fields[kv.key] = kv.value;
                }
            }
            return ret;
        }
        else
            static assert(false, "Op not implemented");
    }
}

unittest
{
    import asdf;
    import libmucor.jsonlops.basic : spookyhashObject;
    import std.array : array;
    import htslib.hts_log;
    import std.file: mkdirRecurse;

    hts_set_log_level(htsLogLevel.HTS_LOG_DEBUG);
    {
        mkdirRecurse("/tmp/test_idx");
        auto idx = new InvertedIndex("/tmp/test_idx", true);
        idx.addJsonObject(`{"test":"hello", "test2":"foo","test3":1}`.parseJson.spookyhashObject);
        idx.addJsonObject(`{"test":"world", "test2":"bar","test3":2}`.parseJson.spookyhashObject);
        idx.addJsonObject(`{"test":"world", "test4":"baz","test3":3}`.parseJson.spookyhashObject);
        idx.close;
    }
    {
        auto idx = InvertedIndex("/tmp/test_idx", false);
        assert(idx.bidxReader.sums.length == 3);
        // writeln(idx.bidxReader.getKeysWithId.map!(x => x[0]));
        assert(idx.query("test2", "foo").byKey.array == [0]);
        assert(idx.queryRange("test3", 1, 3).byKey.array == [0, 1]);
        idx.close;
    }

}

unittest
{
    import asdf;
    import libmucor.jsonlops.basic : spookyhashObject;
    import std.array : array;
    import htslib.hts_log;

    hts_set_log_level(htsLogLevel.HTS_LOG_INFO);
    set_log_level(LogLevel.Info);
    {
        auto idx = new InvertedIndex("/tmp/test_idx", true);
        idx.addJsonObject(
                `{"test":"hello", "test2":{"foo": "bar"}, "test3":1}`.parseJson.spookyhashObject);
        idx.addJsonObject(
                `{"test":"world", "test2":"baz",          "test3":2}`.parseJson.spookyhashObject);
        idx.addJsonObject(
                `{"test":"worl",                          "test3":3, "test4":{"foo": "bar"}}`
                .parseJson.spookyhashObject);
        idx.addJsonObject(
                `{"test":"hi",                            "test3":4, "test4":"baz"}`
                .parseJson.spookyhashObject);
        idx.addJsonObject(
                `{"test":"bye",                           "test3":5, "test4":"bar"}`
                .parseJson.spookyhashObject);
        idx.addJsonObject(
                `{"test":"hello",                         "test3":6, "test4":{"foo": "baz"}}`
                .parseJson.spookyhashObject);
        idx.addJsonObject(`{"test":"hello",               "test3":7, "test4":{"foo": ["baz", "bar"]}}`
                .parseJson.spookyhashObject);
        idx.addJsonObject(
                `{"test":"hello world",                   "test3":8, "test4":{"foo": "?"}}`
                .parseJson.spookyhashObject);
        idx.close;
    }
    {
        auto q1 = Query("test = hello");
        auto q2 = Query("test = \"hello world\"");
        auto q3 = Query("test = (world | worl | hi | bye)");
        auto q4 = Query("test3 > 3");
        auto q5 = Query("test3 >= 3");
        auto q6 = Query("test3 <= 3");
        auto q7 = Query("test3 < 3");
        auto q8 = Query("test3 = 3..6");
        auto q9 = Query("test4/foo = (bar | baz)");
        auto q10 = Query("test4/foo = (bar & baz)");
        auto q11 = Query("test4* = bar");
        auto q12 = Query("test4* = baz");
        auto q13 = Query("test* = bar");
        auto q14 = Query("(test* = baz) & (test3 = 6..7)");
        auto q15 = Query("(test = (world | worl | hi | bye)) & (test4/foo = (bar & baz))");
        auto q16 = Query("(test = hello) & (test4/foo = (bar & baz))");

        auto idx = new InvertedIndex("/tmp/test_idx", false);

        assert(idx.bidxReader.sums.length == 8);

        assert(q1.evaluate(idx).byKey.array == [0, 6, 5]);
        assert(q2.evaluate(idx).byKey.array == [7]);
        assert(q3.evaluate(idx).byKey.array == [4, 2, 1, 3]);
        assert(q4.evaluate(idx).byKey.array == [7, 4, 6, 5, 3]);
        assert(q5.evaluate(idx).byKey.array == [7, 2, 4, 6, 5, 3]);
        assert(q6.evaluate(idx).byKey.array == [0, 2, 1]);
        assert(q7.evaluate(idx).byKey.array == [0, 1]);
        assert(q8.evaluate(idx).byKey.array == [2, 4, 3]);
        assert(q9.evaluate(idx).byKey.array == [2, 6, 5]);
        assert(q10.evaluate(idx).byKey.array == [6]);
        assert(q11.evaluate(idx).byKey.array == [2, 6, 4]);
        assert(q12.evaluate(idx).byKey.array == [3, 6, 5]);
        assert(q13.evaluate(idx).byKey.array == [0, 2, 4, 6]);
        assert(q14.evaluate(idx).byKey.array == [5]);
        assert(q15.evaluate(idx).byKey.array == []);
        assert(q16.evaluate(idx).byKey.array == [6]);
        idx.close;
    }

}


unittest
{
    import asdf;
    import libmucor.jsonlops.basic : spookyhashObject;
    import std.array : array;
    import htslib.hts_log;

    hts_set_log_level(htsLogLevel.HTS_LOG_INFO);
    set_log_level(LogLevel.Info);
    {
        auto idx = new InvertedIndex("/tmp/test_idx", true);
        idx.addJsonObject(`{"test1":0.4,      "test2":-1e-2, "test3":1}`.parseJson.spookyhashObject);
        idx.addJsonObject(`{"test1":0.1,      "test2":-1e-3, "test3":10}`.parseJson.spookyhashObject);
        idx.addJsonObject(`{"test1":0.8,      "test2":-1e-4, "test3":10000}`.parseJson.spookyhashObject);
        idx.addJsonObject(`{"test1":1.2,      "test2":-1e-5, "test3":2}`.parseJson.spookyhashObject);
        idx.addJsonObject(`{"test1":0.2,      "test2":-1e-6, "test3":40.0}`.parseJson.spookyhashObject);
        idx.addJsonObject(`{"test1":0.001,    "test2":-1e-7, "test3":42}`.parseJson.spookyhashObject);
        idx.addJsonObject(`{"test1":0.000001, "test2":-1e-8, "test3":50}`.parseJson.spookyhashObject);
        idx.addJsonObject(`{"test1":-0.2,     "test2":-1e-9, "test3":97}`.parseJson.spookyhashObject);
        idx.close;
    }
    {
        auto q1 = Query("test1 = 0.4");
        auto q2 = Query("test1 > 0.1");
        auto q3 = Query("test1 < 0.1");
        auto q4 = Query("test1 >= 0.1");
        auto q5 = Query("test1 <= 0.1");
        auto q6 = Query("test2 < -1e-2");
        auto q7 = Query("test2 > -1e-2");
        auto q8 = Query("test3 > 10");
        auto q9 = Query("test3 > 10.0");
        auto q10 = Query("test3 >= 10");
        auto q11 = Query("test3 >= 10.0");
        auto q12 = Query("test3 < 10");
        auto q13 = Query("test3 < 10.0");
        auto q14 = Query("test3 <= 10");
        auto q15 = Query("test3 <= 10.0");

        auto idx = new InvertedIndex("/tmp/test_idx", false);

        assert(idx.bidxReader.sums.length == 8);

        assert(q1.evaluate(idx).byKey.array == [0]);
        assert(q2.evaluate(idx).byKey.array == [0, 2, 4, 3]);
        assert(q3.evaluate(idx).byKey.array == [7, 6, 5]);
        assert(q4.evaluate(idx).byKey.array == [0, 2, 4, 1, 3]);
        assert(q5.evaluate(idx).byKey.array == [6, 1, 5, 7]);
        assert(q6.evaluate(idx).byKey.array == []);
        assert(q7.evaluate(idx).byKey.array == [2, 4, 6, 1, 5, 3, 7]);
        assert(q8.evaluate(idx).byKey.array == [2, 4, 6, 5, 7]);
        assert(q9.evaluate(idx).byKey.array == [2, 4, 6, 5, 7]);
        assert(q10.evaluate(idx).byKey.array == [2, 4, 6, 1, 5, 7]);
        assert(q11.evaluate(idx).byKey.array == [2, 4, 6, 1, 5, 7]);
        assert(q12.evaluate(idx).byKey.array == [0, 3]);
        assert(q13.evaluate(idx).byKey.array == [0, 3]);
        assert(q14.evaluate(idx).byKey.array == [0, 1, 3]);
        assert(q15.evaluate(idx).byKey.array == [0, 1, 3]);
        idx.close;
    }

}
