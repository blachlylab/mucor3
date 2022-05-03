module libmucor.invertedindex.invertedindex;
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
import libmucor.invertedindex.binaryindex;
import libmucor.invertedindex.store;
import libmucor.khashl;
import std.digest.md : MD5Digest, toHexString;
import libmucor.error;
import libmucor.query;
import std.sumtype;
import std.typecons: Tuple;

char sep = '/';

struct InvertedIndex
{
    BinaryIndexReader* bidxReader;
    BinaryIndexWriter* bidxWriter;
    this(string prefix, bool write, ulong cacheSize = 8192, ulong smallsMax = 128)
    {
        // read const sequence
        if (!write)
        {
            this.bidxReader = new BinaryIndexReader(prefix);
        }
        else
        {
            this.bidxWriter = new BinaryIndexWriter(prefix, cacheSize, smallsMax);
            import std.file: mkdirRecurse, exists;
            if(!prefix.exists)
                mkdirRecurse(prefix);
        }
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

    // void addJsonObject(Asdf root, const(char)[] path = "", uint128 md5 = uint128(0))
    // {
    //     if (path == "")
    //     {
    //         if (root["md5"] == Asdf.init)
    //             log_err(__FUNCTION__, "record with no md5");
    //         auto m = root["md5"].deserializeAsdf!string;
    //         root["md5"].remove;
    //         md5.fromHexString(m);
    //     }
    //     foreach (key, value; root.byKeyValue)
    //     {
    //         JSONValue valkey;
    //         if (value.kind == Asdf.Kind.object)
    //         {
    //             addJsonObject(value, path ~ sep ~ key);
    //             continue;
    //         }
    //         else if (value.kind == Asdf.Kind.array)
    //         {
    //             addJsonArray(value, path ~ sep ~ key);
    //             continue;
    //         }
    //         else if (value.kind == Asdf.Kind.null_)
    //         {
    //             continue;
    //         }
    //         else
    //         {
    //             valkey = JSONValue(value);
    //         }
    //         this.bidxWriter.insert(path ~ sep ~ key, valkey);
    //     }
    //     if (path == "")
    //     {
    //         assert(md5 != uint128(0));
    //         this.bidxWriter.sums.write(md5);
    //         this.bidxWriter.numSums++;
    //     }
    // }

    // void addJsonArray(Asdf root, const(char)[] path)
    // {
    //     assert(path != "");
    //     foreach (value; root.byElement)
    //     {
    //         JSONValue valkey;
    //         if (value.kind == Asdf.Kind.object)
    //         {
    //             addJsonObject(value, path);
    //             continue;
    //         }
    //         else if (value.kind == Asdf.Kind.array)
    //         {
    //             addJsonArray(value, path);
    //             continue;
    //         }
    //         else if (value.kind == Asdf.Kind.null_)
    //         {
    //             continue;
    //         }
    //         else
    //         {
    //             valkey = JSONValue(value);
    //         }
    //         this.bidxWriter.insert(path, valkey);
    //     }

    // }
    
    void addJsonObject(Asdf root)
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
        
        queue.push(AsdfKeyValue("",root));
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

    khashlSet!(ulong) allIds()
    {
        khashlSet!(ulong) ret;
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
        if (key[0] != '/')
            throw new Exception("key is missing leading /");
        uint128[] ret;
        auto wildcard = key.indexOf('*');
        if (wildcard == -1)
        {
            auto hash = getKeyHash(key);
            // writefln("%x",hash);
            if (!(hash in this.bidxReader.seenKeys))
                throw new Exception(" key " ~ key.idup ~ " is not found");
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

    khashlSet!(ulong) query(T)(const(char)[] key, T value)
    {
        auto matchingFields = getFields(key);
        auto matchingValues = this.bidxReader.jsonStore.filter([value]);
        return cartesianProduct(matchingFields, matchingValues).map!(x => combineHash(x[0], x[1]))
            .map!(x => this.bidxReader.idCache.getIds(x))
            .reduce!((a, b) => a | b);
    }

    khashlSet!(ulong) queryRange(T)(const(char)[] key, T first, T second)
    {
        auto matchingFields = getFields(key);
        auto matchingValues = this.bidxReader.jsonStore.filterRange([
            first, second
        ]);
        return cartesianProduct(matchingFields, matchingValues).map!(x => combineHash(x[0], x[1]))
            .map!(x => this.bidxReader.idCache.getIds(x))
            .reduce!((a, b) => a | b);
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

    khashlSet!(ulong) queryOp(T)(const(char)[] key, T val, string op)
    {
        import std.traits : ReturnType;

        auto matchingFields = getFields(key);
        auto matchingValues = getMatchingValues(val, op);

        return cartesianProduct(matchingFields, matchingValues).map!(x => combineHash(x[0], x[1]))
            .map!(x => this.bidxReader.idCache.getIds(x))
            .reduce!((a, b) => a | b);
    }

    ulong[] queryAND(T)(const(char)[] key, T[] values)
    {
        auto matchingFields = getFields(key);
        auto matchingValues = this.bidxReader.jsonStore.filter(values);
        return reduce!((a, b) => setIntersection(a, b).array)(cartesianProduct(matchingFields,
                matchingValues).map!(x => combineHash(x[0], x[1]))
                .map!(x => this.bidxReader.idCache.getIds(x).array.sort.array))
            .array.sort.uniq.array;
    }

    ulong[] queryOR(T)(const(char)[] key, T[] values)
    {
        auto matchingFields = getFields(key);
        auto matchingValues = this.bidxReader.jsonStore.filter(values);
        return cartesianProduct(matchingFields, matchingValues).map!(x => combineHash(x[0], x[1]))
            .map!(x => this.bidxReader.idCache.getIds(x))
            .joiner
            .array
            .sort
            .uniq
            .array;
    }

    khashlSet!(ulong) queryNOT(khashlSet!(ulong) values)
    {
        return allIds - values;
    }

    uint128[] convertIds(khashlSet!(ulong) ids)
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

khashlSet!(ulong) evaluateQuery(Query* query, InvertedIndex* idx, string lastKey = "")
{
    return (*query).match!((KeyValue x) {
        switch (x.op)
        {
        case ValueOp.Equal:
            return queryValue(x.lhs, x.rhs, idx);
        case ValueOp.ApproxEqual:
            return queryValue(x.lhs, x.rhs, idx);
        default:
            return queryOpValue(x.lhs, x.rhs, idx, cast(string) x.op);
        }
    }, (UnaryKeyOp x) {
        final switch (x.op)
        {
        case KeyOp.Exists:
            return khashlSet!(ulong)(); /// TODO: complete
        }
    }, (NotValue x) {
        if (lastKey == "")
            log_err(__FUNCTION__, "Key cannot be null");
        return idx.queryNOT(queryValue(lastKey, x.value, idx));
    }, (Value x) {
        if (lastKey == "")
            log_err(__FUNCTION__, "Key cannot be null");
        return queryValue(lastKey, x, idx);
    }, (ComplexKeyValue x) {
        switch (x.op)
        {
        case ValueOp.Equal:
            return evaluateQuery(x.rhs, idx, x.lhs);
        default:
            log_err(__FUNCTION__, "%s operator not allowed here: %s",
                cast(string) x.op, queryToString(*query));
            return khashlSet!(ulong)();
        }
    }, (NotQuery x) => idx.queryNOT(evaluateQuery(x.rhs, idx, lastKey)), (ComplexQuery x) {
        switch (x.op)
        {
        case LogicalOp.And:
            return evaluateQuery(x.rhs, idx,
                lastKey) & evaluateQuery(x.lhs, idx, lastKey);
        case LogicalOp.Or:
            return evaluateQuery(x.rhs, idx,
                lastKey) | evaluateQuery(x.lhs, idx, lastKey);
        default:
            log_err(__FUNCTION__, "%s operator not allowed here: %s",
                cast(string) x.op, queryToString(*query));
            return khashlSet!(ulong)();
        }
    }, (Subquery x) => evaluateQuery(x.subquery, idx, lastKey),);
}

khashlSet!(ulong) queryValue(const(char)[] key, Value value, InvertedIndex* idx)
{

    return (*value.expr).match!((bool x) => idx.query(key, x),
            (long x) => idx.query(key, x), (double x) => idx.query(key, x),
            (string x) => idx.query(key, x),
            (DoubleRange x) => idx.queryRange(key, x[0], x[1]),
            (LongRange x) => idx.queryRange(key, x[0], x[1]),);
}

khashlSet!(ulong) queryOpValue(const(char)[] key, Value value, InvertedIndex* idx, string op)
{
    alias f = tryMatch!((long x) { return idx.queryOp!long(key, x, op); }, (double x) {
        return idx.queryOp!double(key, x, op);
    });
    return f(*value.expr);
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
        writeln(idx.bidxReader.getKeysWithId.map!(x => x[0]));
        assert(idx.query("/test2", "foo").byKey.array == [0]);
        assert(idx.queryRange("/test3", 1, 3).byKey.array == [0, 1]);
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
        auto q1 = parseQuery("/test = hello");
        auto q2 = parseQuery("/test = \"hello world\"");
        auto q3 = parseQuery("/test = (world | worl | hi | bye)");
        auto q4 = parseQuery("/test3 > 3");
        auto q5 = parseQuery("/test3 >= 3");
        auto q6 = parseQuery("/test3 <= 3");
        auto q7 = parseQuery("/test3 < 3");
        auto q8 = parseQuery("/test3 = 3:6");
        auto q9 = parseQuery("/test4/foo = (bar | baz)");
        auto q10 = parseQuery("/test4/foo = (bar & baz)");
        auto q11 = parseQuery("/test4* = bar");
        auto q12 = parseQuery("/test4* = baz");
        auto q13 = parseQuery("/test* = bar");
        auto q14 = parseQuery("(/test* = baz) & (/test3 = 6:7)");
        auto q15 = parseQuery("(/test = (world | worl | hi | bye)) & (/test4/foo = (bar & baz))");
        auto q16 = parseQuery("(/test = hello) & (/test4/foo = (bar & baz))");

        auto idx = new InvertedIndex("/tmp/test_idx", false);

        assert(idx.bidxReader.sums.length == 8);
        assert(idx.bidxReader.idCache.smallsIds.getAll.array.length == 25);
        assert(idx.bidxReader.idCache.smallsMeta.getAll.array.length == 21);

        assert(evaluateQuery(q1, idx).byKey.array == [0, 6, 5]);
        assert(evaluateQuery(q2, idx).byKey.array == [7]);
        assert(evaluateQuery(q3, idx).byKey.array == [2, 4, 1, 3]);
        assert(evaluateQuery(q4, idx).byKey.array == [3, 4, 6, 5, 7]);
        assert(evaluateQuery(q5, idx).byKey.array == [3, 4, 2, 6, 5, 7]);
        assert(evaluateQuery(q6, idx).byKey.array == [2, 0, 1]);
        assert(evaluateQuery(q7, idx).byKey.array == [0, 1]);
        assert(evaluateQuery(q8, idx).byKey.array == [4, 2, 3]);
        assert(evaluateQuery(q9, idx).byKey.array == [2, 6, 5]);
        assert(evaluateQuery(q10, idx).byKey.array == [6]);
        assert(evaluateQuery(q11, idx).byKey.array == [4, 2, 6]);
        assert(evaluateQuery(q12, idx).byKey.array == [5, 6, 3]);
        assert(evaluateQuery(q13, idx).byKey.array == [0, 4, 2, 6]);
        assert(evaluateQuery(q14, idx).byKey.array == [5]);
        assert(evaluateQuery(q15, idx).byKey.array == []);
        assert(evaluateQuery(q16, idx).byKey.array == [6]);

        // assert(idx.query("/test2","foo").byKey.array == [0]);
        // assert(idx.queryRange("/test3", 1, 3).byKey.array == [0, 1]);
        idx.close;
    }

}
