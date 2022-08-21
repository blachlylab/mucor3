module libmucor.invertedindex;

import libmucor.invertedindex.record;
import libmucor.invertedindex.store;
import libmucor.serde;
import libmucor.khashl;
import libmucor.error;
import libmucor.query;

import std.algorithm : map, filter, reduce;
import std.array : array, replace;
import std.conv : to;
import std.string : indexOf;
import std.stdio;
import std.parallelism;
import std.algorithm : balancedParens;
import std.datetime.stopwatch : StopWatch;
import std.container.array;

import mir.ion.value;

char sep = '/';

struct InvertedIndex
{
    InvertedIndexStore store;

    this(string prefix)
    {
        this.store = InvertedIndexStore(prefix);
    }

    void insert(const(char[])[] symbolTable, IonDescribedValue val)
    {
        assert(val.descriptor.type == IonTypeCode.struct_);
        IonStruct obj;
        val.get(obj);
        this.store.insert(obj.withSymbols(symbolTable));
    }

    void insert(ref VcfIonRecord rec)
    {
        this.store.insert(rec);
    }

    khashlSet!(uint128)* allIds()
    {
        return this.store.records.byKeyValue.map!(x => x[0]).collect;
        // return this.recordMd5s;
    }

    auto convertIdsToIon(khashlSet!(uint128)* ids)
    {
        return this.store.getIonObjects(ids.byKey());
    }

    const(char[])[] getFields(const(char)[] key)
    {
        import std.regex : matchFirst, regex;

        auto keycopy = key.idup;
        const(char[])[] ret;
        auto wildcard = key.indexOf('*');
        auto keys = this.store.getIonKeys;
        if (wildcard == -1)
        {
            if (!(key in *keys))
            {
                return [];
            }
            else
            {
                ret = [key];
            }
        }
        else
        {
            key = key.replace("*", ".*");
            auto reg = regex("^" ~ key ~ "$");
            ret = keys.byKey.filter!(x => !(x[].matchFirst(reg).empty)).map!(x => x[].dup).array;
            if (ret.length == 0)
            {
                log_warn(__FUNCTION__,
                        "Warning: Key wildcards sequence %s matched no keys in index!", keycopy);
            }
        }
        log_debug(__FUNCTION__, "Key %s matched %d keys", keycopy, ret.length);
        return ret;
    }

    khashlSet!(uint128)* query(T)(const(char)[] key, T value)
    {
        auto matchingFields = getFields(key);
        return reduce!unionIds(new khashlSet!uint128,
                matchingFields.map!(x => this.store.filterSingle(x, value))
                .filter!(x => !x.isNone)
                .map!(x => x.unwrap.collect()));
    }

    khashlSet!(uint128)* queryRange(T)(const(char)[] key, T first, T second)
    {
        auto matchingFields = getFields(key);
        return reduce!unionIds(new khashlSet!uint128,
                matchingFields.map!(x => this.store.filterRange(x, [
                        first, second
                    ]).collect()));
    }

    auto getMatchingValues(T)(const(char)[] key, T val, string op)
    {
        switch (op)
        {
        case ">=":
            return this.store.filterOp!(">=", T)(key, val);
        case ">":
            return this.store.filterOp!(">", T)(key, val);
        case "<":
            return this.store.filterOp!("<", T)(key, val);
        case "<=":
            return this.store.filterOp!("<=", T)(key, val);
        default:
            log_err(__FUNCTION__, "%s operator is not valid here", op);
            throw new Exception("An error has occured");
        }
    }

    khashlSet!(uint128)* queryOp(T)(const(char)[] key, T val, string op)
    {
        import std.traits : ReturnType;
        import std.algorithm : mean;

        log_info(__FUNCTION__, "fetching ids for query: %s %s %s", key, op, val.to!string);
        auto matchingFields = getFields(key).array;
        khashlSet!(uint128)*[] ids = new khashlSet!(uint128)*[matchingFields.length];
        foreach (i, kh; parallel(matchingFields))
        {
            ids[i] = this.getMatchingValues(kh, val, op).collect;
        }
        auto ret = taskPool.reduce!unionIds(ids, 1);
        log_info(__FUNCTION__, "%d ids fetched for query: %s %s %s", ret.count,
                key, op, val.to!string);
        return ret;
    }

    khashlSet!(uint128)* queryNOT(khashlSet!(uint128)* values)
    {
        auto r = this.allIds;
        (*r) -= (*values);
        return r;
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
    import std.array : array;
    import htslib.hts_log;
    import std.file : mkdirRecurse;
    import mir.ion.conv;
    import mir.ion.stream;
    import std.path;
    import std.file;

    auto dbname = "/tmp/test_idx_1";
    if (dbname.exists)
        rmdirRecurse(dbname);

    uint128[] checksums;
    ubyte[] data;
    hts_set_log_level(htsLogLevel.HTS_LOG_DEBUG);
    {
        auto idx = InvertedIndex(dbname);
        checksums ~= uint128.fromHexString("271eea785e564a5d8c8099556c93b5a4");
        checksums ~= uint128.fromHexString("7dc8741714834d2b9fffdb315e07b6bd");
        checksums ~= uint128.fromHexString("68c07c663e854421bccd1dbe8fe6a388");
        data ~= cast(ubyte[])(
                `{test:"hello",test2:"foo",test3:1,checksum:` ~ checksums[0].toString ~ `}`)
            .text2ion;
        data ~= cast(ubyte[])(
                `{test:"world",test2:"bar",test3:2,checksum:` ~ checksums[1].toString ~ `}`)
            .text2ion;
        data ~= cast(ubyte[])(
                `{test:"world",test4:"baz",test3:3,checksum:` ~ checksums[2].toString ~ `}`)
            .text2ion;
        foreach (symTable, val; IonValueStream(data))
        {
            idx.insert(symTable, val);
        }
    }
    {
        auto idx = InvertedIndex(dbname);
        idx.store.print;
        assert(idx.query("test2", "foo").byKey.array == [checksums[0]]);
        assert(idx.queryRange("test3", 1, 3).byKey.array == [
                checksums[0], checksums[1]
                ]);
    }

}

unittest
{
    import asdf;
    import std.array : array;
    import htslib.hts_log;
    import mir.ion.conv;
    import mir.ion.stream;
    import std.path;
    import std.file;

    auto dbname = "/tmp/test_idx_2";
    if (dbname.exists)
        rmdirRecurse(dbname);

    uint128[] checksums;
    ubyte[] data;
    hts_set_log_level(htsLogLevel.HTS_LOG_INFO);
    set_log_level(LogLevel.Info);
    {
        checksums ~= uint128.fromHexString("271eea785e564a5d8c8099556c93b5a4");
        checksums ~= uint128.fromHexString("7dc8741714834d2b9fffdb315e07b6bd");
        checksums ~= uint128.fromHexString("68c07c663e854421bccd1dbe8fe6a388");
        checksums ~= uint128.fromHexString("47b7423e77814ab7b96d3cfa70ae6fa8");
        checksums ~= uint128.fromHexString("c4366b54a241429980ab6e6bd813f8d8");
        checksums ~= uint128.fromHexString("5edd465728d24f70b27540b3b24a97a3");
        checksums ~= uint128.fromHexString("c35c7fba59054a728ae54ea7ade538e5");
        checksums ~= uint128.fromHexString("239ef3ec18e848c38a2a8876993b8edf");

        data ~= cast(ubyte[])(
                `{test:"hello",test2:{foo:bar},test3:1,checksum:` ~ checksums[0].toString ~ `}`)
            .text2ion;
        data ~= cast(ubyte[])(
                `{test:"world",test2:baz,test3:2,checksum:` ~ checksums[1].toString ~ `}`).text2ion;
        data ~= cast(ubyte[])(
                `{test:"worl",test3:3,test4:{foo:bar},checksum:` ~ checksums[2].toString ~ `}`)
            .text2ion;
        data ~= cast(ubyte[])(`{test:"hi",test3:4,test4:baz,checksum:` ~ checksums[3].toString ~ `}`)
            .text2ion;
        data ~= cast(ubyte[])(`{test:"bye",test3:5,test4:bar,checksum:` ~ checksums[4].toString
                ~ `}`).text2ion;
        data ~= cast(ubyte[])(
                `{test:"hello",test3:6,test4:{foo:baz},checksum:` ~ checksums[5].toString ~ `}`)
            .text2ion;
        data ~= cast(ubyte[])(
                `{test:"hello",test3:7,test4:{foo:[baz,bar]},checksum:` ~ checksums[6].toString
                ~ `}`).text2ion;
        data ~= cast(ubyte[])(
                `{test:"hello world",test3:8,test4:{foo:"?"},checksum:` ~ checksums[7].toString
                ~ `}`).text2ion;

        auto idx = InvertedIndex(dbname);
        foreach (symTable, val; IonValueStream(data))
        {
            idx.insert(symTable, val);
        }
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

        auto idx = InvertedIndex(dbname);
        auto a = q1.evaluate(idx);
        auto b = [checksums[0], checksums[6], checksums[5]].collect;
        foreach (key; a.byKey)
        {
            if (!(key in *b))
                writeln("notfound:", key);
        }
        foreach (key; b.byKey)
        {
            if (!(key in *a))
                writeln("notfound:", key);
        }
        writeln((q1.evaluate(idx)).byKey.array);
        assert(*(q1.evaluate(idx)) == *([
                checksums[0], checksums[6], checksums[5]
                ].collect));
        assert(*q2.evaluate(idx) == *([checksums[7]].collect));
        assert(*q3.evaluate(idx) == *([
                checksums[4], checksums[2], checksums[1], checksums[3]
                ].collect));
        assert(*q4.evaluate(idx) == *([
                checksums[7], checksums[4], checksums[6], checksums[5],
                checksums[3]
                ].collect));
        assert(*q5.evaluate(idx) == *([
                checksums[7], checksums[2], checksums[4], checksums[6],
                checksums[5], checksums[3]
                ].collect));
        assert(*q6.evaluate(idx) == *([checksums[0], checksums[2], checksums[1]].collect));
        assert(*q7.evaluate(idx) == *([checksums[0], checksums[1]].collect));
        assert(*q8.evaluate(idx) == *([checksums[2], checksums[4], checksums[3]].collect));
        assert(*q9.evaluate(idx) == *([checksums[2], checksums[6], checksums[5]].collect));
        assert(*q10.evaluate(idx) == *([checksums[6]].collect));
        assert(*q11.evaluate(idx) == *([
                checksums[2], checksums[6], checksums[4]
                ].collect));
        assert(*q12.evaluate(idx) == *([
                checksums[3], checksums[6], checksums[5]
                ].collect));
        assert(*q13.evaluate(idx) == *([
                checksums[0], checksums[2], checksums[4], checksums[6]
                ].collect));
        assert(*q14.evaluate(idx) == *([checksums[5]].collect));
        assert(*q15.evaluate(idx) == *(new khashlSet!uint128()));
        assert(*q16.evaluate(idx) == *([checksums[6]].collect));
    }

}

unittest
{
    import asdf;
    import std.array : array;
    import htslib.hts_log;
    import mir.ion.conv;
    import mir.ion.stream;
    import std.path;
    import std.file;

    auto dbname = "/tmp/test_idx_3";
    if (dbname.exists)
        rmdirRecurse(dbname);

    uint128[] checksums;
    ubyte[] data;
    hts_set_log_level(htsLogLevel.HTS_LOG_INFO);
    set_log_level(LogLevel.Info);
    {
        checksums ~= uint128.fromHexString("271eea785e564a5d8c8099556c93b5a4");
        checksums ~= uint128.fromHexString("7dc8741714834d2b9fffdb315e07b6bd");
        checksums ~= uint128.fromHexString("68c07c663e854421bccd1dbe8fe6a388");
        checksums ~= uint128.fromHexString("47b7423e77814ab7b96d3cfa70ae6fa8");
        checksums ~= uint128.fromHexString("c4366b54a241429980ab6e6bd813f8d8");
        checksums ~= uint128.fromHexString("5edd465728d24f70b27540b3b24a97a3");
        checksums ~= uint128.fromHexString("c35c7fba59054a728ae54ea7ade538e5");
        checksums ~= uint128.fromHexString("239ef3ec18e848c38a2a8876993b8edf");

        auto idx = InvertedIndex(dbname);
        data ~= cast(ubyte[])(
                `{test1:0.4,test2:-1e-2,test3:1,checksum:` ~ checksums[0].toString ~ `}`).text2ion;
        data ~= cast(ubyte[])(
                `{test1:0.1,test2:-1e-3,test3:10,checksum:` ~ checksums[1].toString ~ `}`).text2ion;
        data ~= cast(ubyte[])(
                `{test1:0.8,test2:-1e-4,test3:10000,checksum:` ~ checksums[2].toString ~ `}`)
            .text2ion;
        data ~= cast(ubyte[])(
                `{test1:1.2,test2:-1e-5,test3:2,checksum:` ~ checksums[3].toString ~ `}`).text2ion;
        data ~= cast(ubyte[])(
                `{test1:0.2,test2:-1e-6,test3:40.0,checksum:` ~ checksums[4].toString ~ `}`)
            .text2ion;
        data ~= cast(ubyte[])(
                `{test1:0.001,test2:-1e-7,test3:42,checksum:` ~ checksums[5].toString ~ `}`)
            .text2ion;
        data ~= cast(ubyte[])(
                `{test1:0.000001,test2:-1e-8,test3:50,checksum:` ~ checksums[6].toString ~ `}`)
            .text2ion;
        data ~= cast(ubyte[])(
                `{test1:-0.2,test2:-1e-9,test3:97,checksum:` ~ checksums[7].toString ~ `}`)
            .text2ion;
        foreach (symTable, val; IonValueStream(data))
        {
            idx.insert(symTable, val);
        }
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

        auto idx = InvertedIndex(dbname);
        import std.algorithm : map, countUntil;

        assert(*q1.evaluate(idx) == *([checksums[0]].collect));
        assert(*q2.evaluate(idx) == *([
                checksums[0], checksums[2], checksums[4], checksums[3]
                ].collect));
        assert(*q3.evaluate(idx) == *([checksums[7], checksums[6], checksums[5]].collect));
        assert(*q4.evaluate(idx) == *([
                checksums[0], checksums[2], checksums[4], checksums[1],
                checksums[3]
                ].collect));
        assert(*q5.evaluate(idx) == *([
                checksums[6], checksums[1], checksums[5], checksums[7]
                ].collect));
        assert(*q6.evaluate(idx) == *([
                checksums[7], checksums[5], checksums[1], checksums[6],
                checksums[4], checksums[2], checksums[3]
                ].collect));
        assert(*q7.evaluate(idx) == *(new khashlSet!uint128()));
        assert(*q8.evaluate(idx) == *([
                checksums[2], checksums[4], checksums[6], checksums[5],
                checksums[7]
                ].collect));
        assert(*q9.evaluate(idx) == *([
                checksums[2], checksums[4], checksums[6], checksums[5],
                checksums[7]
                ].collect));
        assert(*q10.evaluate(idx) == *([
                checksums[2], checksums[4], checksums[6], checksums[1],
                checksums[5], checksums[7]
                ].collect));
        assert(*q11.evaluate(idx) == *([
                checksums[2], checksums[4], checksums[6], checksums[1],
                checksums[5], checksums[7]
                ].collect));
        assert(*q12.evaluate(idx) == *([checksums[0], checksums[3]].collect));
        assert(*q13.evaluate(idx) == *([checksums[0], checksums[3]].collect));
        assert(*q14.evaluate(idx) == *([
                checksums[0], checksums[1], checksums[3]
                ].collect));
        assert(*q15.evaluate(idx) == *([
                checksums[0], checksums[1], checksums[3]
                ].collect));
    }

    import std.file;

    rmdirRecurse("/tmp/test_idx_3");

}

auto query(string outfn, ref InvertedIndex idx, string queryStr)
{
    StopWatch sw;
    sw.start;
    if (!queryStr.balancedParens('(', ')'))
    {
        log_err(__FUNCTION__, "Parentheses aren't matched in query: %s", queryStr);
    }
    auto q = Query(queryStr);
    log_info(__FUNCTION__, "Time to parse query: %s usecs", sw.peek.total!"usecs");
    sw.reset;
    auto idxs = q.evaluate(idx);
    log_info(__FUNCTION__, "Time to evaluate query: %s seconds", sw.peek.total!"seconds");
    sw.stop;
    log_info(__FUNCTION__, "%d records matched your query", idxs.count);
    SymbolTable table;
    auto tdata = idx.store.getSharedSymbolTable.unwrap.unwrap;
    auto tarr = cast(const(ubyte)[])tdata[];
    table.loadSymbolTable(tarr);
    auto serializer = VcfSerializer(outfn, cast(string[]) table.table[10 .. $], SerdeTarget.ion);
    foreach (d; idx.convertIdsToIon(idxs))
    {
        serializer.putData(d[]);
    }
}

void index(ref VcfIonDeserializer range, string prefix)
{
    import core.atomic : atomicOp;

    InvertedIndex idx = InvertedIndex(prefix);
    StopWatch sw;
    sw.start;
    shared(ulong) count;
    foreach (rec; parallel(range))
    {
        auto r = rec.unwrap;
        idx.insert(r);
        r.deallocate;
        count.atomicOp!"+="(1);
    }
    idx.store.storeSharedSymbolTable(range.symbols);
    // assert(count == idx.recordMd5s.length,"number of md5s doesn't match number of records");

    sw.stop;
    log_info(__FUNCTION__, "Indexed %d records in %d secs", count, sw.peek.total!"seconds");
    log_info(__FUNCTION__, "Avg time to index record: %f usecs",
            float(sw.peek.total!"usecs") / float(count));
}

unittest
{
    import std.path;
    import std.file;
    import libmucor.serde;
    import libmucor.atomize;

    auto dbname = "/tmp/test2.ion_index";
    if (dbname.exists)
        rmdirRecurse(dbname);

    {
        parseVCF("test/data/vcf_file.vcf", -1, false, false, "/tmp/test2.ion");
    }

    {
        auto rdr = VcfIonDeserializer("/tmp/test2.ion");
        index(rdr, dbname);

        InvertedIndex idx = InvertedIndex(dbname);
        // idx.store.print;
        query("/tmp/test3.ion", idx, "QUAL > 60");
    }
    {
        auto rdr = VcfIonDeserializer("/tmp/test3.ion");

        foreach (rec; rdr)
        {
            // writeln(r.symbolTable);
            auto r = rec.unwrap;
            writeln(vcfIonToText(r.withSymbols(cast(const(char[])[])r.symbols.table[])));
            writeln(vcfIonToJson(r.withSymbols(cast(const(char[])[])r.symbols.table[])));
        }
    }
}
