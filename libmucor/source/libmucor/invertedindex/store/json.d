module libmucor.invertedindex.store.json;

import libmucor.invertedindex.store.binary;
import libmucor.jsonlops.jsonvalue;
import libmucor.invertedindex.metadata;
import libmucor.invertedindex.store : getKeyHash, getValueHash, getShortHash;
import libmucor.wideint;
import libmucor.khashl;
import std.file : exists;
import std.typecons : Tuple;
import libmucor.error;
import std.sumtype : match;
import std.format : format;
import std.algorithm : sort, uniq, map, std_filter = filter, canFind, joiner;
import std.range : chain, zip;
import std.traits;
import std.array : array;
import std.range : InputRange, inputRangeObject;

/// Stores json data and ids
/// for a given field
struct JsonStoreWriter
{
    string prefix;

    /// hashset of observed values
    khashlSet!(uint128) valuesSeen;

    /// store json value meta data
    JsonMetaStore* metadata;
    /// store integer json values
    LongStore* longs;
    /// store double json values
    DoubleStore* doubles;
    /// store strings json values
    StringStore* strings;

    this(string prefix)
    {
        this.prefix = prefix;
        this.metadata = new JsonMetaStore(prefix ~ ".json.meta", "wbu");
        this.longs = new LongStore(prefix ~ ".json.longs", "wbu");
        this.doubles = new DoubleStore(prefix ~ ".json.doubles", "wbu");
        this.strings = new StringStore(prefix ~ ".json.strings", "wbu");
    }

    void close()
    {
        this.metadata.close;
        this.longs.close;
        this.doubles.close;
        this.strings.close;
    }

    auto insert(JSONValue item)
    {
        auto keyhash = getValueHash(item);
        auto p = keyhash in this.valuesSeen;
        if (!p)
        {
            JsonKeyMetaData meta;
            meta.keyHash = keyhash;
            meta.type = item.getType;
            (item.val).match!((bool x) { meta.padding = (cast(ulong) x) + 1; }, (long x) {
                meta.keyOffset = this.longs.tell;
                this.longs.write(x);
                meta.keyLength = this.longs.tell - meta.keyOffset;
            }, (double x) {
                meta.keyOffset = this.doubles.tell;
                this.doubles.write(x);
                meta.keyLength = this.doubles.tell - meta.keyOffset;
            }, (const(char)[] x) {
                meta.keyOffset = this.strings.tell;
                this.strings.write(x);
                meta.keyLength = this.strings.tell - meta.keyOffset;
            });
            metadata.write(meta);
            valuesSeen.insert(keyhash);
        }
        return keyhash;
    }
}

/// Stores json data and ids
/// for a given field
struct JsonStoreReader
{

    /// hashset of observed values
    khashlSet!(uint128) valuesSeen;

    JsonKeyMetaData[] metadata;
    /// store integer json values
    LongStore* longs;
    /// store double json values
    DoubleStore* doubles;
    /// store strings json values
    StringStore* strings;

    this(string prefix)
    {
        this.longs = new LongStore(prefix ~ ".json.longs", "rb");
        this.doubles = new DoubleStore(prefix ~ ".json.doubles", "rb");
        this.strings = new StringStore(prefix ~ ".json.strings", "rb");
        auto md = new JsonMetaStore(prefix ~ ".json.meta", "rb");
        this.metadata = md.getAll.array;
        md.close;
        foreach (JsonKeyMetaData key; metadata)
        {
            this.valuesSeen.insert(key.keyHash);
        }
    }

    void close()
    {
        this.longs.close;
        this.doubles.close;
        this.strings.close;
    }

    auto filter(T)(T[] items)
    {
        return items.map!(x => getValueHash(JSONValue(x)))
            .std_filter!(x => x in valuesSeen);
    }

    auto filterRange(T)(T[] range)
    {
        assert(range.length == 2);
        assert(range[0] <= range[1]);
        auto type = JSONValue(range[0]).getType;
        // JSONValue[2] r = [JSONValue(range[0]), JSONValue(range[1]];
        auto hashes = this.metadata
            .std_filter!(x => x.type == type)
            .map!(x => x.keyHash);

        static if (isIntegral!T)
        {
            auto values = this.longs.getAll();
        }
        else static if (isBoolean!T)
        {
            auto values = this.metadata.getAll().filter!(x => x.padding > 0)
                .map!(x => x.padding == 1 ? false : true);
        }
        else static if (isFloatingPoint!T)
        {
            auto values = this.doubles.getAll();
        }
        else static if (isSomeString!T)
        {
            auto values = this.strings.getAll();
        }

        return zip(hashes, values).std_filter!(x => x[1] >= range[0])
            .std_filter!(x => x[1] < range[1])
            .map!(x => x[0]);
    }

    InputRange!uint128 filterOp(string op, T)(T val)
    {
        auto type = JSONValue(val).getType;
        // JSONValue[2] r = [JSONValue(range[0]), JSONValue(range[1]];
        auto hashes = this.metadata
            .std_filter!(x => x.type == type)
            .map!(x => x.keyHash);

        static if (isIntegral!T)
        {
            auto values = this.longs.getAll();
        }
        else static if (isBoolean!T)
        {
            auto values = this.metadata.getAll().filter!(x => x.padding > 0)
                .map!(x => x.padding == 1 ? false : true);
        }
        else static if (isFloatingPoint!T)
        {
            auto values = this.doubles.getAll();
        }
        else static if (isSomeString!T)
        {
            auto values = this.strings.getAll();
        }
        else
        {
            static assert(0);
        }
        mixin("auto func = (" ~ T.stringof ~ " a) => a " ~ op ~ " val;");
        return zip(hashes, values).std_filter!(x => func(x[1]))
            .map!(x => x[0])
            .inputRangeObject;
    }

    auto getJsonValues()
    {
        auto l = this.longs.getAll();
        auto d = this.doubles.getAll();
        auto s = this.strings.getAll();
        auto b = this.metadata
            .std_filter!(x => x.padding > 0)
            .map!(x => x.padding == 1 ? false : true);
        return chain(l.map!(x => JSONValue(x)), d.map!(x => JSONValue(x)),
                s.map!(x => JSONValue(x)), b.map!(x => JSONValue(x)));
    }

    auto getJsonValuesByType(T)()
    {
        static if (isIntegral!T)
        {
            return this.longs.getAll();
        }
        else static if (isBoolean!T)
        {
            return this.metadata.getAll().filter!(x => x.padding > 0)
                .map!(x => x.padding == 1 ? false : true);
        }
        else static if (isFloatingPoint!T)
        {
            return this.doubles.getAll();
        }
        else static if (isSomeString!T)
        {
            return this.strings.getAll();
        }
    }
}

struct IdsStoreWriter
{
    UlongStore* ids;

    this(string prefix)
    {
        this.ids = new UlongStore(prefix ~ ".ids", "wbu");
    }

    void close()
    {
        this.ids.close;
    }

    void insert(ulong id)
    {
        log_debug(__FUNCTION__, "inserting %d", id);
        ids.write(id);
    }
}

struct IdsStoreReader
{
    UlongStore* ids;

    this(string prefix)
    {
        this.ids = new UlongStore(prefix ~ ".ids", "rb");
    }

    void close()
    {
        this.ids.close;
    }

    ulong[] getIds()
    {
        return this.ids.getAll.array;
    }
}

unittest
{
    import htslib.hts_log;
    import std.stdio;

    set_log_level(LogLevel.Debug);
    {
        auto jidx = JsonStoreWriter("/tmp/test_jidx");
        jidx.insert(JSONValue("testval"));
        jidx.insert(JSONValue("testval2"));
        jidx.insert(JSONValue(0));
        jidx.insert(JSONValue(2));
        jidx.insert(JSONValue(3));
        jidx.insert(JSONValue(5));
        jidx.insert(JSONValue(1.2));

        jidx.insert(JSONValue("testval2"));
        jidx.insert(JSONValue(3));
        jidx.insert(JSONValue(5));
        jidx.insert(JSONValue(1.2));
        jidx.close;
    }
    {
        auto jidx = JsonStoreReader("/tmp/test_jidx");
        assert(jidx.getJsonValues.array.length == 7);
        assert(jidx.metadata.length == 7);
        assert(jidx.getJsonValuesByType!(const(char)[]).array == [
                "testval", "testval2"
                ]);
        assert(jidx.getJsonValuesByType!(long).array == [0, 2, 3, 5]);
        assert(jidx.getJsonValuesByType!(double).array == [1.2]);
        writeln(jidx.filter(["testval", "testval2"]).map!(x => format("%x", x)).array);
        assert(jidx.filter(["testval", "testval2"]).map!(x => format("%x", x))
                .array == [
                    "593B6F8099E807BA705B5CAF4AFFB497",
                    "F3D78B7682E976BF2BB122AFB37DF618"
                ]);
    }

}
