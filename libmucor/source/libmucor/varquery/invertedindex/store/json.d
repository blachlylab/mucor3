module libmucor.varquery.invertedindex.store.json;

import libmucor.varquery.invertedindex.store.binary;
import libmucor.varquery.invertedindex.jsonvalue;
import libmucor.varquery.invertedindex.metadata;
import libmucor.varquery.invertedindex.store: getKeyHash, getValueHash;
import libmucor.wideint; 
import libmucor.khashl;
import std.file: exists;
import std.typecons: Tuple;
import htslib.hts_log;
import std.sumtype: match;
import std.format: format;
import std.algorithm : sort, uniq, map, std_filter = filter, canFind, joiner;
import std.range: chain, zip;
import std.traits;
import std.array: array;

/// Stores json data and ids
/// for a given field
struct JsonStoreWriter {
    string prefix;
    /// hashmap for writing
    /// json value hash maps to a set of ids
    khashl!(uint128, IdsStoreWriter) hashmap;

    /// store json value meta data
    JsonMetaStore metadata;
    /// store integer json values
    LongStore longs;
    /// store double json values
    DoubleStore doubles;
    /// store strings json values
    StringStore strings;

    this(string prefix) {
        this.prefix = prefix;
        this.metadata = JsonMetaStore(prefix ~  ".json.meta", "wb");
        this.longs = LongStore(prefix ~  ".json.longs", "wb");
        this.doubles = DoubleStore(prefix ~  ".json.doubles", "wb");
        this.strings = StringStore(prefix ~  ".json.strings", "wb");
    }

    ~this() {
        this.hashmap.kh_release;
    }

    void insert(JSONValue item, ulong id) {
        auto keyhash = getValueHash(item);
        auto p = keyhash in hashmap;
        if(p) {
            p.insert(id);
        } else {
            JsonKeyMetaData meta;
            meta.keyHash = keyhash;
            meta.type = item.getType;
            hts_log_info(__FUNCTION__, format("creating new id index %s for json value %s", prefix ~ "_" ~ format("%x",keyhash), item));
            auto s = IdsStoreWriter(prefix ~ "_" ~ format("%x", keyhash));
            s.insert(id);
            this.hashmap[keyhash] = s;
            (item.val).match!(
                (bool x) {
                    meta.padding = (cast(ulong)x) + 1;
                },
                (long x) {
                    meta.keyOffset = this.longs.tell;
                    this.longs.write(x);
                    meta.keyLength = this.longs.tell - meta.keyOffset;
                },
                (double x) {
                    meta.keyOffset = this.doubles.tell;
                    this.doubles.write(x);
                    meta.keyLength = this.doubles.tell - meta.keyOffset;
                },
                (const(char)[] x) {
                    meta.keyOffset = this.strings.tell;
                    this.strings.write(x);
                    meta.keyLength = this.strings.tell - meta.keyOffset;
                }
            );
            metadata.write(meta);
        }
    }
}

/// Stores json data and ids
/// for a given field
struct JsonStoreReader {

    /// hashmap for writing
    /// json value hash maps to a set of ids
    khashl!(uint128, IdsStoreReader) hashmap;
    JsonKeyMetaData[] metadata;
    /// store integer json values
    LongStore longs;
    /// store double json values
    DoubleStore doubles;
    /// store strings json values
    StringStore strings;

    this(string prefix) {
        this.longs = LongStore(prefix ~ ".json.longs", "rb");
        this.doubles = DoubleStore(prefix ~ ".json.doubles", "rb");
        this.strings = StringStore(prefix ~ ".json.strings", "rb");
        this.metadata = JsonMetaStore(prefix ~  ".json.meta", "rb").getAll;
        hts_log_info(__FUNCTION__, format("loading json index %s with %d keys", prefix, this.metadata.length));
        foreach(meta; this.metadata) {
            assert((prefix ~ "_" ~ format("%x", meta.keyHash) ~ ".ids").exists);
            hashmap[meta.keyHash] = IdsStoreReader(prefix ~ "_" ~ format("%x", meta.keyHash));
        }
    }

    ~this() {
        this.hashmap.kh_release;
    }

    ulong[] filter(T)(T[] items)
    {
        debug {
            foreach(k;items){
                auto v = JSONValue(k);
                auto hash = getValueHash(v);
                if (!(hash in hashmap)) {
                    hts_log_warning(__FUNCTION__, format("Value %s with hash %x not found", v, hash));
                }
            }
        }
        return items.map!(x => getValueHash(JSONValue(x)))
            .std_filter!(x => x in hashmap)
            .map!(x => (cast(IdsStoreReader) hashmap[x]).getIds)
            .joiner.array; 
    }

    ulong[] filterRange(T)(T[] range)
    {
        assert(range.length==2);
        assert(range[0]<=range[1]);
        auto type = JSONValue(range[0]).getType;
        // JSONValue[2] r = [JSONValue(range[0]), JSONValue(range[1]];
        auto hashes = this.metadata
            .std_filter!(x=> x.type == type)
            .map!(x => x.keyHash);

        static if(isIntegral!T) {
            auto values =  this.longs.getAll();
        } else static if(isBoolean!T) {
            auto values =  this.metadata.getAll()
                .filter!(x => x.padding > 0)
                .map!(x => x.padding == 1 ? false : true);
        } else static if(isFloatingPoint!T) {
            auto values =  this.doubles.getAll();
        } else static if(isSomeString!T) {
            auto values = this.strings.getAll();    
        }

        return zip(hashes, values)
            .std_filter!(x=>x[1] >= range[0])
            .std_filter!(x=>x[1] < range[1])
            .map!(x => (cast(IdsStoreReader)hashmap[x[0]]).getIds)
            .joiner.array;
    }

    ulong[] filterOp(string op, T)(T val)
    {
        auto type = JSONValue(val).getType;
        // JSONValue[2] r = [JSONValue(range[0]), JSONValue(range[1]];
        auto hashes = this.metadata
            .std_filter!(x=> x.type == type)
            .map!(x => x.keyHash);

        static if(isIntegral!T) {
            auto values =  this.longs.getAll();
        } else static if(isBoolean!T) {
            auto values =  this.metadata.getAll()
                .filter!(x => x.padding > 0)
                .map!(x => x.padding == 1 ? false : true);
        } else static if(isFloatingPoint!T) {
            auto values =  this.doubles.getAll();
        } else static if(isSomeString!T) {
            auto values = this.strings.getAll();    
        }
        mixin("auto func = ("~T.stringof~" a) => a "~op~" val;");
        return zip(hashes, values)
            .std_filter!(x => func(x[1]))
            .map!(x => (cast(IdsStoreReader)hashmap[x[0]]).getIds)
            .joiner.array;
    }

    auto getJsonValues() {
        auto l = this.longs.getAll();
        auto d = this.doubles.getAll();
        auto s = this.strings.getAll();
        auto b = this.metadata
            .std_filter!(x => x.padding > 0)
            .map!(x => x.padding == 1 ? false : true);
        return chain(
            l.map!(x => JSONValue(x)),
            d.map!(x => JSONValue(x)),
            s.map!(x => JSONValue(x)),
            b.map!(x => JSONValue(x))
        );
    }

    auto getJsonValuesByType(T)() {
        static if(isIntegral!T) {
            return this.longs.getAll();
        } else static if(isBoolean!T) {
            return this.metadata.getAll()
                .filter!(x => x.padding > 0)
                .map!(x => x.padding == 1 ? false : true);
        } else static if(isFloatingPoint!T) {
            return this.doubles.getAll();
        } else static if(isSomeString!T) {
            return this.strings.getAll();    
        }
    }
}


struct IdsStoreWriter {
    UlongStore ids;

    this(string prefix) {
        this.ids = UlongStore(prefix ~ ".ids", "wb");
    }

    void insert(ulong id) {
        ids.write(id);
    }
}

struct IdsStoreReader {
    UlongStore ids;

    this(string prefix) {
        this.ids = UlongStore(prefix ~ ".ids", "rb");
    }

    ulong[] getIds() {
        return this.ids.getAll;
    }
}

unittest
{
    import htslib.hts_log;
    import std.stdio;
    hts_set_log_level(htsLogLevel.HTS_LOG_DEBUG);
    {
        auto jidx = JsonStoreWriter("/tmp/test_json");
        jidx.insert(JSONValue("testval"), 0);
        jidx.insert(JSONValue("testval2"), 1);
        jidx.insert(JSONValue(0), 2);
        jidx.insert(JSONValue(2), 3);
        jidx.insert(JSONValue(3), 4);
        jidx.insert(JSONValue(5), 5);
        jidx.insert(JSONValue(1.2), 6);
    }
    {
        auto jidx = JsonStoreReader("/tmp/test_json");
        assert(jidx.getJsonValues.array.length == 7);
        assert(jidx.metadata.length == 7);
        assert(jidx.getJsonValuesByType!(const(char)[]) == ["testval", "testval2"]);
        assert(jidx.getJsonValuesByType!(long) == [0, 2, 3, 5]);
        assert(jidx.getJsonValuesByType!(double) == [1.2]);
        assert(jidx.filter(["testval", "testval2"]) == [0, 1]);
    }
    
}

