module libmucor.invertedindex.record;

import std.traits;
import std.typecons : tuple;
import std.algorithm : std_map = map;
import std.format;
import std.array;
import std.conv;

import libmucor.hts_endian;
import libmucor.fp80;
import option;

import drocks.database;
import drocks.columnfamily;
import drocks.env;
import drocks.options;

import mir.bignum.integer;
import mir.ser.ion;
import core.stdc.stdlib : free;

alias uint128 = BigInt!2;
alias uint256 = BigInt!4;

size_t toHash(uint128 x) nothrow @safe
{
    ulong p = 0x5555555555555555; // pattern of alternating 0 and 1
    ulong c = 17316035218449499591; // random uneven integer constant; 
    ulong tmp = p * (x.data[0] ^ (x.data[0] >> 32));
    auto hash = c * (tmp ^ (tmp >> 32));
    return hash ^ (x.data[1] + c);
}

struct RecordStore(K, V) {
    ColumnFamily * family;

    this(ColumnFamily * family) {
        this.family = family;
    }

    Result!(Option!(V), string) opIndex(K key)
    {
        alias innerFun = (Option!(ubyte[]) x) => x.map!(y => deserialize!V(y));
        return (*this.family)[serialize(key)].map!(x => innerFun(x));
    }

    auto opIndexAssign(V value, K key)
    {
        return (*this.family)[serialize(key)] = serialize(value);
    }

    static if(isArray!V && !isSomeString!V) {
        auto opIndexOpAssign(string op: "~")(ForeachType!V value, K key)
        {
            this.family.opIndexOpAssign!op(serialize(value), serialize(key));
        }
    }

    auto byKeyValue() {
        auto r = this.family.iter;
        return r.std_map!(x => tuple(deserialize!(K,true)(x[0]), deserialize!(V,true)(x[1])));
    }

    auto filterOp(string op)(K key) {
        auto r = this.family.iter;
        static if(op == "<")
            return r.lt(serialize(key)).std_map!(x => tuple(deserialize!(K,true)(x[0]), deserialize!(V,true)(x[1])));
        else static if(op == "<=")
            return r.lte(serialize(key)).std_map!(x => tuple(deserialize!(K,true)(x[0]), deserialize!(V,true)(x[1])));
        else static if(op == ">")
            return r.gt(serialize(key)).std_map!(x => tuple(deserialize!(K,true)(x[0]), deserialize!(V,true)(x[1])));
        else static if(op == ">=")
            return r.gte(serialize(key)).std_map!(x => tuple(deserialize!(K,true)(x[0]), deserialize!(V,true)(x[1])));
        else static assert(0);
    }
    auto filterRange(K start, K end) {
        auto r = this.family.iter;
        return r.gte(serialize(start))
            .lt(serialize(end))
            .std_map!(x => tuple(deserialize!(K,true)(x[0]), deserialize!(V,true)(x[1])));
    }
}

enum IndexValueType: char {
    String = 's', 
    Number = 'n',
    Bool = 'b',
}

union IndexValue {
    const(char)[] s;
    FP80 n;
    bool b;
}

struct CompositeKey {
    const(char)[] key;
    IndexValueType vtype;

    IndexValue val;

    this(T)(const(char)[] key, T val) {
        this.key = key;
        static if(isNumeric!T){
            vtype = IndexValueType.Number;
            this.val.n = FP80(val); 
        } else static if(is(T == bool)) {
            vtype = IndexValueType.Bool;
            this.val.b = cast(long) val;
        } else {
            vtype = IndexValueType.String;
            this.val.s = val;
        }
    }

    auto toString() const {
        final switch(this.vtype) {
            case IndexValueType.Bool:
                return "%s::%s::%s".format(key, cast(char)vtype, val.b);
            case IndexValueType.Number:
                return "%s::%s::%s".format(key, cast(char)vtype, val.n.toString);
            case IndexValueType.String:
                return "%s::%s::%s".format(key, cast(char)vtype, val.s);
        }
    }

    int opCmp(const CompositeKey other) const
    {
        if(this.toString == other.toString) return 0;
        return this.toString < other.toString ? -1 : 1;
    }
}

alias Hash2IonStore = RecordStore!(uint128, IonData);
alias KVIndex = RecordStore!(CompositeKey, uint128[]);

alias IonData = immutable(ubyte)[];

auto serialize(T)(T val)
{
    static if(is(T == IonData))
        return cast(ubyte[])val;
    else static if(isSomeString!T)
        return cast(ubyte[])val;
    else static if(is(T == CompositeKey))
    {
        ubyte[] arr;
        arr ~= cast(ubyte[])val.key;
        arr ~= '\0';
        arr ~= cast(ubyte) val.vtype;
        final switch(val.vtype) {
            case IndexValueType.Bool:
                arr ~= cast(ubyte) val.val.b;
                return arr;
            case IndexValueType.Number:
                arr ~= nativeToBigEndian(val.val.n);
                return arr;
            case IndexValueType.String:
                return arr ~ cast(ubyte[])val.val.s;
        }
    }
    else static if(isArray!T && isSomeString!(ForeachType!T)) {
        // Calculate total space needed for strings
        ubyte[] arr;
        auto len = size_t.sizeof;
        foreach(s; val) {
            len += (cast(ubyte[]) val).length + size_t.sizeof;
        }

        // create array
        arr = new ubyte[len];
        auto p = arr.ptr;

        // store num strings
        u64_to_le(val.length, p);
        p += size_t.sizeof;

        // loop over strings
        foreach(s; val) {
            // store size of string
            auto a = cast(ubyte[]) s;
            u64_to_le(a.length, p);
            p += size_t.sizeof;
            // store string
            p[0 .. a.length] = a[];
            p += a.length;
        }
        return arr;   
    }
    else static if(is(T == uint128)) {
        ubyte[16] arr;
        u64_to_le(val.data[0], arr.ptr);
        u64_to_le(val.data[1], arr.ptr + 8);
        return arr;
    } else static if(isArray!T){
        ubyte[] arr;
        foreach (v; val)
        {
            arr ~= serialize(v);
        }
        return arr;

    } else static assert(0);
}

T deserialize(T, bool useGC = false)(ubyte[] val)
{
    T ret;
    static if(is(T == IonData))
        ret = val.dup;
    else static if(is(T == CompositeKey)) {
        import std.string;
        ret.key = fromStringz(cast(char*)val.ptr);
        ret.vtype = cast(IndexValueType) val[ret.key.length + 1];
        final switch(ret.vtype) {
            case IndexValueType.Bool:
                ret.val.b = cast(bool) val[ret.key.length + 2];
                break;
            case IndexValueType.Number:
                ret.val.n = bigEndianToNative(val[ret.key.length + 2 .. ret.key.length + 2 + 10][0..10]);
                break;
            case IndexValueType.String:
                ret.val.s = cast(const(char)[])val[ret.key.length + 2 .. $];
                break;
        }
    } else static if(isArray!T && isSomeString!(ForeachType!T)) {
        // first get num of strings
        auto p = val.ptr;
        auto len = le_to_u64(p);
        // create array
        p += size_t.sizeof;
        ret = new ForeachType!T[len];
        // loop over num strings
        for(auto i = 0; i < len; i++) {
            // create char array that is length of string
            char[] arr = new char[le_to_u64(p)];
            p += 8;
            // copy string data
            arr[] = cast(char[])(p[0 .. arr.length]);
            p += arr.length;
            // copy to string[]
            ret[i] = cast(ForeachType!T)arr;
        }

    } else static if(is(T == uint128)) {
        ret = uint128([le_to_u64(val.ptr), le_to_u64(val.ptr + 8)]);
    } else static if(is(T == uint128[])){
        ret.length = val.length / 16;
        auto p = val.ptr;
        for(auto i = 0; i < ret.length; i++){
            ret[i] = uint128([le_to_u64(p), le_to_u64(p + 8)]);
            p += 16;
        }
    } else static assert(0);

    if(!useGC) free(val.ptr);
    val = [];
    return ret;
}

unittest {
    auto h = uint128.fromHexString("e85b76fab7734ce9bebcd1e4517162e6");
    assert(deserialize!(uint128, true)(serialize(h)) == h);

}

unittest {
    import std.stdio : writefln;
    import std.algorithm : map, filter;
    import std.range;
    import std.datetime.stopwatch : benchmark;
    import drocks.env;
    import drocks.options;
    import drocks.merge;
    import mir.ion.conv;

    writefln("Testing Data stores");

    Env env;
    env.initialize;
    env.backgroundThreads = 2;
    env.highPriorityBackgroundThreads = 1;

    RocksDBOptions opts;
    opts.initialize;
    opts.createIfMissing = true;
    opts.errorIfExists = false;
    opts.compression = CompressionType.None;
    opts.setMergeOperator(createAppendMergeOperator);
    opts.env = env;
    import std.file;
    import std.path;

    if("/tmp/test_store".exists)
        rmdirRecurse("/tmp/test_store");
    
    auto db = RocksDB(opts, "/tmp/test_store");
    auto records = Hash2IonStore(db.createColumnFamily("records").unwrap);
    auto idx = KVIndex(db.createColumnFamily("idx").unwrap);

    // Test string putting and getting
    auto h1 = uint128.fromHexString("c688402c66d84855a3a0ecb87240fc09");
    auto h2 = uint128.fromHexString("44630f65ef0a4cb1b065629923ccf249");
    auto h3 = uint128.fromHexString("f43fe659c1ed4d55b90b1b30a57eb92a");

    records[h1] =`{key1:test,key2:1.2,key3:[1,2],key4:"test"}`.text2ion;
    records[h2] =`{key1:test2,key2:3,key3:[1,2,3],key4:"test"}`.text2ion;
    records[h3] =`{key1:test3,key3:[1]}`.text2ion;

    assert(records[h1].unwrap.unwrap.ion2text == `{key1:test,key2:1.2,key3:[1,2],key4:"test"}`);
    assert(records[h2].unwrap.unwrap.ion2text == `{key1:test2,key2:3,key3:[1,2,3],key4:"test"}`);
    assert(records[h3].unwrap.unwrap.ion2text == `{key1:test3,key3:[1]}`);
    idx[CompositeKey("key1", "test")] ~= h1;
    idx[CompositeKey("key1", "test2")] ~= h2;
    idx[CompositeKey("key1", "test3")] ~= h3;

    assert(idx[CompositeKey("key1", "test")].unwrap.unwrap == [h1]);
    assert(idx[CompositeKey("key1", "test2")].unwrap.unwrap == [h2]);
    assert(idx[CompositeKey("key1", "test3")].unwrap.unwrap == [h3]);

    idx[CompositeKey("key3", 1)] ~= h1;
    idx[CompositeKey("key3", 1)] ~= h2;
    idx[CompositeKey("key3", 1)] ~= h3;

    idx[CompositeKey("key3", 2)] ~= h2;
    idx[CompositeKey("key3", 2)] ~= h3;

    idx[CompositeKey("key3", 3)] ~= h3;

    assert(idx[CompositeKey("key3", 1)].unwrap.unwrap == [h1, h2, h3]);
    assert(idx[CompositeKey("key3", 2)].unwrap.unwrap == [h2, h3]);
    assert(idx[CompositeKey("key3", 3)].unwrap.unwrap == [h3]);
    
    assert(idx.filterOp!">"(CompositeKey("key3", 1)).filter!(x => x[0].vtype == IndexValueType.Number).map!(x => x[1]).array == [[h2, h3], [h3]]);
    assert(idx.filterOp!">"(CompositeKey("key3", 2)).filter!(x => x[0].vtype == IndexValueType.Number).map!(x => x[1]).array == [[h3]]);
    assert(idx.filterOp!">"(CompositeKey("key3", 3)).filter!(x => x[0].vtype == IndexValueType.Number).map!(x => x[1]).array == []);

    assert(idx.filterOp!">="(CompositeKey("key3", 1)).filter!(x => x[0].vtype == IndexValueType.Number).map!(x => x[1]).array == [[h1, h2, h3], [h2, h3], [h3]]);
    assert(idx.filterOp!">="(CompositeKey("key3", 2)).filter!(x => x[0].vtype == IndexValueType.Number).map!(x => x[1]).array == [[h2, h3], [h3]]);
    assert(idx.filterOp!">="(CompositeKey("key3", 3)).filter!(x => x[0].vtype == IndexValueType.Number).map!(x => x[1]).array == [[h3]]);

    assert(idx.filterOp!"<"(CompositeKey("key3", 1)).filter!(x => x[0].vtype == IndexValueType.Number).map!(x => x[1]).array == []);
    assert(idx.filterOp!"<"(CompositeKey("key3", 2)).filter!(x => x[0].vtype == IndexValueType.Number).map!(x => x[1]).array == [[h1, h2, h3]]);
    assert(idx.filterOp!"<"(CompositeKey("key3", 3)).filter!(x => x[0].vtype == IndexValueType.Number).map!(x => x[1]).array == [[h1, h2, h3], [h2, h3]]);

    assert(idx.filterOp!"<="(CompositeKey("key3", 1)).filter!(x => x[0].vtype == IndexValueType.Number).map!(x => x[1]).array == [[h1, h2, h3]]);
    assert(idx.filterOp!"<="(CompositeKey("key3", 2)).filter!(x => x[0].vtype == IndexValueType.Number).map!(x => x[1]).array == [[h1, h2, h3], [h2, h3]]);
    assert(idx.filterOp!"<="(CompositeKey("key3", 3)).filter!(x => x[0].vtype == IndexValueType.Number).map!(x => x[1]).array == [[h1, h2, h3], [h2, h3], [h3]]);


}