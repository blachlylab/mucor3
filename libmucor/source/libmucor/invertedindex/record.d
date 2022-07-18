module libmucor.invertedindex.record;

import std.traits;
import std.typecons : tuple;
import std.algorithm : std_map = map;

import libmucor.hts_endian;
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

size_t toHash(uint256 x) nothrow @safe
{
    auto hi = uint128(x.data[0..2]);
    auto lo = uint128(x.data[2..4]);

    return uint128([hi.toHash, lo.toHash]).toHash;
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
}

alias Hash2IonStore = RecordStore!(uint128, immutable(ubyte)[]);
alias String2HashStore = RecordStore!(const(char)[], uint128);
alias Long2HashStore = RecordStore!(long, uint128);
alias Float2HashStore = RecordStore!(double, uint128);
alias KVIndex = RecordStore!(uint256, uint128[]);

alias IonData = immutable(ubyte)[];

auto serialize(T)(T val)
{
    static if(is(T == IonData))
        return cast(ubyte[])val;
    else static if(isSomeString!T)
        return cast(ubyte[])val;
    else static if(isNumeric!T){
        ubyte[T.sizeof] arr;
        static if(is(T == float)){
            float_to_le(val, arr.ptr);
        }else static if(is(T == double)){
            double_to_le(val, arr.ptr);
        } else static if(is(T == ulong)){
            u64_to_le(val, arr.ptr);
        } else static if(is(T == long)){
            i64_to_le(val, arr.ptr);
        } else static assert(0);
        return arr;
    } else static if(is(T == uint128)) {
        ubyte[16] arr;
        u64_to_le(val.data[0], arr.ptr);
        u64_to_le(val.data[1], arr.ptr + 8);
        return arr;
    } else static if(is(T == uint256)) {
        ubyte[32] arr;
        u64_to_le(val.data[0], arr.ptr);
        u64_to_le(val.data[1], arr.ptr + 8);
        u64_to_le(val.data[2], arr.ptr + 16);
        u64_to_le(val.data[3], arr.ptr + 24);
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
    else static if(isSomeString!T)
        ret = cast(T)(val.dup);
    else static if(isNumeric!T){
        static if(is(T == float)){
            ret = le_to_float(val.ptr);
        }else static if(is(T == double)){
            ret = le_to_double(val.ptr);
        } else static if(is(T == ulong)){
            ret = le_to_u64(val.ptr);
        } else static if(is(T == long)){
            ret = le_to_i64(val.ptr);
        } else static assert(0);
    } else static if(is(T == uint128)) {
        ret = uint128([le_to_u64(val.ptr), le_to_u64(val.ptr + 8)]);
    } else static if(is(T == uint256)) {
        ret = uint256([
            le_to_u64(val.ptr), 
            le_to_u64(val.ptr + 8),
            le_to_u64(val.ptr + 16),
            le_to_u64(val.ptr + 24)]);
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
    auto i = 1L;
    assert(deserialize!(long, true)(serialize(i)) == i);
    i = 2;
    assert(deserialize!(long, true)(serialize(i)) == i);

    auto f = 1.2f;
    assert(deserialize!(float, true)(serialize(f)) == f);
    
    f = 10.000050;
    assert(deserialize!(float, true)(serialize(f)) == f);

    auto h = uint128.fromHexString("e85b76fab7734ce9bebcd1e4517162e6");
    assert(deserialize!(uint128, true)(serialize(h)) == h);

    auto h2 = uint256.fromHexString("e85b76fab7734ce9bebcd1e4517162e62550073b968043d39772f665b8fbd46f");
    assert(deserialize!(uint256, true)(serialize(h2)) == h2);

    auto s = "test";
    assert(deserialize!(string, true)(serialize(s)) == s);

}

unittest {
    import std.stdio : writefln;
    import std.datetime.stopwatch : benchmark;
    import drocks.env;
    import drocks.options;
    import drocks.merge;
    import mir.ion.conv;
    import libmucor.invertedindex.hash;

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
    idx[combineHash(getKeyHash("key1"), getValueHash("test"))] ~= h1;
    idx[combineHash(getKeyHash("key1"), getValueHash("test2"))] ~= h2;
    idx[combineHash(getKeyHash("key1"), getValueHash("test3"))] ~= h3;

    assert(idx[combineHash(getKeyHash("key1"), getValueHash("test"))].unwrap.unwrap == [h1]);
    assert(idx[combineHash(getKeyHash("key1"), getValueHash("test2"))].unwrap.unwrap == [h2]);
    assert(idx[combineHash(getKeyHash("key1"), getValueHash("test3"))].unwrap.unwrap == [h3]);

    import std.file;
    rmdirRecurse("/tmp/test_store");
}