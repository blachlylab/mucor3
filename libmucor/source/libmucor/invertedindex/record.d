module libmucor.invertedindex.record;

import std.traits;
import std.typecons : tuple;
import std.algorithm : map;

import libmucor.hts_endian;
import libmucor.spookyhash;
import option;

import drocks.database;
import drocks.columnfamily;
import drocks.env;
import drocks.options;

import mir.bignum.integer;
import mir.ion.value;
import mir.ser.ion;
import core.stdc.stdlib : free;

alias uint128 = BigInt!2;
alias uint256 = BigInt!4;

struct RecordStore(K, V) {
    ColumnFamily * family;

    this(ColumnFamily * family) {
        this.family = family;
    }

    auto opIndex(K key)
    {
        return deserialize!V((*this.family)[serialize(key)].unwrap.unwrap);
    }

    auto opIndexAssign(V value, K key)
    {
        return (*this.family)[serialize(key)] = serialize(value);
    }

    static if(isArray!V && !isSomeString!V) {
        auto opIndexOpAssign(string op: "~")(ForeachType!V value, K key)
        {
            return (*this.family)[serialize(key)] ~= serialize(value);
        }
    }

    auto byKeyValue() {
        auto r = this.family.iter;
        return r.map!(x => tuple(deserialize!K(x[0]), deserialize!V(x[0])));
    }
}

alias Hash2IonStore = RecordStore!(uint128, immutable(ubyte)[]);
alias String2HashStore = RecordStore!(const(char)[], uint128);
alias Long2HashStore = RecordStore!(long, uint128);
alias Float2HashStore = RecordStore!(float, uint128);
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
    } else static if(isArray!T){
        alias E = ForeachType!T;
        ret.length = val.length / E.sizeof;

        for(auto i = 0; i < val.length; i += E.sizeof){
            ret ~= deserialize!E(val[i .. i + E.sizeof]);
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
    opts.env = env;
    import std.file;
    
    auto db = RocksDB(opts, "/tmp/test_store");
    auto records = Hash2IonStore(db.createColumnFamily("records").unwrap);

    // Test string putting and getting
    auto h1 = uint128.fromHexString("c688402c66d84855a3a0ecb87240fc09");
    auto h2 = uint128.fromHexString("44630f65ef0a4cb1b065629923ccf249");
    auto h3 = uint128.fromHexString("f43fe659c1ed4d55b90b1b30a57eb92a");

    records[h1] =`{key1:test,key2:1.2,key3:[1,2],key4:"test"}`.text2ion;
    records[h2] =`{key1:test2,key2:3,key3:[1,2,3],key4:"test"}`.text2ion;
    records[h3] =`{key1:test3,key3:[1]}`.text2ion;

    assert(records[h1].ion2text == `{key1:test,key2:1.2,key3:[1,2],key4:"test"}`);
    assert(records[h2].ion2text == `{key1:test2,key2:3,key3:[1,2,3],key4:"test"}`);
    assert(records[h3].ion2text == `{key1:test3,key3:[1]}`);

    rmdirRecurse("/tmp/test_store");
}

uint128 getKeyHash(const(char)[] key)
{
    uint128 ret;
    ret.data[0] = SEED2;
    ret.data[1] = SEED4;
    SpookyHash.Hash128(key.ptr, key.length, &ret.data[0], &ret.data[1]);
    return ret;
}

enum SEED1 = 0x48e9a84eeeb9f629;
enum SEED2 = 0x2e1869d4e0b37fcb;
enum SEED3 = 0xb5b35cb029261cef;
enum SEED4 = 0x34095e180ababeec;
uint128 getValueHash(T)(T val)
{
    static if(isBoolean!T) {
        uint128 ret;
        auto v = cast(ulong) x;
        ret.data[0] = SEED1;
        ret.data[1] = SEED2;
        SpookyHash.Hash128(&v, 8, &ret.data[0], &ret.data[1]);
        return ret;
    } else static if(isIntegral!T) {
        uint128 ret;
        auto v = cast(ulong) val;
        ret.data[0] = SEED2;
        ret.data[1] = SEED3;
        SpookyHash.Hash128(&v, 8, &ret.data[0], &ret.data[1]);
        return ret;
    } else static if(isFloatingPoint!T) {
        uint128 ret;
        ret.data[0] = SEED1;
        ret.data[1] = SEED3;
        SpookyHash.Hash128(&val, T.sizeof, &ret.data[0], &ret.data[1]);
        return ret;
    } else static if(isSomeString!T) {
        uint128 ret;
        ret.data[0] = SEED1;
        ret.data[1] = SEED4;
        SpookyHash.Hash128(val.ptr, val.length, &ret.data[0], &ret.data[1]);
        return ret;
    } else static assert(0);
}

uint256 combineHash(uint128 a, uint128 b)
{
    uint256 v;
    v.data[0..2] = a.data[];
    v.data[2..4] = b.data[];
    return v;
}

struct InvertedIndexStore {
    RocksDB db;
    Hash2IonStore records;
    String2HashStore keys;
    String2HashStore strings;
    Long2HashStore longs;
    Float2HashStore floats;
    KVIndex idx;

    this(string dbfn) {
        Env env;
        env.initialize;
        env.backgroundThreads = 2;
        env.highPriorityBackgroundThreads = 1;

        RocksDBOptions opts;
        opts.initialize;
        opts.createIfMissing = true;
        opts.errorIfExists = false;
        opts.compression = CompressionType.None;
        opts.env = env;
        this.db = RocksDB(opts, "/tmp/test_store");
        auto cf = "records" in this.db.columnFamilies;
        if(cf) {
            this.records = Hash2IonStore(&this.db.columnFamilies["records"]);
            this.keys = String2HashStore(&this.db.columnFamilies["keys"]);
            this.strings = String2HashStore(&this.db.columnFamilies["strings"]);
            this.longs = Long2HashStore(&this.db.columnFamilies["longs"]);
            this.floats = Float2HashStore(&this.db.columnFamilies["floats"]);
            this.idx = KVIndex(&this.db.columnFamilies["idx"]);
        } else {
            this.records = Hash2IonStore(this.db.createColumnFamily("records").unwrap);
            this.keys = String2HashStore(this.db.createColumnFamily("keys").unwrap);
            this.strings = String2HashStore(this.db.createColumnFamily("strings").unwrap);
            this.longs = Long2HashStore(this.db.createColumnFamily("longs").unwrap);
            this.floats = Float2HashStore(this.db.createColumnFamily("floats").unwrap);
            this.idx = KVIndex(this.db.createColumnFamily("idx").unwrap);
        }
    }

    void insert(ref IonStructWithSymbols data) {
        IonInt hashValue; 
        auto err = data["checksum"].get(hashValue);
        assert(err == IonErrorCode.none);

        uint128 hash = uint128.fromBigEndian(hashValue.data, hashValue.sign);

        this.records[hash] = serializeIon(data);

        foreach (key,value; data)
        {

        }
    }

    void insertIonValue(const(char)[] key, IonDescribedValue value, const(char[])[] symbolTable, uint128 checksum) {
        if(key == "checksum") return;
        final switch(value.descriptor.type) {
            case IonTypeCode.null_:
                return;
            case IonTypeCode.bool_:
                debug assert(0, "We don't handle bool values yet");
                else return;
            case IonTypeCode.uInt:
                IonInt val;
                auto err = value.get(val);
                assert(err == IonErrorCode.none);

                auto l = val.get!long;
                auto vh = getValueHash(l);
                auto kh = getKeyHash(key);

                this.longs[l] = vh;
                this.idx[combineHash(kh, vh)] ~= checksum;
                this.keys[key] = kh;
                return;

            case IonTypeCode.nInt:
                IonNInt val;
                auto err = value.get(val);
                assert(err == IonErrorCode.none);

                auto l = val.get!long;
                auto vh = getValueHash(l);
                auto kh = getKeyHash(key);

                this.longs[l] = vh;
                this.idx[combineHash(kh, vh)] ~= checksum;
                this.keys[key] = kh;
                return;

            case IonTypeCode.float_:
                IonFloat val;
                auto err = value.get(val);
                assert(err == IonErrorCode.none);

                auto f = val.get!float;
                auto vh = getValueHash(f);
                auto kh = getKeyHash(key);

                this.floats[f] = vh;
                this.idx[combineHash(kh, vh)] ~= checksum;
                this.keys[key] = kh;
                return;
            case IonTypeCode.decimal:
                debug assert(0, "We don't handle ion decimal values");
                else return;
            case IonTypeCode.symbol:
                IonSymbolID val;
                auto err = value.get(val);
                assert(err == IonErrorCode.none);

                auto i = val.get;
                auto s = symbolTable[i];
                auto vh = getValueHash(s);
                auto kh = getKeyHash(key);

                this.strings[s] = vh;
                this.idx[combineHash(kh, vh)] ~= checksum;
                this.keys[key] = kh;
                return;
            case IonTypeCode.string:
                const(char)[] val;
                auto err = value.get(val);
                assert(err == IonErrorCode.none);

                auto vh = getValueHash(val);
                auto kh = getKeyHash(key);

                this.strings[val] = vh;
                this.idx[combineHash(kh, vh)] ~= checksum;
                this.keys[key] = kh;
                return;
            case IonTypeCode.clob:
                debug assert(0, "We don't handle ion clob values");
                else return;
            case IonTypeCode.blob:
                debug assert(0, "We don't handle ion blob values");
                else return;
            case IonTypeCode.timestamp:
                debug assert(0, "We don't handle ion timestamp values");
                else return;
            case IonTypeCode.list:
                IonList vals;
                auto err = value.get(vals);
                assert(err == IonErrorCode.none);

                foreach (val; vals)
                {
                    this.insertIonValue(key, val, symbolTable, checksum);
                }
                return;
            case IonTypeCode.sexp:
                debug assert(0, "We don't handle ion sexp values");
                else return;
            case IonTypeCode.struct_:
                IonStruct obj;
                IonStructWithSymbols objWSym;
                auto err = value.get(obj);
                assert(err == IonErrorCode.none);

                objWSym = obj.withSymbols(symbolTable);

                foreach (k, v; objWSym)
                {
                    this.insertIonValue(key ~ "/" ~ k, v, symbolTable, checksum);
                }
                return;
            case IonTypeCode.annotations:
                debug assert(0, "We don't handle ion annotations");
                else return;
        }
    }
}