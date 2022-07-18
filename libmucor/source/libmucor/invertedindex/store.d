module libmucor.invertedindex.store;

import libmucor.invertedindex.record;
import libmucor.invertedindex.hash;
import libmucor.error;
import option;
import drocks.database;
import drocks.env;
import drocks.options;
import drocks.merge;

import std.algorithm : map, filter, joiner, each;
import std.range : inputRangeObject, InputRangeObject, chain;
import std.array : array;
import std.traits;

import mir.ion.value;
import mir.ser.ion;
import mir.utility;

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
        opts.setMergeOperator(createAppendMergeOperator);
        opts.env = env;
        this.db = RocksDB(opts, dbfn);
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

    void insert(IonStructWithSymbols data) {
        IonInt hashValue;
        if(_expect(!("checksum" in data), false)) {
            log_err(__FUNCTION__, "record with no md5");
        }
        auto err = data["checksum"].get(hashValue);
        assert(err == IonErrorCode.none);

        uint128 hash = uint128.fromBigEndian(hashValue.data, hashValue.sign);

        this.records[hash] = serializeIon(data);

        foreach (key,value; data)
        {
            insertIonValue(key, value, data.symbolTable, hash);
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

                auto f = val.get!double;
                auto vh = getValueHash(f);
                auto kh = getKeyHash(key);

                this.floats[f] = vh;
                this.idx[combineHash(kh, vh)] ~= checksum;
                this.keys[key] = kh;
                return;
            case IonTypeCode.decimal:
                IonDecimal val;
                auto err = value.get(val);
                assert(err == IonErrorCode.none);

                auto f = val.get!double;
                auto vh = getValueHash(f);
                auto kh = getKeyHash(key);

                this.floats[f] = vh;
                this.idx[combineHash(kh, vh)] ~= checksum;
                this.keys[key] = kh;
                return;
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

    bool checkKey(const(char)[] key) {
        if(this.keys[key].unwrap.isNone) {
            log_warn(__FUNCTION__, "Key \"%s\" was not found in index!", key);
            return false;
        }
        return true;
    }

    auto getKeysWithId() {
        return this.keys.byKeyValue;
    }

    /// key = val
    Option!(uint128[]) filterSingle(T)(uint128 kh, T val) {
        auto vh = getValueHash(val);

        static if(isSomeString!T) {
            if(this.strings[val].unwrap.isNone) {
                log_warn(__FUNCTION__, "String value \"%s\" was not found in index!", val);
            }

        } else static if(isIntegral!T) {
            if(this.longs[val].unwrap.isNone) {
                log_warn(__FUNCTION__, "Int value \"%d\" was not found in index!", val);
            }

        } else static if(isFloatingPoint!T) {
            if(this.floats[val].unwrap.isNone) {
                log_warn(__FUNCTION__, "Float value \"%f\" was not found in index!", val);
            }
        } else static if(isBoolean!T) {

        }

        return this.idx[combineHash(kh, vh)].unwrap;
    }

    auto filterRange(T)(uint128 kh, T[] range)
    {
        assert(range.length == 2);
        assert(range[0] <= range[1]);

        static if (isIntegral!T || isFloatingPoint!T)
        {
            auto ivals = this.longs
                .byKeyValue
                .filter!( x => x[0] >= (range[0]) && (x[0] < range[1]))
                .map!(x => x[1]);
            auto fvals = this.floats
                .byKeyValue
                .filter!( x => x[0] >= (range[0]) && (x[0] < range[1]))
                .map!(x => x[1]);
            auto vals = chain(ivals, fvals);
        }
        else static if (isBoolean!T)
        {
            static assert(0);
        }
        else static if (isSomeString!T)
        {
            auto vals = this.strings
                .byKeyValue
                .filter!( x => x[0] >= (range[0]) && (x[0] < range[1]))
                .map!(x => x[1]);
        }
        uint128[] ret;
        vals.map!(x => this.idx[combineHash(kh, x)].unwrap)
            .filter!(x => !x.isNone)
            .map!(x => x.unwrap).each!(x=> ret ~= x);
        return ret;
    }
    
    auto filterOp(string op, T)(uint128 kh, T val)
    if(isFloatingPoint!T || isIntegral!T)
    {

        mixin("auto lfunc = (long a) => a " ~ op ~ " val;");
        mixin("auto dfunc = (double a) => a " ~ op ~ " val;");

        auto ivals = this.longs
            .byKeyValue
            .filter!( x => lfunc(x[0]))
            .map!(x => x[1]);

        auto dvals = this.floats
            .byKeyValue
            .filter!( x => dfunc(x[0]))
            .map!(x => x[1]);
        uint128[] ret;
        chain(ivals, dvals)
            .map!(x => this.idx[combineHash(kh, x)].unwrap)
            .filter!(x => !x.isNone)
            .map!(x => x.unwrap)
            .each!(x=> ret ~= x);
        return ret;
    }

    auto getIonObjects(R)(R range) {
        return range.map!(x => this.records[x].unwrap.unwrap);
    }

    void print() {
        import std.stdio;
        stderr.writeln("keys:");
        foreach (kv; this.keys.byKeyValue)
        {
            stderr.writeln(kv);
        }
        stderr.writeln("strings:");
        foreach (kv; this.strings.byKeyValue)
        {
            stderr.writeln(kv);
        }

        stderr.writeln("longs:");
        foreach (kv; this.longs.byKeyValue)
        {
            stderr.writeln(kv);
        }
        stderr.writeln("floats:");
        foreach (kv; this.floats.byKeyValue)
        {
            stderr.writeln(kv);
        }
        stderr.writeln("records:");
        foreach (kv; this.records.byKeyValue)
        {
            stderr.writeln(kv);
        }
        stderr.writeln("idx");
        foreach (kv; this.idx.byKeyValue)
        {
            stderr.writeln(kv);
        }
    }
}


