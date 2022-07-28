module libmucor.invertedindex.store;

import libmucor.invertedindex.record;
import libmucor.error;
import libmucor.atomize.serde;
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
import libmucor.khashl;

struct InvertedIndexStore {
    RocksDB db;
    Hash2IonStore records;
    KVIndex idx;
    khashlSet!(const(char)[]) keys;

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
            this.idx = KVIndex(&this.db.columnFamilies["idx"]);
            getIonKeys;
        } else {
            this.records = Hash2IonStore(this.db.createColumnFamily("records").unwrap);
            this.idx = KVIndex(this.db.createColumnFamily("idx").unwrap);
        }
    }
    ~this(){
        this.storeIonKeys;
    }

    /// load string keys from db that have been observed in data
    /// i.e INFO/ANN, CHROM, POS, ...
    auto getIonKeys() {
        this.keys = *deserialize!(const(char)[][])(this.db[serialize("keys")].unwrap.unwrap).collect;
    }

    /// store string keys into db that have been observed in data
    /// i.e INFO/ANN, CHROM, POS, ...
    auto storeIonKeys() {
        this.db[serialize("keys")] = serialize(this.keys.byKey.array);
    }

    void storeSharedSymbolTable(ref VcfIonDeserializer deserializer){
        this.db[serialize("sharedTable")] = deserializer.sharedSymbolTable.toBytes;
    }

    auto getSharedSymbolTable(){
        return this.db[serialize("sharedTable")];
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

    void insert(ref VcfIonRecord rec) {
        auto data = rec.obj;
        IonInt hashValue;
        if(_expect(!("checksum" in data), false)) {
            log_err(__FUNCTION__, "record with no md5");
        }
        auto err = data["checksum"].get(hashValue);
        assert(err == IonErrorCode.none);

        uint128 hash = uint128.fromBigEndian(hashValue.data, hashValue.sign);

        this.records[hash] = cast(immutable(ubyte)[])(ionPrefix ~ rec.localSymbols.data ~ data.ionStruct.data);

        foreach (key,value; data)
        {
            insertIonValue(key, value, data.symbolTable, hash);
        }
    }

    void insertIonValue(const(char)[] key, IonDescribedValue value, const(char[])[] symbolTable, uint128 checksum) {
        if(key == "checksum") return;
        this.keys.insert(key);
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
                auto k = CompositeKey(key, l);

                this.idx[k] ~= checksum;
                return;

            case IonTypeCode.nInt:
                IonNInt val;
                auto err = value.get(val);
                assert(err == IonErrorCode.none);

                auto l = val.get!long;
                auto k = CompositeKey(key, l);

                this.idx[k] ~= checksum;
                return;

            case IonTypeCode.float_:
                IonFloat val;
                auto err = value.get(val);
                assert(err == IonErrorCode.none);

                auto f = val.get!double;
                auto k = CompositeKey(key, f);

                this.idx[k] ~= checksum;
                return;
            case IonTypeCode.decimal:
                IonDecimal val;
                auto err = value.get(val);
                assert(err == IonErrorCode.none);

                auto f = val.get!double;
                auto k = CompositeKey(key, f);

                this.idx[k] ~= checksum;
                return;
            case IonTypeCode.symbol:
                IonSymbolID val;
                auto err = value.get(val);
                assert(err == IonErrorCode.none);

                auto i = val.get;
                auto s = symbolTable[i];
                auto k = CompositeKey(key, s);

                this.idx[k] ~= checksum;
                return;
            case IonTypeCode.string:
                const(char)[] val;
                auto err = value.get(val);
                assert(err == IonErrorCode.none);

                auto k = CompositeKey(key, val);

                this.idx[k] ~= checksum;
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

    /// key = val
    Option!(uint128[]) filterSingle(T)(const(char)[] key, T val) {
        auto k = CompositeKey(key, val);

        return this.idx[k].unwrap;
    }

    auto filterRange(T)(const(char)[] key, T[] range)
    {
        static if (isNumeric!T)
        {
            auto lowerK = CompositeKey(key, range[0]);
            auto upperK = CompositeKey(key, range[1]);
            auto vals = this.idx.filterRange(lowerK, upperK)
                .filter!(x => x[0].key == lowerK.key)
                .map!(x => x[1]);
        }
        else static if (isBoolean!T)
        {
            static assert(0);
        }
        else static if (isSomeString!T)
        {
            auto lowerK = CompositeKey(key, range[0]);
            auto upperK = CompositeKey(key, range[1]);
            auto vals = this.idx.byKeyValue(lowerK, upperK)
                .map!(x => x[1]);
        }
        
        uint128[] ret;
        vals.each!(x=> ret ~= x);
        return ret;
    }
    
    auto filterOp(string op, T)(const(char)[] key, T val)
    if(isFloatingPoint!T || isIntegral!T)
    { 
        import std.stdio;
        auto ival = CompositeKey(key, val);
    
        auto vals = this.idx.filterOp!op(ival)
            .filter!(x => x[0].key == ival.key)
            .map!(x => x[1]);

        alias ifun = (x) => mixin("x[0] "~op~" ival");
        uint128[] ret;
        vals.each!(x=> ret ~= x);
        return ret;
    }

    auto getIonObjects(R)(R range) {
        return range.map!(x => this.records[x].unwrap.unwrap);
    }

    void print() {
        import std.stdio;
        stderr.writeln("keys:");
        foreach (k; this.keys.byKey)
        {
            stderr.writeln(k);
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


