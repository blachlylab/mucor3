module libmucor.invertedindex.store;

import libmucor.invertedindex.record;
import libmucor.error;
import libmucor.serde;
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
import mir.appender : ScopedBuffer;
import libmucor.khashl;

struct InvertedIndexStore {
    RocksDB db;
    Hash2IonStore records;
    KVIndex idx;
    SymbolTable * currentSymbolTable;
    SymbolTableBuilder * newSymbolTable;

    this(string dbfn) {

        Env env;
        env.initialize;
        env.backgroundThreads = 16;
        env.highPriorityBackgroundThreads = 16;

        RocksDBOptions opts;
        opts.initialize;
        opts.createIfMissing = true;
        opts.errorIfExists = false;
        opts.compression = CompressionType.None;
        opts.setMergeOperator(createAppendMergeOperator);

        RocksBlockBasedOptions bbopts;
        bbopts.initialize;
        bbopts.cacheIndexAndFilterBlocks = true;
        bbopts.setFilterPolicy(FilterPolicy.BloomFull, 18);

        opts.setBlockBasedOptions(bbopts);
        opts.unorderedWrites = true;

        opts.env = env;
        this.db = RocksDB(opts, dbfn);
        auto cf = "records" in this.db.columnFamilies;
        if(cf) {
            this.records = Hash2IonStore(&this.db.columnFamilies["records"]);
            this.idx = KVIndex(&this.db.columnFamilies["idx"]);
            
            auto existing = this.getSharedSymbolTable.unwrap;
            if(!existing.isNone){
                this.currentSymbolTable = new SymbolTable;
                auto d = cast(const(ubyte)[]) existing.unwrap;
                this.currentSymbolTable.loadSymbolTable(d);
                this.newSymbolTable = new SymbolTableBuilder;
                foreach (key; this.currentSymbolTable.table[10..$])
                {
                    this.newSymbolTable.insert(key);
                }
            }
            
        } else {
            this.records = Hash2IonStore(this.db.createColumnFamily("records").unwrap);
            this.idx = KVIndex(this.db.createColumnFamily("idx").unwrap);
        }
    }

    /// load string keys from db that have been observed in data
    /// i.e INFO/ANN, CHROM, POS, ...
    khashlSet!(const(char)[]) * getIonKeys() {
        return this.idx.byKeyValue().map!(x => cast(const(char)[])(x[0].key.dup)).collect;
    }

    void storeSharedSymbolTable(SymbolTable * lastSymbolTable){
        import mir.ion.symbol_table : IonSymbolTable;
        IonSymbolTable!false table;
        table.initialize;
        foreach (key; lastSymbolTable.table[10 .. $])
        {
            table.insert(key.dup);        
        }
        table.finalize;
        this.db[serialize("sharedTable")] = table.data;
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
        handleIonError(err);

        uint128 hash = uint128.fromBigEndian(hashValue.data, hashValue.sign);

        this.records[hash] = serializeIon(data);

        foreach (key,value; data)
        {
            insertIonValue(key, value, data.symbolTable, hash);
        }
    }

    void insert(ref VcfIonRecord rec) {
        IonStructWithSymbols data;
        IonStruct sval;
        IonInt hashValue;
        IonErrorCode err;
        
        if(this.currentSymbolTable && rec.symbols.table != this.currentSymbolTable.table) {
            auto val = convertIonSymbols(rec);
            IonDescribedValue dval;
            err = val.describe(dval);
            handleIonError(err);
            
            err = dval.get(sval);
            handleIonError(err);
            data = sval.withSymbols(this.currentSymbolTable.table);
            
        } else {
            sval = rec.getObj;
            data = sval.withSymbols(rec.symbols.table);
        }

        data = sval.withSymbols(rec.symbols.table);
        if(_expect(!("checksum" in data), false)) {
            log_err(__FUNCTION__, "record with no md5");
        }
        err = data["checksum"].get(hashValue);
        handleIonError(err);

        uint128 hash = uint128.fromBigEndian(hashValue.data, hashValue.sign);
        this.records[hash] = cast(immutable(ubyte)[])rec.toBytes;

        foreach (key,value; data)
        {
            insertIonValue(key, value, data.symbolTable, hash);
        }
    }

    void insertIonValue(const(char)[] key, IonDescribedValue value, const(char[])[] symbolTable, uint128 checksum) {
        if(key == "checksum") return;

        IonErrorCode err;
        final switch(value.descriptor.type) {
            case IonTypeCode.null_:
                return;
            case IonTypeCode.bool_:
                debug assert(0, "We don't handle bool values yet");
                else return;
            case IonTypeCode.uInt:
                IonUInt val;
                err = value.get(val);
                handleIonError(err);

                ulong l;
                err = val.get(l);
                handleIonError(err);
                auto k = CompositeKey(key, l);

                this.idx[k] ~= checksum;
                return;

            case IonTypeCode.nInt:
                IonNInt val;
                err = value.get(val);
                handleIonError(err);

                long l;
                err = val.get(l);
                handleIonError(err);
                auto k = CompositeKey(key, l);

                this.idx[k] ~= checksum;
                return;

            case IonTypeCode.float_:
                IonFloat val;
                err = value.get(val);
                handleIonError(err);

                double f;
                err = val.get(f);
                handleIonError(err);
                auto k = CompositeKey(key, f);

                this.idx[k] ~= checksum;
                return;
            case IonTypeCode.decimal:
                IonDecimal val;
                err = value.get(val);
                handleIonError(err);

                double f;
                err = val.get(f);
                handleIonError(err);
                auto k = CompositeKey(key, f);

                this.idx[k] ~= checksum;
                return;
            case IonTypeCode.symbol:
                IonSymbolID val;
                err = value.get(val);
                handleIonError(err);

                auto i = val.get;
                auto s = symbolTable[i];
                auto k = CompositeKey(key, s);

                this.idx[k] ~= checksum;
                return;
            case IonTypeCode.string:
                const(char)[] val;
                err = value.get(val);
                handleIonError(err);

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
                err = value.get(vals);
                handleIonError(err);

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
                err = value.get(obj);
                handleIonError(err);

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

    IonValue convertIonSymbols(ref VcfIonRecord rec) {
        IonDescribedValue value;
        auto err = rec.val.describe(value);
        handleIonError(err);
        IonSerializer!(nMax * 8, [], false) serializer;
        serializer.initializeNoTable;

        this.convertIonSymbols(value, rec.symbols.table[10 .. $], serializer);

        serializer.finalize;
        this.currentSymbolTable = new SymbolTable;
        auto d = this.newSymbolTable.serialize();
        this.currentSymbolTable.loadSymbolTable(d);
        return IonValue(serializer.data.dup);
    }

    void convertIonSymbols(S)(IonDescribedValue value, const(char[])[] oldSymbols, ref S serializer) {
        IonErrorCode err;
        final switch(value.descriptor.type) {
            case IonTypeCode.null_:
            case IonTypeCode.bool_:
            case IonTypeCode.uInt:
            case IonTypeCode.nInt:
            case IonTypeCode.float_:
            case IonTypeCode.decimal:
            case IonTypeCode.string:
            case IonTypeCode.clob:
            case IonTypeCode.blob:
            case IonTypeCode.timestamp:
            case IonTypeCode.sexp:
            case IonTypeCode.annotations:
                value.serialize(serializer);
                return;
            case IonTypeCode.symbol:
                IonSymbolID val;
                err = value.get(val);
                handleIonError(err);

                auto i = val.get;
                auto sym = oldSymbols[i];
                auto newSym = this.newSymbolTable.insert(sym);
                serializer.putSymbolId(newSym);
                return;
            
            case IonTypeCode.list:
                IonList vals;
                err = value.get(vals);
                handleIonError(err);
                auto l = serializer.listBegin;
                foreach (val; vals)
                {
                    convertIonSymbols(val, oldSymbols, serializer);
                }
                serializer.listEnd(l);
                return;
            case IonTypeCode.struct_:
                IonStruct obj;
                IonStructWithSymbols objWSym;
                err = value.get(obj);
                handleIonError(err);

                objWSym = obj.withSymbols(oldSymbols);
                auto s = serializer.structBegin;
                foreach (k, v; objWSym)
                {
                    auto newSym = this.newSymbolTable.insert(k);
                    serializer.putKeyId(newSym);
                    convertIonSymbols(v, oldSymbols, serializer);
                }
                serializer.structEnd(s);
                return;
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
        foreach (k; this.getIonKeys.byKey)
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


