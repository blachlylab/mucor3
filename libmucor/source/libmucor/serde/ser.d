module libmucor.serde.ser;

import mir.utility: _expect;
import mir.ser: serializeValue;
import mir.ser.ion : IonSerializer, ionSerializer;
import mir.ion.symbol_table: IonSymbolTable, IonSystemSymbolTable_v1;
import mir.serde : SerdeTarget, serdeGetSerializationKeysRecurse;

import libmucor.atomize.ann;
import libmucor.serde.symbols;
import libmucor.atomize.header;
import libmucor.serde;
import libmucor.khashl;

import std.stdio;

enum nMax = 4096u;

/// Serialize a vcf record
/// Uses a global/shared VCF symbol table
/// and a local symbol table for the record
/// 
/// When this is serialized we serialize as such:
/// [ion 4 byte prefix] + [local symbol table ] + [ion data]
struct VcfRecordSerializer {
    SymbolTableBuilder * symbols;
    
    IonSerializer!(nMax * 8, [], false) serializer;
    SerdeTarget target;

    this(ref SymbolTableBuilder symbols, SerdeTarget target) {
        this.symbols = &symbols;
        this.target = target;

        this.initialize;
    }

    void initialize() {
        this.serializer.initializeNoTable(target);
    }

    void putKey(scope const char[] key) //@nogc
    {
        serializer.putKeyId(symbols.insert(key));
    }

    void putSymbol(scope const char[] key) //@nogc
    {
        serializer.putSymbolId(symbols.insert(key));
    }

    void putValue(V)(V val) {
        return serializer.putValue(val);
    }

    auto structBegin() {
        return serializer.structBegin();
    }

    void structEnd(size_t state) {
        return serializer.structEnd(state);
    }

    auto listBegin() {
        return serializer.listBegin();
    }

    void listEnd(size_t state) {
        return serializer.listEnd(state);
    }

    const(ubyte)[] finalize() {
        import std.stdio;
        serializer.finalize;
        auto symData = symbols.serialize;
        return () @trusted { return symData ~ serializer.data; } ();
    }

}

/// Serialize multiple vcf records to a file
/// in ion format
/// Uses a global/shared VCF symbol table
/// 
/// When this is serialized we serialize as such:
/// [ion 4 byte prefix] + [shared symbol table ] + [ion data records]
struct VcfSerializer {
    File outfile;
    SymbolTableBuilder symbols;

    SerdeTarget target;

    VcfRecordSerializer recSerializer;

    this(File outfile, ref HeaderConfig hdrInfo, SerdeTarget target) {
        this.outfile = outfile;
        initializeTableFromHeader(hdrInfo);
        writeSharedTable;
        this.target = target;

        this.recSerializer = VcfRecordSerializer(this.symbols, target);
    }

    this(ref HeaderConfig hdrInfo, SerdeTarget target) {
        initializeTableFromHeader(hdrInfo);
        this.target = target;

        this.recSerializer = VcfRecordSerializer(this.symbols, target);
    }

    this(string[] symbols, SerdeTarget target) {
        initializeTableFromStrings(symbols);
        this.target = target;

        this.recSerializer = VcfRecordSerializer(this.symbols, target);
    }

    this(File outfile, string[] symbols, SerdeTarget target) {
        this.outfile = outfile;
        initializeTableFromStrings(symbols);
        writeSharedTable;
        this.target = target;

        this.recSerializer = VcfRecordSerializer(this.symbols, target);
    }

    void initializeTableFromHeader(ref HeaderConfig hdrInfo) {
        symbols.insert("CHROM");
        symbols.insert("POS");
        symbols.insert("ID");
        symbols.insert("REF");
        symbols.insert("ALT");
        symbols.insert("QUAL");
        symbols.insert("FILTER");
        symbols.insert("INFO");
        symbols.insert("checksum");
        symbols.insert("sample");
        if(hdrInfo.fmts.byAllele.names.length > 0 || hdrInfo.fmts.other.names.length > 0)
            symbols.insert("FORMAT");
        if(hdrInfo.fmts.byAllele.names.length > 0 || hdrInfo.infos.byAllele.names.length > 0)
            symbols.insert("byAllele");
        foreach (name; hdrInfo.infos.byAllele.names)
        {
            symbols.insert(name);
        }

        foreach (name; hdrInfo.infos.other.names)
        {
            symbols.insert(name);
        }

        foreach (name; hdrInfo.infos.annotations.names)
        {
            if(name == "ANN"){
                
                import mir.serde : SerdeTarget, serdeGetSerializationKeysRecurse;
                enum annFields = serdeGetSerializationKeysRecurse!Annotation.removeSystemSymbols;
                static foreach (const(string) key; annFields)
                {
                    symbols.insert(key);    
                }   
            }
            symbols.insert(name);
        }

        foreach (name; hdrInfo.fmts.byAllele.names)
        {
            symbols.insert(name);
        }

        foreach (name; hdrInfo.fmts.other.names)
        {
            symbols.insert(name);
        }
        foreach (f; hdrInfo.filters){
            symbols.insert(f);
        }
        foreach (sam; hdrInfo.samples)
        {
            symbols.insert(sam);
        }
    }

    void initializeTableFromStrings(string[] sharedSymbols) {
        foreach (key; sharedSymbols)
        {
            symbols.insert(key);
        }
    }

    // this(const(ubyte)[] sharedSymbolData, SerdeTarget target) {
    //     this.sharedSymbols.loadSymbolTable(sharedSymbolData);
    //     this.target = target;
    // }

    // this(File outfile, const(ubyte)[] sharedSymbolData, SerdeTarget target) {
    //     this.outfile = outfile;
    //     this.sharedSymbols.loadSymbolTable(sharedSymbolData);
    //     this.target = target;
    // }

    void putRecord(T)(ref T val) {
        this.recSerializer.initialize;
        val.serialize(this.recSerializer);
        outfile.rawWrite(this.recSerializer.finalize);
    }

    void putData(const(ubyte)[] d) {
        outfile.rawWrite(d);
    }

    void writeSharedTable() {
        outfile.rawWrite(ionPrefix ~ symbols.serialize);
    }
    
}

/// used for debug/testing 
auto serializeVcfToIon(T)(T val, string[] symbols = [], SerdeTarget serdeTarget = SerdeTarget.ion) {
    VcfSerializer ser = VcfSerializer(symbols, serdeTarget);
    val.serialize(ser.recSerializer);
    return ionPrefix ~ ser.recSerializer.finalize;
}

/// used for debug/testing 
auto serializeVcfToIon(T)(T val, ref HeaderConfig hdrInfo, SerdeTarget serdeTarget = SerdeTarget.ion) {
    VcfSerializer ser = VcfSerializer(hdrInfo, serdeTarget);
    val.serialize(ser.recSerializer);
    return ionPrefix ~ ser.recSerializer.finalize;
}