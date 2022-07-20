module libmucor.atomize.serde.ser;

import mir.utility: _expect;
import mir.ser: serializeValue;
import mir.ser.ion : IonSerializer, ionSerializer;
import mir.ion.symbol_table: IonSymbolTable, IonSystemSymbolTable_v1;
import mir.serde : SerdeTarget, serdeGetSerializationKeysRecurse;

import libmucor.atomize.ann;
import libmucor.atomize.serde.symbols;
import libmucor.atomize.header;
import libmucor.atomize.serde;
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
    khashl!(const(char)[], size_t, true) * sharedSymbols;
    size_t numShared;
    
    IonSymbolTable!false localSymbols;
    
    IonSerializer!(nMax * 8, [], false) serializer;

    this(ref khashl!(const(char)[], size_t, true) sharedSymbols, size_t numShared, SerdeTarget target) {
        this.sharedSymbols = &sharedSymbols;
        this.localSymbols.initialize;
        this.numShared = numShared;

        this.serializer = ionSerializer!(nMax * 8, [], false);
        this.serializer.initialize(localSymbols, target);
    }

    void putKey(scope const char[] key) //@nogc
    {
        serializer.putKeyId(localSymbols.insert(key) + numShared);
    }

    void putSymbol(scope const char[] key) //@nogc
    {
        serializer.putSymbolId(localSymbols.insert(key) + numShared);
    }

    void putSharedKey(scope const char[] key) //@nogc
    {
        serializer.putKeyId((*sharedSymbols)[key]);
    }

    void putSharedSymbol(scope const char[] key) //@nogc
    {
        serializer.putSymbolId((*sharedSymbols)[key]);
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

    auto finalize() {
        serializer.finalize;
        // use runtime table
        assert(localSymbols.initialized);
        localSymbols.finalize; 
        return () @trusted { return  cast(immutable) (ionPrefix ~ localSymbols.data ~ serializer.data); } ();
        
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
    SymbolTable sharedSymbols;
    khashl!(const(char)[], size_t, true) sharedSymbolTable;
    size_t numSharedSymbols;

    SerdeTarget target;

    this(File outfile, ref HeaderConfig hdrInfo, SerdeTarget target) {
        this.outfile = outfile;
        this(hdrInfo, target);
        writeSharedTable;
        this.target = target;
    }

    this(ref HeaderConfig hdrInfo, SerdeTarget target) {
        sharedSymbols.createFromHeaderConfig(hdrInfo);
        foreach (i,key; sharedSymbols.table)
        {
            sharedSymbolTable[cast(const(char)[])key.dup] = i;
        }
        this.target = target;
    }

    this(string[] sharedSymbols, SerdeTarget target) {
        this.sharedSymbols.createFromStrings(sharedSymbols);
        foreach (i,key; this.sharedSymbols.table)
        {
            sharedSymbolTable[cast(const(char)[])key.dup] = i;
        }
        this.target = target;
    }

    void putRecord(T)(ref T val) {
        auto serializer = VcfRecordSerializer(this.sharedSymbolTable, this.sharedSymbols.table.length, target);
        val.serialize(serializer);
        outfile.rawWrite(serializer.finalize);
    }

    void writeSharedTable() {
        outfile.rawWrite(ionPrefix ~ sharedSymbols.data);
    }
    
}

/// used for debug/testing 
auto serializeVcfToIon(T)(T val, string[] symbols = [], SerdeTarget serdeTarget = SerdeTarget.ion) {
    VcfSerializer ser = VcfSerializer(symbols, serdeTarget);
    VcfRecordSerializer serializer = VcfRecordSerializer(ser.sharedSymbolTable, ser.sharedSymbols.table.length, serdeTarget);
    val.serialize(serializer);
    return ionPrefix ~ ser.sharedSymbols.data ~ serializer.finalize;
}

/// used for debug/testing 
auto serializeVcfToIon(T)(T val, ref HeaderConfig hdrInfo, SerdeTarget serdeTarget = SerdeTarget.ion) {
    VcfSerializer ser = VcfSerializer(hdrInfo, serdeTarget);
    VcfRecordSerializer serializer = VcfRecordSerializer(ser.sharedSymbolTable, ser.sharedSymbols.table.length, serdeTarget);
    val.serialize(serializer);
    return ionPrefix ~ ser.sharedSymbols.data ~ serializer.finalize;
}