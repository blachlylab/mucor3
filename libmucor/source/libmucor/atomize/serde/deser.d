module libmucor.atomize.serde.deser;

import mir.ion.exception;
import mir.ion.type_code;
import mir.ion.value;
import mir.utility: _expect;
import mir.appender: ScopedBuffer;
import mir.ion.symbol_table;
import mir.ion.stream;

import libmucor.atomize.serde;

import option;
import std.stdio;

/// mixin for trying to parse ion data
/// if we encounter unexpectedEndOfData error, 
/// increase the buffer and try to reparse
template tryRead(string ins, string handleError = "assert(!error, ionErrorMsg(error))") {
    enum tryRead = "
    error = "~ ins~";     
    while(error == IonErrorCode.unexpectedEndOfData){
        writeln(\"extending\");
        ubyte[4096] tmpBuf;
        ubyte[] tmp = tmpBuf[];
        writeln(tmp.length);
        tmp = inFile.rawRead(tmp);
        writeln(tmp.length);
        this.buffer ~= tmp;
        writeln(this.buffer.length);
        this.bufferView = buffer;
        error = " ~ ins ~";
        // if(tmp.length < 4096){
        //     this.eof = true;
        //     break;
        // }
    }
    this.buffer = bufferView;
    "~handleError~";";
}

struct VcfIonRecord {
    
    SymbolTable * sharedSymbolTable;
    
    SymbolTable localSymbols;

    IonDescribedValue val;

    this(ref SymbolTable sst, ref SymbolTable lst, IonDescribedValue val) {
        this.sharedSymbolTable = &sst;
        this.localSymbols = lst;
        this.val = val;
    }

    alias getObj this;

    auto getObj(){
        IonStruct obj;
        IonErrorCode error = val.get(obj);
        assert(!error, ionErrorMsg(error));

        return obj.withSymbols(this.sharedSymbolTable.table ~ localSymbols.table);
    }

    auto serializeLocalSymbols() {
        IonSymbolTable!false tmptable;
        tmptable.initialize;
        foreach (name; localSymbols.table[10..$])
        {
            tmptable.insert(name);
        }
        tmptable.finalize;
        return tmptable.data.dup;
    }
}

/// Deserialize VCF ion
/// Data is laid out as such:
/// 
/// [ ion prefix ]
/// [ shared symbol table ]
///
///  stream of ion data: [ 
///     [ ion prefix ]
///     [ local symbol table ]
///     [ ion data ]        
/// ]
///     
struct VcfIonDeserializer {
    File inFile;
    SymbolTable sharedSymbolTable;

    size_t bufferSize;
    ubyte[] buffer;
    const(ubyte)[] bufferView;
    bool eof;
    bool empty;
    IonErrorCode error;

    VcfIonRecord frontVal;

    this(File inFile, size_t bufferSize = 4096){

        this.inFile = inFile;
        this.bufferSize = bufferSize;
        this.initialize();

        assert(!error, ionErrorMsg(error));
        writefln("reading shared table: %d", bufferView.length);
        tryReadSharedSymbolTable(this.sharedSymbolTable);
        this.popFront;
    }

    /// set up buffer, read first chunk, and validate version/ionPrefix
    void initialize() {
        buffer.length = bufferSize;

        bufferView = cast(const(ubyte)[]) this.inFile.rawRead(buffer);

        writefln("reading version: %d", bufferView.length);
        error = readVersion(this.bufferView);
    }

    /// set up buffer, read first chunk, and validate version/ionPrefix
    Result!(VcfIonRecord, string) front() {
        Result!(VcfIonRecord, string) ret;
        if(error) ret = Err(ionErrorMsg(error));
        else ret = Ok(frontVal);
        return ret;
    }

    void popFront(){
        SymbolTable localSymbols;
        IonDescribedValue val;

        if(this.eof && this.bufferView.length == 0){
            
            this.empty = true;
            return;
        }
        
        writefln("reading version: %d", bufferView.length);
        error = readVersion(this.bufferView);
        

        writefln("reading local table: %d", bufferView.length);
        tryReadSymbolTable(localSymbols);
        
        writefln("reading value: %d", bufferView.length);
        tryReadValue(val);
        writeln(bufferView.length);

        this.frontVal = VcfIonRecord(this.sharedSymbolTable, localSymbols, val);
        foreach (key, val; this.frontVal.getObj)
        {
            writefln("%s: %s", key, val);
        }
        
        
    }

    void tryReadSharedSymbolTable(ref SymbolTable sym) {
        IonDescriptor des;
        error = parseValueDescriptor(this.bufferView, des);
        assert(!error, ionErrorMsg(error));

        error = ensureLoaded(des.L);
        assert(!error, ionErrorMsg(error));

        error = sym.loadSymbolTable(bufferView);
        sym.data = cast(const(ubyte)[])sym.data.dup;

        assert(!error, ionErrorMsg(error));
    }

    void tryReadSymbolTable(ref SymbolTable sym) {
        IonDescriptor des;
        error = parseValueDescriptor(this.bufferView, des);
        assert(!error, ionErrorMsg(error));

        error = ensureLoaded(des.L);
        assert(!error, ionErrorMsg(error));

        // auto data = cast(const(ubyte)[]) bufferView[0 .. des.L + 1].dup;
        error = sym.loadSymbolTable(bufferView);
        // bufferView = bufferView[des.L + 1 .. $];
        sym.data = cast(const(ubyte)[])sym.data.dup;

        assert(!error, ionErrorMsg(error));
    }

    void tryReadValue(ref IonDescribedValue val) {
        IonDescriptor des;
        error = parseValueDescriptor(this.bufferView, des);
        assert(!error, ionErrorMsg(error));
        
        error = ensureLoaded(des.L);
        assert(!error, ionErrorMsg(error));

        error = parseValue(bufferView, val);
        val.data = cast(const(ubyte)[])val.data.dup;
        assert(!error, ionErrorMsg(error));
    }

    IonErrorCode ensureLoaded(uint size) {
        
        if(size > bufferView.length) {
            writefln("Loading more data, have %d, need %d", bufferView.length, size);
            writeln(bufferView);
            /// move remaining to beginning
            buffer[0 .. bufferView.length] = bufferView.dup[];

            /// increase buffer size if needed
            if(size > buffer.length) 
                buffer.length = size + (4096 - (size % 4096));

            /// read new bytes
            auto tmp = inFile.rawRead(buffer[bufferView.length .. $]);

            if(tmp.length < buffer[bufferView.length .. $].length) {
                this.eof = true;
            }

            /// if returned is too short, err
            if(_expect(bufferView.length + tmp.length < size, false)) 
                return IonErrorCode.unexpectedEndOfData;

            /// reset view
            bufferView = buffer[0 .. tmp.length + bufferView.length];
            writefln("Loaded: %d", bufferView.length);
            writeln(bufferView);
        }
        return IonErrorCode.none;
    }
}

auto vcfIonToText(const(ubyte)[] data){
    import mir.ser.text;
    import mir.ser.unwrap_ids;
    import std.array : appender;

    SymbolTable sharedSymbolTable;
    SymbolTable localSymbols;
    IonErrorCode error;
    IonDescribedValue val;
    IonStruct obj;

    auto d = data;
    error = readVersion(d);
    if(error) throw new Exception(ionErrorMsg(error));
    error = sharedSymbolTable.loadSymbolTable(d);
    if(error) throw new Exception(ionErrorMsg(error));
    error = readVersion(d);
    if(error) throw new Exception(ionErrorMsg(error));
    error = localSymbols.loadSymbolTable(d);
    if(error) throw new Exception(ionErrorMsg(error));
    error = parseValue(d, val);
    if(error) throw new Exception(ionErrorMsg(error));
    error = val.get(obj);
    if(error) throw new Exception(ionErrorMsg(error));

    auto table = sharedSymbolTable.table ~ localSymbols.table;

    auto buffer = appender!string;
    auto ser = textSerializer!""(&buffer);
    auto unwrappedSer = unwrapSymbolIds(ser, table);
    obj.serialize(unwrappedSer);
    return buffer.data;
}

auto vcfIonToText(IonStructWithSymbols data){
    import mir.ser.text;
    import mir.ser.unwrap_ids;
    import std.array : appender;

    auto buffer = appender!string;
    auto ser = textSerializer!""(&buffer);
    auto unwrappedSer = unwrapSymbolIds(ser, data.symbolTable);
    data.serialize(unwrappedSer);
    return buffer.data;
}

IonErrorCode readVersion(ref const(ubyte)[] buffer) {
    IonVersionMarker versionMarker;
    auto error = buffer.parseVersion(versionMarker);
    if (!error)
    {
        if (versionMarker != IonVersionMarker(1, 0))
        {
            error = IonErrorCode.unexpectedVersionMarker;
        }
    }
    return error;
}

IonErrorCode parseValueDescriptor()(const(ubyte)[] data, scope ref IonDescriptor descriptor)
@safe pure nothrow @nogc
{
    version (LDC) pragma(inline, true);

    if (_expect(data.length == 0, false))
        return IonErrorCode.unexpectedEndOfData;
    auto descriptorPtr = &data[0];
    ubyte descriptorData = *descriptorPtr;

    if (_expect(descriptorData > 0xEE, false))
        return IonErrorCode.illegalTypeDescriptor;

    descriptor = IonDescriptor(descriptorPtr);

    // if null
    if (descriptor.L == 0xF)
        return IonErrorCode.none;
    // if bool
    if (descriptor.type == IonTypeCode.bool_)
    {
        if (_expect(descriptor.L > 1, false))
            return IonErrorCode.illegalTypeDescriptor;
        return IonErrorCode.none;
    }
    // if large
    bool sortedStruct = descriptorData == 0xD1;
    if (descriptor.L == 0xE || sortedStruct)
    {
        auto d = data[1..$];
        if (auto error = parseVarUInt(d, descriptor.L))
            return error;
    }
    // NOP Padding
    return descriptor.type == IonTypeCode.null_ ? IonErrorCode.nop : IonErrorCode.none;
}

IonErrorCode loadValueData()(ref const(ubyte)[] data, scope ref IonDescribedValue describedValue)
@safe pure nothrow @nogc
{
    if (_expect(describedValue.length > data.length, false))
        return IonErrorCode.unexpectedEndOfData;
    describedValue.data = data[0 .. length];
    data = data[length .. $];
    return IonErrorCode.none;
}

IonErrorCode parseValue()(ref const(ubyte)[] data, scope ref IonDescribedValue describedValue)
@safe pure nothrow @nogc
{
    version (LDC) pragma(inline, true);

    if (_expect(data.length == 0, false))
        return IonErrorCode.unexpectedEndOfData;
    auto descriptorPtr = &data[0];
    data = data[1 .. $];
    ubyte descriptorData = *descriptorPtr;

    if (_expect(descriptorData > 0xEE, false))
        return IonErrorCode.illegalTypeDescriptor;

    describedValue = IonDescribedValue(IonDescriptor(descriptorPtr));

    const L = describedValue.descriptor.L;
    const type = describedValue.descriptor.type;
    // if null
    if (L == 0xF)
        return IonErrorCode.none;
    // if bool
    if (type == IonTypeCode.bool_)
    {
        if (_expect(L > 1, false))
            return IonErrorCode.illegalTypeDescriptor;
        return IonErrorCode.none;
    }
    size_t length = L;
    // if large
    bool sortedStruct = descriptorData == 0xD1;
    if (length == 0xE || sortedStruct)
    {
        if (auto error = parseVarUInt(data, length))
            return error;
    }
    if (_expect(length > data.length, false))
        return IonErrorCode.unexpectedEndOfData;
    describedValue.data = data[0 .. length];
    data = data[length .. $];
    // NOP Padding
    return type == IonTypeCode.null_ ? IonErrorCode.nop : IonErrorCode.none;
}

package IonErrorCode parseVersion(ref const(ubyte)[] data, scope ref IonVersionMarker versionMarker)
@safe pure nothrow @nogc
{
    version (LDC) pragma(inline, true);
    if (data.length < 4 || data[0] != 0xE0 || data[3] != 0xEA)
        return IonErrorCode.cantParseValueStream;
    versionMarker = IonVersionMarker(data[1], data[2]);
    data = data[4 .. $];
    return IonErrorCode.none;
}

package IonErrorCode parseVarUInt(bool checkInput = true, U)(scope ref const(ubyte)[] data, scope out U result)
@safe pure nothrow @nogc
    if (is(U == ubyte) || is(U == ushort) || is(U == uint) || is(U == ulong))
{
    version (LDC) pragma(inline, true);
    enum mLength = U(1) << (U.sizeof * 8 / 7 * 7);
    for(;;)
    {
        static if (checkInput)
        {
            if (_expect(data.length == 0, false))
                return IonErrorCode.unexpectedEndOfData;
        }
        else
        {
            assert(data.length);
        }
        ubyte b = data[0];
        data = data[1 .. $];
        result <<= 7;
        result |= b & 0x7F;
        if (cast(byte)b < 0)
            return IonErrorCode.none;
        static if (checkInput)
        {
            if (_expect(result >= mLength, false))
                return IonErrorCode.overflowInParseVarUInt;
        }
        else
        {
            assert(result < mLength);
        }
    }
}