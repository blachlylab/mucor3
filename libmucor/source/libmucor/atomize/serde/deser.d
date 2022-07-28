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
        ubyte[4096] tmpBuf;
        ubyte[] tmp = tmpBuf[];
        tmp = inFile.rawRead(tmp);
        this.buffer ~= tmp;
        this.bufferView = buffer;
        error = " ~ ins ~";
    }
    this.buffer = bufferView;
    "~handleError~";";
}

struct VcfIonRecord {
    
    SymbolTable * sharedSymbolTable;
    
    SymbolTable * localSymbols;

    IonStructWithSymbols obj;

    this(ref SymbolTable sst, ref SymbolTable lst, ref IonStructWithSymbols val) {
        this.sharedSymbolTable = &sst;
        this.localSymbols = &lst;
        this.obj = val;
    }

    alias obj this;
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
    const(ubyte)[] buffer;
    const(ubyte)[] bufferView;
    bool eof;
    IonErrorCode error;

    VcfIonRecord frontVal;

    this(File inFile, size_t bufferSize = 4096){

        this.inFile = inFile;
        this.bufferSize = bufferSize;
        this.initialize();

        assert(!error, ionErrorMsg(error));
        mixin(tryRead!"this.sharedSymbolTable.loadSymbolTable(bufferView)");

        this.popFront;
    }

    /// set up buffer, read first chunk, and validate version/ionPrefix
    void initialize() {
        buffer.length = bufferSize;

        buffer = cast(ubyte[]) this.inFile.rawRead(cast(ubyte[])buffer);

        error = readVersion(this.buffer);

        this.bufferView = this.buffer;
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
        error = readVersion(this.buffer);
        if(this.inFile.eof && this.buffer.length == 0){
            
            this.eof = true;
            return;
        }
        
        mixin(tryRead!("localSymbols.loadSymbolTable(this.bufferView)", "if(_expect(error, false)) return"));
        
        mixin(tryRead!("parseValue(bufferView, val)", "if(_expect(error, false)) return"));

        IonStruct obj;
        error = val.get(obj);
        
        if(_expect(error, false))
            return;

        auto objWsym = obj.withSymbols(this.sharedSymbolTable.table ~ localSymbols.table);

        this.frontVal = VcfIonRecord(this.sharedSymbolTable, localSymbols, objWsym);
        
    }

    bool empty() {
        return this.eof;
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