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
import std.traits : ReturnType;

struct VcfIonRecord {
    
    SymbolTable * symbols;

    IonDescribedValue val;

    this(ref SymbolTable st, IonDescribedValue val) {
        this.symbols = &st;
        this.val = val;
    }

    alias getObj this;

    auto getObj(){
        IonStruct obj;
        IonErrorCode error = val.get(obj);
        assert(!error, ionErrorMsg(error));

        return obj.withSymbols(this.symbols.table);
    }

}

/// Deserialize VCF ion
/// Data is laid out as such:
/// 
/// [ ion prefix ]
/// [ primary symbol table ]
///
///  stream of ion data: [ 
///     [ possible local symbol table ]
///     [ ion data ]        
/// ]
///     
struct VcfIonDeserializer {
    File inFile;

    SymbolTable symbols;

    ReturnType!(File.byChunk) chunks;

    const(ubyte)[] buffer;

    bool eof;
    bool empty;
    IonErrorCode error;

    VcfIonRecord frontVal;

    this(File inFile, size_t bufferSize = 4096){

        this.inFile = inFile;
        this.chunks = this.inFile.byChunk(bufferSize);

        this.buffer = this.chunks.front;

        error = readVersion(this.buffer);
        handleIonError(error);

        this.readSymbolTable();

        this.popFront;
    }

    /// set up buffer, read first chunk, and validate version/ionPrefix
    Result!(VcfIonRecord, string) front() {
        Result!(VcfIonRecord, string) ret;
        if(error) ret = Err(ionErrorMsg(error));
        else ret = Ok(frontVal);
        return ret;
    }

    void popFront(){
        
        IonDescribedValue val;
        if(this.eof && this.buffer.length == 0){
            
            this.empty = true;
            return;
        }
        import std.stdio;
        this.readSymbolTable;
        this.readValue(val);

        IonStruct obj;
        error = val.get(obj);
        handleIonError(error);
        
        this.frontVal = VcfIonRecord(this.symbols, val);   
    }

    IonErrorCode loadMoreBytes(){
        if(_expect(!this.chunks.empty, true)){
            if(buffer.length > 0) {
                this.buffer = this.buffer.dup;
                this.chunks.popFront;
                this.buffer ~= this.chunks.front;
            } else {
                this.chunks.popFront;
                this.buffer = this.chunks.front;
            }
            return IonErrorCode.none;
        }
        return IonErrorCode.eof;
    }

    void readSymbolTable(){
        auto view = buffer[];
        error = this.symbols.loadSymbolTable(view);
        while(error == IonErrorCode.unexpectedEndOfData) {
            error = this.loadMoreBytes;
            handleIonError(error);
            view = buffer[];
            error = this.symbols.loadSymbolTable(view);
        }
        import std.stdio;
        writefln("read st: %d bytes", buffer.length - view.length);
        handleIonError(error);
        this.buffer = view;
    }


    void readValue(ref IonDescribedValue val){
        auto view = buffer[];
        error = IonValue(view).describe(val);
        error = IonValue(view[1..$]).describe(val);
        while(error == IonErrorCode.unexpectedEndOfData) {
            error = this.loadMoreBytes;
            handleIonError(error);
            view = buffer[];
            error = IonValue(view).describe(val);
        }
        writefln("read val: %d bytes", buffer.length - view.length);
        handleIonError(error);
        this.buffer = view;
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