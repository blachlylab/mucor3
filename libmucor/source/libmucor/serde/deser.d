module libmucor.serde.deser;

import mir.ion.exception;
import mir.ion.type_code;
import mir.ion.value;
import mir.utility : _expect;
import mir.appender : ScopedBuffer;
import mir.ion.symbol_table;
import mir.ion.stream;

import libmucor.serde;

import option;
import std.stdio;
import std.traits : ReturnType;
import std.container : Array;
import core.sync.mutex : Mutex;
import core.stdc.stdlib : malloc, free;

struct VcfIonRecord
{

    SymbolTable* symbols;

    IonValue val;
    IonDescribedValue des;

    this(SymbolTable* st, IonValue val)
    {
        this.val = val;
        this.symbols = st;
        auto err = val.describe(des);
        handleIonError(err);
    }

    alias getObj this;

    auto getObj()
    {
        IonStruct obj;
        IonErrorCode error = des.get(obj);
        assert(!error, ionErrorMsg(error));

        return obj;
    }

    auto toBytes()
    {
        return this.val.data;
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
struct VcfIonDeserializer
{
    Bgzf inFile;

    Array!(char) fn;

    Array!(ubyte) buffer;

    SymbolTable* symbols;

    bool empty;
    IonErrorCode error;

    Mutex m;

    this(string fn, size_t bufferSize = 4096)
    {
        this.m = new Mutex;
        this.fn = Array!(char)(cast(char[])fn);
        this.fn ~= '\0';

        char[2] mode = ['r','\0'];

        this.inFile = Bgzf(bgzf_open(this.fn.data.ptr, mode.ptr));

        this.symbols = new SymbolTable;

        error = readVersion();
        handleIonError(error);

        IonDescriptor des;
        error = parseDescriptor(des);
        handleIonError(error);
        
        auto len = this.buffer.length;
        this.buffer.length = len + des.L;

        auto n = bgzf_read(inFile, this.buffer.data[len .. $].ptr, this.buffer.length - len); 
        if(n != this.buffer.length - len) handleIonError(IonErrorCode.unexpectedEndOfData);

        auto view = cast(const(ubyte)[])this.buffer.data();
        error = this.symbols.loadSymbolTable(view);
        handleIonError(error);

        this.popFront;
    }

    /// set up buffer, read first chunk, and validate version/ionPrefix
    Result!(VcfIonRecord, string) front()
    {
        Result!(VcfIonRecord, string) ret;
        if (error)
            ret = Err(ionErrorMsg(error));
        else
            ret = Ok(VcfIonRecord(this.symbols, IonValue(this.buffer.data.dup)));
        return ret;
    }

    void popFront()
    {

        this.buffer.length = 0;
        IonDescriptor des;
        error = parseDescriptor(des);
        if (_expect(error == IonErrorCode.eof, false))
        {
            this.empty = true;
            return;
        }
        handleIonError(error);

        if (des.type == IonTypeCode.annotations)
        {
            auto len = this.buffer.length;
            this.buffer.length = len + des.L;

            auto n = bgzf_read(inFile, this.buffer.data[len .. $].ptr, this.buffer.length - len); 
            if(n != this.buffer.length - len) handleIonError(IonErrorCode.unexpectedEndOfData);

            auto view = cast(const(ubyte)[])this.buffer.data();
            error = this.symbols.loadSymbolTable(view);
            handleIonError(error);

            this.buffer.length = 0;

            error = parseDescriptor(des);
            handleIonError(error);
        }

        auto len = this.buffer.length;
        this.buffer.length = len + des.L;

        auto n = bgzf_read(inFile, this.buffer.data[len .. $].ptr, this.buffer.length - len); 
        if(n != this.buffer.length - len) handleIonError(IonErrorCode.unexpectedEndOfData);
    }

    IonErrorCode readVersion()
    {
        IonVersionMarker versionMarker;
        ubyte[4] buf;
        auto n = bgzf_read(inFile, buf.ptr, 4);
        if(_expect(n != 4, false)) {
            return IonErrorCode.unexpectedEndOfData;
        }
        auto error = parseVersion(buf[],versionMarker);
        if (!error)
        {
            if (versionMarker != IonVersionMarker(1, 0))
            {
                error = IonErrorCode.unexpectedVersionMarker;
            }
        }
        return error;
    }

    IonErrorCode parseDescriptor()(scope ref IonDescriptor descriptor) @trusted nothrow @nogc
    {
        version (LDC) pragma(inline, true);
        auto res = bgzf_getc(inFile);
        if (_expect(res == -1, false))
            return IonErrorCode.eof;
        ubyte descriptorData = cast(ubyte) res;

        if (_expect(descriptorData > 0xEE, false))
            return IonErrorCode.illegalTypeDescriptor;
        
        this.buffer ~= descriptorData;
        descriptor = IonDescriptor(&descriptorData);

        const L = descriptor.L;
        const type = descriptor.type;
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
        // if large
        bool sortedStruct = descriptorData == 0xD1;
        if (L == 0xE || sortedStruct)
        {
            if (auto error = parseVarUInt(descriptor.L))
                return error;
        }
        return IonErrorCode.none;
    }

    package IonErrorCode parseVersion(const(ubyte)[] data, scope ref IonVersionMarker versionMarker) @safe pure nothrow @nogc
    {
        version (LDC) pragma(inline, true);
        if (data.length < 4 || data[0] != 0xE0 || data[3] != 0xEA)
            return IonErrorCode.cantParseValueStream;
        versionMarker = IonVersionMarker(data[1], data[2]);
        return IonErrorCode.none;
    }

    package IonErrorCode parseVarUInt(bool checkInput = true, U)(scope out U result) @trusted nothrow @nogc
            if (is(U == ubyte) || is(U == ushort) || is(U == uint) || is(U == ulong))
    {
        version (LDC) pragma(inline, true);
        enum mLength = U(1) << (U.sizeof * 8 / 7 * 7);
        for (;;)
        {
            ubyte b = cast(ubyte)bgzf_getc(inFile);
            this.buffer ~= b;
            result <<= 7;
            result |= b & 0x7F;
            if (cast(byte) b < 0)
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

}

auto vcfIonToText(IonStructWithSymbols data)
{
    import mir.ser.text;
    import mir.ser.unwrap_ids;
    import std.array : appender;

    auto buffer = appender!string;
    auto ser = textSerializer!""(&buffer);
    auto unwrappedSer = unwrapSymbolIds(ser, data.symbolTable);
    data.serialize(unwrappedSer);
    return buffer.data;
}

auto vcfIonToJson(IonStructWithSymbols data)
{
    import mir.ser.json;
    import mir.ser.unwrap_ids;
    import std.array : appender;

    auto buffer = appender!string;
    auto ser = jsonSerializer!""(&buffer);
    auto unwrappedSer = unwrapSymbolIds(ser, data.symbolTable);
    data.serialize(unwrappedSer);
    return buffer.data;
}

IonErrorCode readVersion(ref const(ubyte)[] buffer)
{
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

IonErrorCode parseDescriptor()(const(ubyte)[] data, scope ref IonDescriptor descriptor) @safe pure nothrow @nogc
{
    auto len = data.length;
    version (LDC) pragma(inline, true);

    if (_expect(data.length == 0, false))
        return IonErrorCode.unexpectedEndOfData;
    auto descriptorPtr = &data[0];
    data = data[1 .. $];
    ubyte descriptorData = *descriptorPtr;

    if (_expect(descriptorData > 0xEE, false))
        return IonErrorCode.illegalTypeDescriptor;

    descriptor = IonDescriptor(descriptorPtr);

    const L = descriptor.L;
    const type = descriptor.type;
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
    // if large
    bool sortedStruct = descriptorData == 0xD1;
    if (L == 0xE || sortedStruct)
    {
        if (auto error = parseVarUInt(data, descriptor.L))
            return error;
        descriptor.L += cast(uint)(len - data.length);
    }
    return IonErrorCode.none;
}

IonErrorCode parseValue()(ref const(ubyte)[] data, scope ref IonDescribedValue describedValue) @safe pure nothrow @nogc
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

package IonErrorCode parseVersion(ref const(ubyte)[] data, scope ref IonVersionMarker versionMarker) @safe pure nothrow @nogc
{
    version (LDC) pragma(inline, true);
    if (data.length < 4 || data[0] != 0xE0 || data[3] != 0xEA)
        return IonErrorCode.cantParseValueStream;
    versionMarker = IonVersionMarker(data[1], data[2]);
    data = data[4 .. $];
    return IonErrorCode.none;
}

package IonErrorCode parseVarUInt(bool checkInput = true, U)(
        scope ref const(ubyte)[] data, scope out U result) @safe pure nothrow @nogc
        if (is(U == ubyte) || is(U == ushort) || is(U == uint) || is(U == ulong))
{
    version (LDC) pragma(inline, true);
    enum mLength = U(1) << (U.sizeof * 8 / 7 * 7);
    for (;;)
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
        if (cast(byte) b < 0)
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
