module libmucor.atomize.field;

import libmucor.serde.ser;
import libmucor.utility;
import libmucor.atomize.header;

import std.traits;
import std.meta;
import std.bitmanip;
import std.string : fromStringz;

import mir.ser;
import memory;

import dhtslib.vcf : BcfRecordType, RecordTypeSizes;
import htslib.vcf : bcf_fmt_t, bcf_info_t;

alias RecordTypeToDType = AliasSeq!(null, byte, short, int, long, float, null, const(char)[], char);
alias MissingValues = AliasSeq!(null, 0x80, 0x8000, 0x80000000, 0x8000000000000000, 0x7F800001, null, 0, 0);

enum OutputMode {
    All,
    OnePerAllele,
    OnePerAlt,
    One,
}

struct FmtField {
    BcfRecordType type;
    const(char)[] id;
    ubyte[] data;
    OutputMode mode;
    int alleleIdx;

    this(bcf_fmt_t * fmt, const(char)[] id, int sampleIdx) @nogc @trusted nothrow {
        this.type = cast(BcfRecordType)fmt.type;
        this.id = id;
        this.data = fmt.p[sampleIdx*fmt.size .. (sampleIdx+1)*fmt.size];

        if(this.data.length == RecordTypeSizes[this.type])
            this.mode = OutputMode.One;
        else 
            this.mode = OutputMode.All;
    }

    this(bcf_fmt_t * fmt, const(char)[] id, int sampleIdx, int alleleIdx) @nogc @trusted nothrow {
        this.type = cast(BcfRecordType)fmt.type;
        this.id = id;
        this.data = fmt.p[sampleIdx*fmt.size .. (sampleIdx+1)*fmt.size];

        this.mode = OutputMode.OnePerAlt;
        this.alleleIdx = alleleIdx;
    }

    this(bcf_fmt_t * fmt, const(char)[] id, int sampleIdx, int alleleIdx, bool isPerAllele) @nogc @trusted nothrow {
        this.type = cast(BcfRecordType)fmt.type;
        this.id = id;
        this.data = fmt.p[sampleIdx*fmt.size .. (sampleIdx+1)*fmt.size];

        assert(isPerAllele);
        this.mode = OutputMode.OnePerAllele;
        this.alleleIdx = alleleIdx;
    }
}

struct InfoField {
    BcfRecordType type;
    const(char)[] id;
    ubyte[] data;
    bool isFlag;
    OutputMode mode;
    int alleleIdx = -1;

    this(bcf_info_t * info, const(char)[] id) @nogc @trusted nothrow {
        this.type = cast(BcfRecordType)info.type;
        this.data = info.vptr[0 .. info.vptr_len];
        this.id = id;
        
        if(this.data.length == RecordTypeSizes[this.type])
            this.mode = OutputMode.One;
        else 
            this.mode = OutputMode.All;
    }

    this(bcf_info_t * info, const(char)[] id, int alleleIdx) @nogc @trusted nothrow {
        this.type = cast(BcfRecordType)info.type;
        this.data = info.vptr[0 .. info.vptr_len];
        this.id = id;
        
        this.alleleIdx = alleleIdx;
        this.mode = OutputMode.OnePerAlt;
    }

    this(bcf_info_t * info, const(char)[] id, int alleleIdx, bool isPerAllele) @nogc @trusted nothrow {
        this.type = cast(BcfRecordType)info.type;
        this.data = info.vptr[0 .. info.vptr_len];
        this.id = id;
        this.alleleIdx = alleleIdx;
        assert(isPerAllele);
        this.mode = OutputMode.OnePerAllele;
    }

    this(bool _b, const(char)[] id) @nogc @safe nothrow {
        this.id = id;
        this.isFlag = true;
    }

}

/// check if value is padding
bool isMissing(T)(T val) {
    return cast(bool)((cast(ulong)val) == MissingValues[staticIndexOf!(T, RecordTypeToDType)]);
}

void serializeValueByMode(DT, OutputMode mode, T)(T field, ref VcfRecordSerializer s) @nogc @trusted nothrow {
    static if(mode == OutputMode.All) {
        s.putKey(field.id);
        auto arr = (cast(DT*)field.data.ptr)[0..field.data.length / DT.sizeof];
        serializeValue(s.serializer, arr);
    }
    else static if(mode == OutputMode.One) {
        s.putKey(field.id);
        s.putValue(*(cast(DT*)field.data.ptr));
    }
    else static if(mode == OutputMode.OnePerAllele) {
        
        DT[2] arr;
        arr[0] = *(cast(DT*)field.data.ptr);
        arr[1] = (cast(DT*)field.data.ptr)[field.alleleIdx + 1];
        s.putKey(field.id);
        serializeValue(s.serializer, arr);
    }
    else static if(mode == OutputMode.OnePerAlt) {
        s.putKey(field.id);
        s.putValue((cast(DT*)field.data.ptr)[field.alleleIdx]);
    }
}

void serializeValueByType(OutputMode mode, T)(T field, ref VcfRecordSerializer s) @nogc @trusted nothrow {
    final switch (field.type) {
        static foreach(BT; EnumMembers!BcfRecordType){
            static if(BT == BcfRecordType.Char) {}
            else static if(BT == BcfRecordType.Null) {}
            else {
                case BT:
                    field.serializeValueByMode!(RecordTypeToDType[BT], mode)(s);
                    return;
            }
        }
        static if(mode == OutputMode.All) {
            case BcfRecordType.Char:
                if(cast(char)field.data[0] == ','){
                    auto str = fromStringz(cast(char*)field.data.ptr + 1);
                    s.putKey(field.id);
                    auto l = s.listBegin;
                    auto split = findSplit(str, ',');
                    while(split[1].length != 0){
                        serializeValue(s.serializer, split[0]);
                        split = findSplit(split[1], ',');
                    }
                    serializeValue(s.serializer, split[0]);
                    s.listEnd(l);
                } else {
                    s.putKey(field.id);
                    auto arr = (cast(char*)field.data.ptr)[0..field.data.length];
                    if(!arr[$-1]) {
                        arr = fromStringz(cast(char*)field.data.ptr);
                    }
                    serializeValue(s.serializer, arr);
                }
                return;
            case BcfRecordType.Null:
                return;
        } else static if(mode == OutputMode.One) {
            case BcfRecordType.Char:
                s.putKey(field.id);
                serializeValue(s.serializer, (cast(char*)field.data.ptr)[0..1]);
                return;
            case BcfRecordType.Null:
                return;
        } else static if(mode == OutputMode.OnePerAllele) {
            case BcfRecordType.Char:
                debug assert(false, "ByAllele char?");
                else return;
            case BcfRecordType.Null:
                return;
        } else static if(mode == OutputMode.OnePerAlt) {
            case BcfRecordType.Char:
                if(cast(char)field.data[0] == ','){
                    auto str = (cast(char*)field.data.ptr)[1..field.data.length];
                    auto split = findSplit(str, ',');
                    int i = 0;
                    while(split[1].length != 0 && i < field.alleleIdx){
                        split = findSplit(split[1], ',');
                        i++;
                    }

                    assert(i == field.alleleIdx);
                    s.putKey(field.id);
                    serializeValue(s.serializer, split[0]);
                } else {
                    s.putKey(field.id);
                    auto arr = (cast(char*)field.data.ptr)[0..field.data.length];
                    if(!arr[$-1]) {
                        arr = fromStringz(cast(char*)field.data.ptr);
                    }
                    serializeValue(s.serializer, arr);
                }
                
                return;
            case BcfRecordType.Null:
                return;
        }
    }
}

void serialize(T)(T field, ref VcfRecordSerializer s) @nogc @trusted nothrow
if(is(T == InfoField) || is(T == FmtField))
{
    static if(is(T == InfoField)) {
        if(field.isFlag){
            s.putKey(field.id); 
            s.putValue(true);
            return;
        }
    }
    final switch(field.mode) {
        case OutputMode.All:
            field.serializeValueByType!(OutputMode.All)(s);
            return;
        case OutputMode.One:
            field.serializeValueByType!(OutputMode.One)(s);
            return;
        case OutputMode.OnePerAllele:
            field.serializeValueByType!(OutputMode.OnePerAllele)(s);
            return;
        case OutputMode.OnePerAlt:
            field.serializeValueByType!(OutputMode.OnePerAlt)(s);
            return;
        }   
    }