module libmucor.varquery.singleindex;

import std.algorithm.setops;
import std.algorithm : sort, uniq, map, std_filter = filter, canFind, joiner;
import std.math : isClose;
import std.array : array;
import std.conv : to, ConvException;
import std.traits;
import std.meta;

import asdf: deserialize, Asdf, AsdfNode, parseJson, serializeToAsdf;
import libmucor.wideint : uint128;
import libmucor.khashl;
import std.sumtype;
import htslib.hts_endian;
import std.typecons: Tuple, tuple;
import std.exception: enforce;

pragma(inline,true)
bool isNumericStringInteger(const(char)[] val)
{
    foreach (c; val)
    {
        if(c < '0' || c > '9') return false;
    }
    return true;
}

alias JsonTypes = SumType!(const(char)[], double, long, bool);

/// Struct that represents JSON data
/// and can be used with a hashmap
struct JSONValue
{
    JsonTypes val;

    this(Asdf json)
    {
        final switch(json.kind){
            case Asdf.Kind.array:
            case Asdf.Kind.object:
                throw new Exception("Cannot store objects or arrays in JSONValue types");
            case Asdf.Kind.number:
                auto numStr = json.to!string;
                if(isNumericStringInteger(numStr)){
                    val = deserialize!long(json);
                }else{
                    val = deserialize!double(json);
                }
                break;
            case Asdf.Kind.string:
                val = deserialize!string(json);
                break;
            case Asdf.Kind.null_:
                throw new Exception("No nulls should reach this point");
            case Asdf.Kind.false_:
                val = false;
                break;
            case Asdf.Kind.true_:
                val = true;
                break;
        }
    }

    this(T)(T item)
    {
        static if(isIntegral!T)
        {
            val = item.to!long;
        }else static if(isFloatingPoint!T){
            val = item.to!double;
        }else static if(isSomeString!T){
            val = item.to!string;
        }else static if(isBoolean!T){
            val = item.to!bool;
        }else{
            static assert(0, "not a compatible type for JSONValue");
        }
    }

    size_t toHash() const pure nothrow
    {
        return (this.val).match!(
            (bool x) => hashOf(x),
            (long x) => hashOf(x),
            (double x) => hashOf(x),
            (const(char)[] x) => hashOf(x)
        );
    }

    int opCmp(JSONValue other) const pure nothrow
    {
        if(this == other) return 0;
        auto ret = match!(
            (bool a, bool b) => a < b ? -1 : 1,
            (long a, long b) => a < b ? -1 : 1,
            (double a, double b) => a < b ? -1 : 1,
            (const(char)[] a, const(char)[] b) => a < b ? -1 : 1,
            (_a, _b) => -1
        )(this.val, other.val);
        return ret;
    }

    bool isSameType(T)() const pure nothrow
    {
        return this.val.match!(
            (bool _x) {
                static if(isBoolean!T)
                    return true;
                else
                    return false;
            },
            (long _x) {
                static if(isIntegral!T)
                    return true;
                else
                    return false;
            },
            (double _x) {
                static if(isFloatingPoint!T)
                    return true;
                else
                    return false;
            },
            (const(char)[] _x) {
                static if(isSomeString!T)
                    return true;
                else
                    return false;
            }
        );
    }

    bool opEquals(JSONValue other) const pure nothrow
    {
        return match!(
            (bool a, bool b) => a == b,
            (long a, long b) => a == b,
            (double a, double b) => a == b,
            (const(char)[] a, const(char)[] b) => a == b,
            (_a, _b) => false
        )(this.val, other.val);
    }

    string toString() const
    {
        return (this.val).match!(
            (bool x) => x.to!string,
            (long x) => x.to!string,
            (double x) => x.to!string,
            (const(char)[] x) => x.idup,
        );
    }
    
    ulong getType() const {
        return (this.val).match!(
            (bool x) => 0,
            (long x) => 1,
            (double x) => 2,
            (const(char)[] x) => 3
        );
    }

    ubyte[] toBytes() const
    {
        return (this.val).match!(
            (bool x) {
                ubyte[] ret = new ubyte[1];
                ret[0] = x ? 1 : 0;
                return ret;
            },
            (long x) {
                ubyte[] ret = new ubyte[8];
                i64_to_le(x, ret.ptr);
                return ret;
            },
            (double x) {
                ubyte[] ret = new ubyte[4];
                double_to_le(x, ret.ptr);
                return ret;
            },
            (const(char)[] x) {
                ubyte[] ret = new ubyte[x.length];
                ret[] = cast(ubyte[])x;
                return ret;
            }
        );
    }
}

unittest
{
    auto a = JSONValue(`1`.parseJson);
    auto b = JSONValue(`1`.parseJson);
    auto c = JSONValue(`2`.parseJson);
    auto d = JSONValue(`2.0`.parseJson);
    auto e = JSONValue(`3.0`.parseJson);
    auto f = JSONValue(`true`.parseJson);
    auto g = JSONValue(`false`.parseJson);
    auto h = JSONValue(`"a"`.parseJson);
    auto i = JSONValue(`"b"`.parseJson);

    assert(a == b);
    assert(a != c);
    assert(d != e);
    assert(f != g);
    assert(h != i);

    assert(a <= b);
    import std.stdio;
    writeln(b.getType);
    assert(b < c);
    assert(d < e);
    assert(f > g);
    assert(h < i);
    
}

/**
* Single inverted index for one field and one type.
* Holds a single field's values as keys and record 
* number as value.
*/
struct InvertedIndex
{
    // ulong[][JSONValue] hashmap;
    khashl!(JSONValue, ulong[]) hashmap;

    ulong[] filter(T)(T[] items) const
    {
        return items.map!(x => JSONValue(x))
                    .std_filter!(x => x in hashmap)
                    .map!(x => hashmap[x].dup)
                    .joiner.array; 
    }

    ulong[] filterRange(T)(T[] range) const
    {
        assert(range.length==2);
        assert(range[0]<=range[1]);
        JSONValue[2] r = [JSONValue(range[0]), JSONValue(range[1])];
        return hashmap.byKey
                    .std_filter!(x => x.isSameType!T)
                    .std_filter!(x=>x >= r[0])
                    .std_filter!(x=>x < r[1])
                    .map!(x => hashmap[x].dup).joiner.array;
    }

    ulong[] filterOp(string op, T)(T val) const
    {
        mixin("auto func = (JSONValue x) => x " ~ op ~" JSONValue(val);");
        return hashmap.byKey
                        .std_filter!(x => x.isSameType!T)
                        .std_filter!func
                        .map!(x => hashmap[x].dup).joiner.array
                        .sort.uniq.array;
    }

    auto opBinaryRight(string op)(InvertedIndex lhs)
    {
        static if(op == "+") {
            InvertedIndex ret = InvertedIndex(this.hashmap.dup);
            foreach(kv; lhs.byKeyValue) {
                auto v = kv.key in ret.hashmap;
                if(v) {
                    *v = *v ~ kv.value;
                } else {
                    ret.fields[kv.key] = kv.value;
                }
            }
            return ret;
        } else
            static assert(false, "Op not implemented");
    }
}

/** 
 *  key_type: 1, 15 padd
 *  key_offset: 8,
 *  key_length: 8,
 *
 * Total size: 48 bytes
 */
struct KeyMetaData {
    align:
    ulong keyOffset;
    ulong keyLength;
    ulong fieldOffset;
    ulong fieldLength;

    this(ulong keyOffset, ulong keyLength, ulong fieldOffset, ulong fieldLength) {
        this.keyOffset = keyOffset;
        this.keyLength = keyLength;
        this.fieldOffset = fieldOffset;
        this.fieldLength = fieldLength;

        assert(keyLength > 0);
        assert(fieldLength > 0);
    }

    this(ubyte[] data) {
        auto p = data.ptr;
        this.keyOffset = le_to_u64(p);
        p += 8;
        this.keyLength = le_to_u64(p);
        p += 8;
        this.fieldOffset = le_to_u64(p);
        p += 8;
        this.fieldLength = le_to_u64(p);
    }

    ubyte[32] serialize() {
        ubyte[32] ret;
        u64_to_le(this.keyOffset, ret.ptr + 0);
        u64_to_le(this.keyLength, ret.ptr + 8);
        u64_to_le(this.fieldOffset, ret.ptr + 16);
        u64_to_le(this.fieldLength, ret.ptr + 24);
        return ret;
    }

    auto deserialize_to_tuple(FieldKeyMetaData[] data, ubyte[] keyData) {
        auto kData = keyData[keyOffset..keyOffset+keyLength];
        auto dataForKey = data[fieldOffset..fieldOffset+fieldLength];
        alias RT = Tuple!(string, "key", FieldKeyMetaData[], "value");
        return RT(cast(string)kData, dataForKey);
    }
}

/** 
 *  key_type: 1, 15 padd
 *  key_offset: 8,
 *  key_length: 8,
 *
 * Total size: 48 bytes
 */
struct FieldKeyMetaData {
    align:
    ulong type;
    ulong padding;
    ulong keyOffset;
    ulong keyLength;
    ulong dataOffset;
    ulong dataLength;

    this(ulong type, ulong padding, ulong keyOffset, ulong keyLength, ulong dataOffset, ulong dataLength){
        this.type = type;
        this.padding = padding;
        this.keyOffset = keyOffset;
        this.keyLength = keyLength;
        this.dataOffset = dataOffset;
        this.dataLength = dataLength;
        assert(this.keyLength > 0);
        assert(this.dataLength > 0);
    }

    this(ubyte[] data) {
        auto p = data.ptr;
        this.type = le_to_u64(p);
        p += 8;
        this.padding = le_to_u64(p);
        p += 8;
        this.keyOffset = le_to_u64(p);
        p += 8;
        this.keyLength = le_to_u64(p);
        p += 8;
        this.dataOffset = le_to_u64(p);
        p += 8;
        this.dataLength = le_to_u64(p);
    }

    ubyte[48] serialize() {
        ubyte[48] ret;
        u64_to_le(this.type, ret.ptr);
        u64_to_le(0, ret.ptr + 8);
        u64_to_le(this.keyOffset, ret.ptr + 16);
        u64_to_le(this.keyLength, ret.ptr + 24);
        u64_to_le(this.dataOffset, ret.ptr + 32);
        u64_to_le(this.dataLength, ret.ptr + 40);
        return ret;
    }

    auto deserialize_to_tuple(ulong[] data, ubyte[] keyData) {
        auto kData = keyData[keyOffset..keyOffset+keyLength];
        auto dataForKey = data[dataOffset..dataOffset+dataLength];
        alias RT = Tuple!(JSONValue, "key", ulong[], "value");
        switch(type) {
            case 0: // bool
                return RT(JSONValue(le_to_i64(kData.ptr)), dataForKey);
            case 1: // long
                return RT(JSONValue(le_to_i64(kData.ptr)), dataForKey);
            case 2: // double
                return RT(JSONValue(le_to_double(kData.ptr)), dataForKey);
            case 3: // string
                return RT(JSONValue(cast(const(char)[])kData), dataForKey);
            default: 
                throw new Exception("Error deserializing key");
        }
    }
}

unittest{
    auto field = FieldKeyMetaData(2, 0, 2, 1, 3, 5);
    auto data = field.serialize;
    assert(cast(ulong[])data == [2, 0, 2, 1, 3, 5]);
    assert(FieldKeyMetaData(data) == field);
}

unittest{
    auto field = KeyMetaData(2, 1, 3, 5);
    auto data = field.serialize;
    assert(cast(ulong[])data == [2, 1, 3, 5]);
    assert(KeyMetaData(data) == field);
}