module libmucor.jsonlops.jsonvalue;

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
import libmucor.error;
import std.sumtype;
import libmucor.hts_endian;
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

alias JsonValueTypes = SumType!(const(char)[], double, long, bool);

/// Struct that represents JSON data
/// and can be used with a hashmap
struct JSONValue
{
    JsonValueTypes val;

    this(Asdf json)
    {
        final switch(json.kind){
            case Asdf.Kind.array:
            case Asdf.Kind.object:
                log_err(__FUNCTION__, "Cannot store objects or arrays in JSONValue types");
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
                log_err(__FUNCTION__, "No nulls should reach this point");
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