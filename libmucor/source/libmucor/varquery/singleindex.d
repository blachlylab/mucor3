module libmucor.varquery.singleindex;

import std.algorithm.setops;
import std.regex;
import std.algorithm : sort, uniq, map, std_filter = filter, canFind, joiner;
import std.math : isClose;
import std.array : array;
import std.conv : to, ConvException;
import std.traits;
import std.meta;

import asdf: deserialize, Asdf, AsdfNode, parseJson, serializeToAsdf;
import libmucor.wideint : uint128;
import libmucor.khashl;

pragma(inline,true)
bool isNumericStringInteger(string val)
{
    foreach (c; val)
    {
        if(c < '0' || c > '9') return false;
    }
    return true;
}

/// JSON types
enum TYPES{
    NULL = 0,
    FLOAT,
    INT,
    STRING,
    BOOL
}
alias DTYPES = AliasSeq!(null, double, long, string, bool);

/// Union that can store all json types
union JSONData
{
    double f;
    long i;
    string s;
    bool b;
}

/// Struct that represents JSON data
/// and can be used with a hashmap
struct JSONValue
{
    TYPES type;
    JSONData val;

    this(Asdf json)
    {
        final switch(json.kind){
            case Asdf.Kind.array:
            case Asdf.Kind.object:
                throw new Exception("Cannot store objects or arrays in JSONValue types");
            case Asdf.Kind.number:
                auto numStr = json.to!string;
                if(isNumericStringInteger(numStr)){
                    val.i = deserialize!long(json);
                    type = TYPES.INT;
                }else{
                    val.f = deserialize!double(json);
                    type = TYPES.FLOAT;
                }
                break;
            case Asdf.Kind.string:
                val.s = deserialize!string(json);
                type = TYPES.STRING;
                break;
            case Asdf.Kind.null_:
                throw new Exception("No nulls should reach this point");
            case Asdf.Kind.false_:
                type = TYPES.BOOL;
                val.b = false;
                break;
            case Asdf.Kind.true_:
                type = TYPES.BOOL;
                val.b = true;
                break;
        }
    }

    this(T)(T item)
    {
        static if(isIntegral!T)
        {
            val.i = item.to!long;
            type = TYPES.INT;
        }else static if(isFloatingPoint!T){
            val.f = item.to!double;
            type = TYPES.FLOAT;
        }else static if(isSomeString!T){
            val.s = item.to!string;
            type = TYPES.STRING;
        }else static if(isBoolean!T){
            val.b = item.to!bool;
            type = TYPES.BOOL;
        }else{
            static assert(0, "not a compatible type for JSONValue");
        }
    }

    size_t toHash() const pure nothrow
    {
        final switch(type){
            case TYPES.NULL:
                return hashOf(null);
            case TYPES.FLOAT:
                return hashOf(val.f);
            case TYPES.INT:
                return hashOf(val.i);
            case TYPES.STRING:
                return hashOf(val.s);
            case TYPES.BOOL:
                return hashOf(val.b);
        }
    }

    int opCmp(JSONValue other) const pure nothrow
    {
        if(type != other.type){
            return -1;
        }
        final switch(type){
            case TYPES.NULL:
                return 0;
            case TYPES.FLOAT:
                return val.f < other.val.f;
            case TYPES.INT:
                return val.i < other.val.i;
            case TYPES.STRING:
                return val.s < other.val.s;
            case TYPES.BOOL:
                return val.b < other.val.b;
        }
    }

    bool opEquals(JSONValue other) const pure nothrow
    {
        if(type != other.type){
            return false;
        }
        final switch(type){
            case TYPES.NULL:
                return true;
            case TYPES.FLOAT:
                return val.f == other.val.f;
            case TYPES.INT:
                return val.i == other.val.i;
            case TYPES.STRING:
                return val.s == other.val.s;
            case TYPES.BOOL:
                return val.b == other.val.b;

        }
    }

    string toString() const
    {
        final switch(type){
            case TYPES.NULL:
                return null.to!string;
            case TYPES.FLOAT:
                return val.f.to!string;
            case TYPES.INT:
                return val.i.to!string;
            case TYPES.STRING:
                return val.s;
            case TYPES.BOOL:
                return val.b.to!string;
        }
    }

    ubyte[] toBytes() const
    {
        import std.bitmanip: nativeToLittleEndian;
        assert(type != TYPES.NULL);
        ubyte[] ret = [cast(ubyte)type];
        final switch(type){
            case TYPES.NULL:
            case TYPES.FLOAT:
                ret ~= val.f.nativeToLittleEndian;
                break;
            case TYPES.STRING:
                ret ~= val.s.length.nativeToLittleEndian;
                ret ~= cast(ubyte[])val.s;
                break;
            case TYPES.INT:
                ret ~= val.i.nativeToLittleEndian;
                break;
            case TYPES.BOOL:
                ret ~= val.b.nativeToLittleEndian;
                break;
        }
        return ret;
    }
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
                    .std_filter!(x=> x in hashmap)
                    .map!(x => hashmap[x].dup)
                    .joiner.array; 
    }

    ulong[] filterRange(T)(T[] range) const
    {
        assert(range.length==2);
        assert(range[0]<=range[1]);
        JSONValue[2] r = [JSONValue(range[0]), JSONValue(range[1])];
        return hashmap.byKey
                    .std_filter!(x => x.type == staticIndexOf!(T,DTYPES))
                    .std_filter!(x=>x >= r[0])
                    .std_filter!(x=>x < r[1])
                    .map!(x => hashmap[x].dup).joiner.array;
    }

    ulong[] filterOp(string op, T)(T val) const
    {
        mixin("auto func = (JSONValue x) => x " ~ op ~" JSONValue(val);");
        return hashmap.byKey
                        .std_filter!(x => x.type == staticIndexOf!(T,DTYPES))
                        .std_filter!func
                        .map!(x => hashmap[x].dup).joiner.array
                        .sort.uniq.array;
    }
}

// unittest{
//     import std.stdio;
//     InvertedIndex idx = InvertedIndex();
//     string[] v = ["hi","I","am","t"];
//     idx.hashmap[serialize(&v[0])]=[1,2];
//     idx.hashmap[serialize(&v[1])]=[1,3];
//     idx.hashmap[serialize(&v[2])]=[4];
//     idx.hashmap[serialize(&v[3])]=[5];
//     writeln(idx.filter(["hi","I","am"]));

//     InvertedIndex idx2 = InvertedIndex(TYPES.FLOAT);
//     auto v2 = [0.1,0.4,0.6,0.9];
//     idx2.hashmap[serialize(&v2[0])]=[1,2];
//     idx2.hashmap[serialize(&v2[1])]=[1,3];
//     idx2.hashmap[serialize(&v2[2])]=[4];
//     idx2.hashmap[serialize(&v2[3])]=[5];
//     writeln(idx2.filterRange([0.1,0.8]));
// }