module varquery.singleindex;

import std.algorithm.setops;
import std.regex;
import std.algorithm : sort, uniq, map, std_filter = filter, canFind;
import std.math : isClose;
import std.array : array;
import std.conv : to;

import asdf: deserializeAsdf = deserialize, Asdf, AsdfNode, parseJson, serializeToAsdf;
import varquery.wideint : uint128;


/// JSON types
enum TYPES{
    FLOAT,
    INT,
    STRING,
    BOOL,
    NULL
}

pragma(inline,true)
const(byte)[] serialize(T)(T* v){
    return (cast(const(byte)*) v)[0 .. T.sizeof].dup;
}

pragma(inline,true)
const(byte)[] serialize(string* v){
    return cast(const(byte)[])((*v).idup);
}

pragma(inline,true)
T deserialize(T)(const(byte)[] v){
    return *(cast(T*)(v[0 .. T.sizeof].ptr));
}

pragma(inline,true)
T deserialize(T:string)(const(byte)[] v){
    return cast(string)(v);
}

struct InvertedIndex{
    ulong[][const(byte)[]] hashmap;
    TYPES type;
    this(TYPES type){
        this.type=type;
    }

    ulong[] filter(T:string)(T[] items){
        assert(type==TYPES.STRING);
        ulong[] ret;
        foreach (item; items)
        {
            if(serialize(&item) in hashmap) 
                ret~=hashmap[serialize(&item)];
        } 
        return ret;
    }

    ulong[] filter(T:int)(T[] items){
        assert(type==TYPES.FLOAT);
        ulong[] ret;
        auto vals = hashmap.keys
                            .map!(x=>deserialize!T(x))
                            .std_filter!(x=> items.canFind(x))
                            .array;
        foreach (item; vals)
        {
            if(serialize(&item) in hashmap)
                ret~=hashmap[serialize(&item)];
        }
        return ret;
    }
    
    ulong[] filter(T:float)(T[] items){
        assert(type==TYPES.FLOAT);
        ulong[] ret;
        auto vals = hashmap.keys
                            .map!(x=>deserialize!T(x))
                            .std_filter!(x=> items.canFind!isClose(x))
                            .array;
        foreach (item; vals)
        {
            if(serialize(&item) in hashmap)
                ret~=hashmap[serialize(&item)];
        }
        return ret;
    }

    ulong[] filterRange(T)(T[] range){
        assert(range.length==2);
        assert(range[0]<=range[1]);
        auto vals = hashmap.keys
                            .map!(x=>deserialize!T(x))
                            .std_filter!(x=>x>=range[0])
                            .std_filter!(x=>x<range[1]).array;
        ulong[] ret;
        foreach (item; vals)
        {
            if(serialize(&item) in hashmap)
                ret~=hashmap[serialize(&item)];
        }
        return ret;
    }

    ulong[] filterOp(string op)(float val){
        mixin("auto func = (float x) => x " ~ op ~" val;");
        auto vals = hashmap.keys
                        .map!(x=>deserialize!float(x))
                        .std_filter!func.array;
        ulong[] ret;
        foreach (item; vals)
        {
            if(serialize(&item) in hashmap)
                ret~=hashmap[serialize(&item)];
        }
        return ret.sort.uniq.array;
    }
}

unittest{
    import std.stdio;
    InvertedIndex idx = InvertedIndex(TYPES.STRING);
    string[] v = ["hi","I","am","t"];
    idx.hashmap[serialize(&v[0])]=[1,2];
    idx.hashmap[serialize(&v[1])]=[1,3];
    idx.hashmap[serialize(&v[2])]=[4];
    idx.hashmap[serialize(&v[3])]=[5];
    writeln(idx.filter(["hi","I","am"]));

    InvertedIndex idx2 = InvertedIndex(TYPES.FLOAT);
    auto v2 = [0.1,0.4,0.6,0.9];
    idx2.hashmap[serialize(&v2[0])]=[1,2];
    idx2.hashmap[serialize(&v2[1])]=[1,3];
    idx2.hashmap[serialize(&v2[2])]=[4];
    idx2.hashmap[serialize(&v2[3])]=[5];
    writeln(idx2.filterRange([0.1,0.8]));
}