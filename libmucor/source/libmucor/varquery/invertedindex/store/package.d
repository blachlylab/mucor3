module libmucor.varquery.invertedindex.store;

public import libmucor.varquery.invertedindex.store.binary;
public import libmucor.varquery.invertedindex.store.json;
public import libmucor.varquery.invertedindex.store.filecache;

import libmucor.wideint;
import libmucor.varquery.invertedindex.jsonvalue;
import std.digest.md;
import std.sumtype: match;
import std.format: format;
import libmucor.spookyhash;
import libmucor.hts_endian;
import libmucor.varquery.invertedindex.metadata;
import std.traits;

uint128 getKeyHash(const(char)[] key) {
    uint128 ret;
    ret.hi = SEED2;
    ret.lo = SEED4;
    SpookyHash.Hash128(key.ptr, key.length, &ret.hi,&ret.lo);
    return ret;
}

enum SEED1 = 0x48e9a84eeeb9f629;
enum SEED2 = 0x2e1869d4e0b37fcb;
enum SEED3 = 0xb5b35cb029261cef;
enum SEED4 = 0x34095e180ababeec;
uint128 getValueHash(JSONValue val) {
    SpookyHash h;
    return (val.val).match!(
        (bool x) {
            uint128 ret;
            auto v = cast(ulong)x;
            ret.hi = SEED1;
            ret.lo = SEED2;
            SpookyHash.Hash128(&v, 8, &ret.hi,&ret.lo);
            return ret;
        },
        (long x) {
            uint128 ret;
            ret.hi = SEED2;
            ret.lo = SEED3;
            SpookyHash.Hash128(&x, 8, &ret.hi,&ret.lo);
            return ret;
        },
        (double x) {
            uint128 ret;
            ret.hi = SEED1;
            ret.lo = SEED3;
            SpookyHash.Hash128(&x, 8, &ret.hi,&ret.lo);
            return ret;
        },
        (const(char)[] x) {
            uint128 ret;
            ret.hi = SEED1;
            ret.lo = SEED4;
            SpookyHash.Hash128(x.ptr, x.length, &ret.hi,&ret.lo);
            return ret;
        }
    );
}

uint128 combineHash(uint128 a, uint128 b) {
    uint128 ret;
    uint256 v;
    v.hi = a;
    v.lo = b;
    ret.hi = SEED2;
    ret.lo = SEED4;
    SpookyHash.Hash128(&v, 32, &ret.hi, &ret.lo);
    return ret;
}

string getShortHash(uint128 v) {
    return format("%x", v)[0..8];
}


pragma(inline, true)
auto sizeSerialized(T)(T item) 
if(isIntegral!T || isFloatingPoint!T || is(T == uint128) || is(T == JsonKeyMetaData) || is(T == KeyMetaData))
{
    return T.sizeof;
}

pragma(inline, true)
auto sizeSerialized(T)(T item) 
if(isSomeString!T)
{
    return (cast(ubyte[])item).length + 8;
}

pragma(inline, true)
auto sizeSerialized(T)(T item) 
if(is(T == SmallsIds))
{
    return 16 + 8 + (item.ids.length * 8);
}

pragma(inline, true)
void serialize(T)(T item, ref ubyte * p) 
if(isIntegral!T || isFloatingPoint!T || is(T == uint128))
{
    static if(is(T == ulong)){
        u64_to_le(item, p);
    }else static if(is(T == long)){
        i64_to_le(item, p);
    }else static if(is(T == double)){
        double_to_le(item, p);
    } else static if(is(T == uint128)){
        u64_to_le(item.hi, p);
        u64_to_le(item.lo, p+8);
    } else {
        static assert(0, "Not a valid store type!");
    }
    p += item.sizeSerialized;
}

pragma(inline, true)
void serialize(T: JsonKeyMetaData)(T item, ref ubyte * p)
{
    item.keyHash.serialize(p);
    item.type.serialize(p);
    item.padding.serialize(p);
    item.keyOffset.serialize(p);
    item.keyLength.serialize(p);
}

pragma(inline, true)
void serialize(T: KeyMetaData)(T item, ref ubyte * p)
{
    item.keyHash.serialize(p);
    item.keyOffset.serialize(p);
    item.keyLength.serialize(p);
}

pragma(inline, true)
void serialize(T)(T item, ref ubyte * p)
if(isSomeString!T)
{
    item.length.serialize(p);
    auto buf = cast(ubyte[])item;
    p[0..buf.length] = buf[];
    p += buf.length;
}

pragma(inline, true)
void serialize(T: SmallsIds)(T item, ref ubyte * p)
{
    item.key.serialize(p);
    item.ids.length.serialize(p);
    foreach(id; item.ids){
        u64_to_le(id, p);
        p += 8;
    }
}

pragma(inline, true)
T deserialize(T)(ref ubyte * p)
if(isIntegral!T || isFloatingPoint!T || is(T == uint128))
{
    T ret;
    static if(is(T == ulong)){
        ret = le_to_u64(p);
    }else static if(is(T == long)){
        ret = le_to_i64(p);
    }else static if(is(T == double)){
        ret =  le_to_double(p);
    } else static if(is(T == uint128)){
        ret.hi = le_to_u64(p);
        ret.lo = le_to_u64(p + 8);
    }
    p += T.sizeof;
    return ret;
}

pragma(inline, true)
T deserialize(T)(ref ubyte * p)
if(is(T == JsonKeyMetaData))
{
    T ret;
    ret.keyHash = deserialize!uint128(p);
    ret.type = deserialize!ulong(p);
    ret.padding = deserialize!ulong(p);
    ret.keyOffset = deserialize!ulong(p);
    ret.keyLength = deserialize!ulong(p);
    return ret;
}

pragma(inline, true)
T deserialize(T)(ref ubyte * p)
if(is(T == KeyMetaData))
{
    T ret;
    ret.keyHash = deserialize!uint128(p);
    ret.keyOffset = deserialize!ulong(p);
    ret.keyLength = deserialize!ulong(p);
    return ret;
}

pragma(inline, true)
auto sizeDeserialized(T)(ubyte * p) 
if(isIntegral!T || isFloatingPoint!T || is(T == uint128) || is(T == JsonKeyMetaData) || is(T == KeyMetaData))
{
    return T.sizeof;
}

pragma(inline, true)
auto sizeDeserialized(T)(ubyte * p) 
if(isSomeString!T)
{
    return le_to_u64(p);
}

pragma(inline, true)
auto sizeDeserialized(T)(ubyte * p) 
if(is(T == SmallsIds))
{
    auto len = le_to_u64(p + 16);
    return 16 + 8 + (len * 8);
}