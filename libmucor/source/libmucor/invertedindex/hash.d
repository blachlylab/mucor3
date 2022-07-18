module libmucor.invertedindex.hash;

import libmucor.invertedindex.record;
import libmucor.spookyhash;
import std.traits;
uint128 getKeyHash(const(char)[] key)
{
    uint128 ret = uint128([SEED2, SEED4]);
    SpookyHash.Hash128(key.ptr, key.length, &ret.data[0], &ret.data[1]);
    return ret;
}

unittest {
    import std.stdio;
    writeln("Testing hashes");

    writeln(getKeyHash("test"));
    writeln(getValueHash("test"));
    writeln(getValueHash(1L));
    writeln(getValueHash(2L));
}

enum SEED1 = 0x48e9a84eeeb9f629;
enum SEED2 = 0x2e1869d4e0b37fcb;
enum SEED3 = 0xb5b35cb029261cef;
enum SEED4 = 0x34095e180ababeec;
uint128 getValueHash(T)(T val)
{
    static if(isBoolean!T) {
        uint128 ret = uint128([SEED1, SEED2]);
        auto v = cast(ulong) val;
        SpookyHash.Hash128(&v, 8, &ret.data[0], &ret.data[1]);
        return ret;
    } else static if(isIntegral!T) {
        uint128 ret = uint128([SEED2, SEED3]);
        auto v = cast(ulong) val;
        SpookyHash.Hash128(&v, 8, &ret.data[0], &ret.data[1]);
        return ret;
    } else static if(isFloatingPoint!T) {
        uint128 ret = uint128([SEED1, SEED3]);
        SpookyHash.Hash128(&val, T.sizeof, &ret.data[0], &ret.data[1]);
        return ret;
    } else static if(isSomeString!T) {
        uint128 ret = uint128([SEED1, SEED4]);
        SpookyHash.Hash128(val.ptr, val.length, &ret.data[0], &ret.data[1]);
        return ret;
    } else static assert(0);
}

uint256 combineHash(uint128 a, uint128 b)
{
    return uint256([a.data[0], a.data[1], b.data[0], b.data[1]]);
}