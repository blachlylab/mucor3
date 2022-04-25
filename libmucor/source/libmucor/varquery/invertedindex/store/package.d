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
    ret.hi = SEED1;
    ret.lo = SEED2;
    SpookyHash.Hash128(&v, 32, &ret.hi, &ret.lo);
    return ret;
}

string getShortHash(uint128 v) {
    return format("%x", v)[0..8];
}

