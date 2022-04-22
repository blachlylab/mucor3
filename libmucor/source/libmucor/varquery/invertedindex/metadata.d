module libmucor.varquery.invertedindex.metadata;
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
import libmucor.varquery.invertedindex.jsonvalue;
import libmucor.varquery.invertedindex.binaryindex;
import std.sumtype;
import libmucor.hts_endian;
import std.typecons: Tuple, tuple;
import std.exception: enforce;

/** 
 *  key_type: 1, 15 padd
 *  key_offset: 8,
 *  key_length: 8,
 *
 * Total size: 48 bytes
 */
struct KeyMetaData {
    align:
    uint128 keyHash;
    ulong keyOffset;
    ulong keyLength;

    this(uint128 keyHash, ulong keyOffset, ulong keyLength) {
        this.keyHash = keyHash;
        this.keyOffset = keyOffset;
        this.keyLength = keyLength;
        assert(keyLength > 0);
    }

    this(ubyte[] data) {
        auto p = data.ptr;
        this.keyHash.hi = le_to_u64(p);
        p += 8;
        this.keyHash.lo = le_to_u64(p);
        p += 8;
        this.keyOffset = le_to_u64(p);
        p += 8;
        this.keyLength = le_to_u64(p);
    }

    ubyte[32] serialize() {
        ubyte[32] ret;
        u64_to_le(this.keyHash.hi, ret.ptr + 0);
        u64_to_le(this.keyHash.lo, ret.ptr + 8);
        u64_to_le(this.keyOffset, ret.ptr + 16);
        u64_to_le(this.keyLength, ret.ptr + 24);
        return ret;
    }
}

/** 
 *  key_type: 1, 15 padd
 *  key_offset: 8,
 *  key_length: 8,
 *
 * Total size: 48 bytes
 */
struct JsonKeyMetaData {
    align:
    uint128 keyHash;
    ulong type;
    ulong padding;
    ulong keyOffset;
    ulong keyLength;

    this(uint128 keyHash, ulong type, ulong padding, ulong keyOffset, ulong keyLength){
        this.keyHash = keyHash;
        this.type = type;
        this.padding = padding;
        this.keyOffset = keyOffset;
        this.keyLength = keyLength;
        assert(this.keyLength > 0);
    }

    this(ubyte[] data) {
        auto p = data.ptr;
        this.keyHash.hi = le_to_u64(p);
        p += 8;
        this.keyHash.lo = le_to_u64(p);
        p += 8;
        this.type = le_to_u64(p);
        p += 8;
        this.padding = le_to_u64(p);
        p += 8;
        this.keyOffset = le_to_u64(p);
        p += 8;
        this.keyLength = le_to_u64(p);
    }

    ubyte[48] serialize() {
        ubyte[48] ret;
        u64_to_le(this.keyHash.hi, ret.ptr + 0);
        u64_to_le(this.keyHash.lo, ret.ptr + 8);
        u64_to_le(this.type, ret.ptr + 16);
        u64_to_le(this.padding, ret.ptr + 24);
        u64_to_le(this.keyOffset, ret.ptr + 32);
        u64_to_le(this.keyLength, ret.ptr + 40);
        return ret;
    }
}

unittest{
    auto field = JsonKeyMetaData(uint128(2), 0, 2, 1, 3);
    auto data = field.serialize;
    assert(cast(ulong[])data == [0, 2, 0, 2, 1, 3]);
    assert(JsonKeyMetaData(data) == field);
}

unittest{
    auto field = KeyMetaData(uint128(2), 1, 3);
    auto data = field.serialize;
    assert(cast(ulong[])data == [0, 2, 1, 3]);
    assert(KeyMetaData(data) == field);
}