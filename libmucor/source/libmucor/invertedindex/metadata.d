module libmucor.invertedindex.metadata;
import std.algorithm.setops;
import std.algorithm : sort, uniq, map, std_filter = filter, canFind, joiner;
import std.math : isClose;
import std.array : array;
import std.conv : to, ConvException;
import std.traits;
import std.meta;

import libmucor.wideint : uint128;
import libmucor.khashl;
import libmucor.jsonlops.jsonvalue;
import libmucor.invertedindex.binaryindex;
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
    @nogc:

    this(uint128 keyHash, ulong keyOffset, ulong keyLength) {
        this.keyHash = keyHash;
        this.keyOffset = keyOffset;
        this.keyLength = keyLength;
        assert(keyLength > 0);
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

    @nogc:

    this(uint128 keyHash, ulong type, ulong padding, ulong keyOffset, ulong keyLength){
        this.keyHash = keyHash;
        this.type = type;
        this.padding = padding;
        this.keyOffset = keyOffset;
        this.keyLength = keyLength;
        assert(this.keyLength > 0);
    }
}

unittest{
    import libmucor.invertedindex.store: serialize, deserialize;
    
    auto field = JsonKeyMetaData(uint128(2), 0, 2, 1, 3);
    ubyte[48] data;
    auto p = data.ptr;
    field.serialize(p);
    assert(cast(ulong[])data == [0, 2, 0, 2, 1, 3]);
    JsonKeyMetaData c;
    p = data.ptr;
    assert(deserialize!JsonKeyMetaData(p) == field);
}

// unittest{
//     auto field = KeyMetaData(uint128(2), 1, 3);
//     auto data = field.serialize;
//     assert(cast(ulong[])data == [0, 2, 1, 3]);
//     assert(KeyMetaData(data) == field);
// }

unittest{
    import libmucor.invertedindex.store: serialize, deserialize;
    
    auto field = KeyMetaData(uint128(2), 1, 3);
    ubyte[32] data;
    auto p = data.ptr;
    field.serialize(p);
    assert(cast(ulong[])data == [0, 2, 1, 3]);
    KeyMetaData c;
    p = data.ptr;
    assert(deserialize!KeyMetaData(p) == field);
}