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
import std.sumtype;
import htslib.hts_endian;
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

    auto deserialize_to_tuple(JsonKeyMetaData[] data, ubyte[] keyData) {
        auto kData = keyData[keyOffset..keyOffset+keyLength];
        auto dataForKey = data[fieldOffset..fieldOffset+fieldLength];
        alias RT = Tuple!(string, "key", JsonKeyMetaData[], "value");
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
struct JsonKeyMetaData {
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
    auto field = JsonKeyMetaData(2, 0, 2, 1, 3, 5);
    auto data = field.serialize;
    assert(cast(ulong[])data == [2, 0, 2, 1, 3, 5]);
    assert(JsonKeyMetaData(data) == field);
}

unittest{
    auto field = KeyMetaData(2, 1, 3, 5);
    auto data = field.serialize;
    assert(cast(ulong[])data == [2, 1, 3, 5]);
    assert(KeyMetaData(data) == field);
}