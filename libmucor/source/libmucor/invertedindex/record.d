module libmucor.invertedindex.record;

import std.traits;
import std.typecons : tuple;
import std.algorithm : map;

import libmucor.hts_endian;

import drocks.database;
import drocks.columnfamily;

import mir.bignum.integer;
import core.stdc.stdlib : free;

alias uint128 = BigInt!2;
alias uint256 = BigInt!4;

struct RecordStore(K, V) {
    ColumnFamily * family;

    this(ColumnFamily * family) {
        this.family = family;
    }

    auto opIndex(K key)
    {
        return deserialize!V((*this.family)[serialize(key)]);
    }

    auto opIndexAssign(V value, K key)
    {
        return (*this.family)[serialize(key)] = serialize(value);
    }

    auto byKeyValue() {
        auto r = this.family.iter;
        return r.map!(x => tuple(deserialize!K(x[0]), deserialize!V(x[0])));
    }
}

alias Hash2IonStore = RecordStore!(uint128, ubyte[]);
alias String2HashStore = RecordStore!(const(char)[], uint128);
alias Long2HashStore = RecordStore!(long, uint128);
alias Float2HashStore = RecordStore!(float, uint128);

auto serialize(T)(T val)
{
    static if(is(T == ubyte[]))
        return val;
    else static if(isSomeString!T)
        return cast(ubyte[])val;
    else static if(isNumeric!T){
        ubyte[T.sizeof] arr;
        static if(is(T == float)){
            float_to_le(val, arr.ptr);
        }else static if(is(T == double)){
            double_to_le(val, arr.ptr);
        } else static if(is(T == ulong)){
            u64_to_le(val, arr.ptr);
        } else static if(is(T == long)){
            i64_to_le(val, arr.ptr);
        } else static assert(0);
        return arr;
    } else static if(is(T == uint128)) {
        ubyte[16] arr;
        u64_to_le(val.data[0], arr.ptr);
        u64_to_le(val.data[1], arr.ptr + 8);
        return arr;
    } else static if(is(T == uint256)) {
        ubyte[32] arr;
        u64_to_le(val.data[0], arr.ptr);
        u64_to_le(val.data[1], arr.ptr + 8);
        u64_to_le(val.data[2], arr.ptr + 16);
        u64_to_le(val.data[3], arr.ptr + 24);
        return arr;
    }
}

T deserialize(T)(ubyte[] val)
{
    static if(is(T == ubyte[]))
        return val;
    else static if(isSomeString!T)
        return cast(T)val;
    else static if(isNumeric!T){
        T ret;
        static if(is(T == float)){
            ret = le_to_float(val.ptr);
        }else static if(is(T == double)){
            ret = le_to_double(val.ptr);
        } else static if(is(T == ulong)){
            ret = le_to_u64(val.ptr);
        } else static if(is(T == long)){
            ret = le_to_i64(val.ptr);
        } else static assert(0);
        free(val.ptr);
        val = [];
        return ret;
    } else static if(is(T == uint128)) {
        auto ret = BigInt!2([le_to_u64(val.ptr), le_to_u64(val.ptr + 8)]);
        free(val.ptr);
        val = [];
        return ret;
    } else static if(is(T == uint256)) {
        auto ret = BigInt!2([
            le_to_u64(val.ptr), 
            le_to_u64(val.ptr + 8),
            le_to_u64(val.ptr + 16),
            le_to_u64(val.ptr + 24)]);
        free(val.ptr);
        val = [];
        return ret;
    }
}

