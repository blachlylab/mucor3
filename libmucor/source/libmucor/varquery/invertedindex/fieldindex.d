module libmucor.varquery.invertedindex.fieldindex;

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
* Single inverted index for one field and one type.
* Holds a single field's values as keys and record 
* number as value.
*/
struct FieldIndex
{
    // ulong[][JSONValue] hashmap;
    khashl!(JSONValue, ulong[]) hashmap;

    ulong[] filter(T)(T[] items) const
    {
        return items.map!(x => JSONValue(x))
                    .std_filter!(x => x in hashmap)
                    .map!(x => hashmap[x].dup)
                    .joiner.array; 
    }

    ulong[] filterRange(T)(T[] range) const
    {
        assert(range.length==2);
        assert(range[0]<=range[1]);
        JSONValue[2] r = [JSONValue(range[0]), JSONValue(range[1])];
        return hashmap.byKey
                    .std_filter!(x => x.isSameType!T)
                    .std_filter!(x=>x >= r[0])
                    .std_filter!(x=>x < r[1])
                    .map!(x => hashmap[x].dup).joiner.array;
    }

    ulong[] filterOp(string op, T)(T val) const
    {
        mixin("auto func = (JSONValue x) => x " ~ op ~" JSONValue(val);");
        return hashmap.byKey
                        .std_filter!(x => x.isSameType!T)
                        .std_filter!func
                        .map!(x => hashmap[x].dup).joiner.array
                        .sort.uniq.array;
    }

    auto opBinaryRight(string op)(FieldIndex lhs)
    {
        static if(op == "+") {
            FieldIndex ret = FieldIndex(this.hashmap.dup);
            foreach(kv; lhs.byKeyValue) {
                auto v = kv.key in ret.hashmap;
                if(v) {
                    *v = *v ~ kv.value;
                } else {
                    ret.fields[kv.key] = kv.value;
                }
            }
            return ret;
        } else
            static assert(false, "Op not implemented");
    }
}

