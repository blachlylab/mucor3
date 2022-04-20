module libmucor.varquery.invertedindex.store;

public import libmucor.varquery.invertedindex.store.binary;
public import libmucor.varquery.invertedindex.store.json;

import libmucor.wideint;
import libmucor.varquery.invertedindex.jsonvalue;
import std.digest.md;
import std.sumtype: match;

uint128 getKeyHash(const(char)[] key) {
    return cast(uint128) md5Of(key);
}

uint128 getValueHash(JSONValue val) {
    return (val.val).match!(
        (bool x) {
            auto v = cast(ulong)x;
            return cast(uint128) md5Of((cast(ubyte*)&v)[0..8]);
        },
        (long x) => cast(uint128) md5Of((cast(ubyte*)&x)[0..8]),
        (double x) => cast(uint128) md5Of((cast(ubyte*)&x)[0..8]),
        (const(char)[] x) => cast(uint128) md5Of(x),
    );
}