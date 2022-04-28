module libmucor.varquery.query.keyvalue;

import std.stdio;
import std.algorithm.setops;
import std.regex;
import std.algorithm : map, fold;
import std.array : array;
import std.string : replace;
import std.conv : to, parse;
import std.format : format;
import std.typecons : Tuple;

import std.string;
import std.algorithm.searching;
import htslib.hts_log;

import libmucor.varquery.query.value;
import libmucor.varquery.query.expr;
import libmucor.varquery.query.util;

/// key op value
alias KeyValue = Tuple!(ValueOp, "op", string, "lhs", Value, "rhs");

string keyValueToString(KeyValue kv) {
    return kv.lhs ~ cast(string) kv.op ~ valueToString(kv.rhs);
}

/// Parse Key from string like "foo = bar" to return "foo"
/// input string is expected to have whitespace trimmed from beginning
auto parseKey(string query_str) {
    auto q = query_str;
    if(q.startsWith('"')) {
        if(auto quoteSplit = q[1..$].findSplit("\"")) {
            return quoteSplit[0];
        } else {
            return query_str;
        }
    }
    return query_str;
}

/// key op
alias UnaryKeyOp = Tuple!(KeyOp, "op", string, "lhs");

string unaryKeyOpToString(UnaryKeyOp kv) {
    return kv.lhs ~ ":" ~ cast(string) kv.op;
}

auto parseUnaryKeyOp(string v) {
    auto rest = v.findAmong(":");
    auto key = parseKey(v[0 .. $ - rest.length]);
    rest = rest[1..$].strip;
    auto op = enumFromStr!KeyOp(rest);
    return UnaryKeyOp(op, key);
}

unittest
{
    assert(parseUnaryKeyOp("\"key\": _exists_") == UnaryKeyOp(KeyOp.Exists, "key"));
}