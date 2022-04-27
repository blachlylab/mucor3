module libmucor.varquery.query.primary;

import std.sumtype;
import std.functional : partial;
import std.traits : EnumMembers;
import std.typecons : Tuple;
import std.string;
import std.algorithm.searching;
import std.conv;
import htslib.hts_log;
import std.format;

import libmucor.varquery.query.value;

/// Operators that can be applied to ValueExprs
enum ValueOp : string {
    Equal  = "=",
    GT     = ">",
    LT     = "<",
    GTE    = ">=",
    LTE    = "<=",
    Range  = ":",
    ApproxEqual = "~"
}

/// Respresents a basic "key op value" query
/// e.g. foo = bar
alias KeyValueExpr = Tuple!(ValueOp, "op", string, "key", ValueExpr, "value");


auto createKeyValueExpr(string query_str) {
    auto q = query_str;
    string key;
    ValueOp op;
    ValueExpr expr;
    if(q.startWith('"')) {
        if(auto quoteSplit = q[1..$].findSplit("\"")) {
            key = split[0];
            q = split[2];
        } else {
            goto getKey;
        }
    } 
    getKey: {
        if(auto split = q.findSplit(" ")) {
            key = split[0];
            q = split[2].stripLeft;
        } else {
            if(auto rest = q.findAmong("=><:~")) {
                key = q[0 .. $ - rest.length];
                q = rest;
            } else {
                hts_log_error(__FUNCTION__, format("Could not find a valid operator in in query portion: %s", query_str));
            }
        }
    }
    try {
        op = q.parse!ValueOp;
        expr = createValueExpr(q);
    } catch(ConvException e) {
        hts_log_error(__FUNCTION__, format("Could not find a valid operator in in query portion: %s", query_str));
    }
    return KeyValueExpr()
}

/// Operators that can be applied to just keys
enum KeyOp : string {
    Exists = "_exists_"
}

/// Respresents a basic "key op" query
alias KeyOpExpr = Tuple!(KeyOp, "op", string, "key");

/// Operators that can be applied to a key with a list of values 
enum FilterOp: string
{
    And    = "&",
    Or     = "|",
    Not    = "!"
}

/// Represents a basic filtering query
/// e.g. foo = bar | baz
alias KeyFilterExpr = Tuple!(FilterOp, "op", string, "key", ValueExpr[], "values");

alias PrimaryQuery = SumType!(
    KeyValueExpr,
    KeyOpExpr,
    KeyFilterExpr
);

