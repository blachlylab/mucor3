module libmucor.query.expr.keyvalue;

import std.stdio;
import std.algorithm.setops;
import std.regex;
import std.algorithm : map, fold;
import std.array : array;
import std.string : replace;
import std.format : format;
import std.typecons : Tuple;

import std.string : strip;
import std.algorithm.searching;
import htslib.hts_log;

import libmucor.query;
import libmucor.query.key;
import libmucor.query.value;
import libmucor.query.tokens;


/// key op value
struct KeyValue {
    ValueOp op;
    Key lhs;
    Value rhs;

    this(Tokenized tokens)
    {
        assert(tokens.length == 3);
        this.lhs = Key(tokens.getFrontInner!string);
        tokens.popFront;
        this.op = tokens.getFrontInner!ValueOp;
        tokens.popFront;
        this.rhs = Value(tokens.getFrontInner!string);
    }

    string toString()
    {
        return this.lhs.toString ~ cast(string) this.op ~ this.rhs.toString;
    }

    bool opEquals(const KeyValue other) const
    {
        return this.op == other.op && this.lhs == other.lhs && this.rhs == other.rhs;
    }
}

/// key op
struct UnaryKeyOp {
    KeyOp op;
    Key lhs;

    this(Key lhs, KeyOp op)
    {
        this.lhs = lhs;
        this.op = op;
    }

    this(Tokenized tokens){
        this.lhs = Key(tokens.getFrontInner!string);
        tokens.popFront;
        auto op = tokens.getFrontInner!ValueOp;
        if(op != ValueOp.KeyOperator)
            queryErr(tokens.original, tokens.idxs[0], "Expected \":\"");
        tokens.popFront;
        this.op = tokens.getFrontInner!KeyOp;
    }

    string toString()
    {
        return this.lhs.key ~ ":" ~ cast(string) this.op;
    }

    bool opEquals(const UnaryKeyOp other) const
    {
        return this.op == other.op && this.lhs == other.lhs;
    }

}

string toString(T: UnaryKeyOp)(T kv)
{
    return kv.lhs ~ ":" ~ cast(string) kv.op;
}


// unittest
// {
//     assert(parse!UnaryKeyOp("\"key\": _exists_") == UnaryKeyOp(KeyOp.Exists, "key"));
// }
