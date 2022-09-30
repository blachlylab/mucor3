module libmucor.query.expr;

import std.sumtype;
import std.functional : partial;
import std.traits : EnumMembers;
import std.typecons : Tuple, tuple;
import std.string : strip;
import std.algorithm.searching;
import std.algorithm : reverse;
import std.conv : ConvException, conv_parse = parse;
import std.range : repeat, take, empty;
import std.array : array;

public import libmucor.query.expr.keyvalue;
public import libmucor.query.expr.notvalue;
public import libmucor.query.expr.query;

/**
* Logic for parsing string query and filter results using inverted index.
* Uses !, &, and | operators with key and values represent as key:value
* e.g.: key1=val1 -> get all records where key1=val1
* e.g.: key1=val1 & key2=val2 -> get all records where key1=val1 and key2=val2
* e.g.: key1=val1 | key2=val2 -> get all records where key1=val1 or key2=val2
* e.g.: key1=(val1 | key2) -> get all records where key1=val1 or key1=val2
* can get complicated
* e.g.: !(key1:val1 & key2:(val2 | val3) & key3:1-2) | key4:val4 | key5:(val5 & val6)
*
* Operators: =, :, >, >=, <, <=, &, !, |
* Other specials: (, )
**/

unittest
{
    import std.stdio;
    import libmucor.query.key;
    import libmucor.query.value;
    import libmucor.query.tokens;

    auto q = Query("1.0");
    assert(q == Query(Value("1.0")));

    q = Query("\"key\": exists");
    assert(q == Query(UnaryKeyOp(Key("key"), KeyOp.Exists)));

    q = Query("val1 | 1 | 2.1");
    assert(q.toString() == "val1|1|2.1");

    q = Query("key = val");
    assert(q.toString() == "key=val");

    q = Query("(key = val)");
    assert(q.toString() == "(key=val)");

    q = Query("!val");
    assert(q.toString() == "!val");

    q = Query("!(key = val)");
    assert(q.toString() == "!(key=val)");

    q = Query("!(key = val)");
    assert(q.toString() == "!(key=val)");

    q = Query("!(key = val) & (foo = bar)");
    assert(q.toString() == "!(key=val)&(foo=bar)");

    q = Query("(!(key = 1..2) & (foo = ( bar | 3 | (baz & test & v))))");
    assert(q.toString() == "(!(key=1..2)&(foo=(bar|3|(baz&test&v))))");
}

unittest
{
    import std.stdio;
    import std.exception : assertThrown;

    assertThrown(Query("(! (key = val) & Key = val))"));
    assertThrown(Query("(! ((key = val) & Key = val)"));
}
