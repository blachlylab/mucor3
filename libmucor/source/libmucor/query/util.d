module libmucor.query.util;

import std.sumtype;
import std.functional : partial;
import std.traits : EnumMembers;
import std.typecons : Tuple, tuple;
import std.string;
import std.algorithm.searching;
import std.conv : ConvException, parse;

// import libmucor.query.primary;
import libmucor.query.value;
import libmucor.query.keyvalue;
import libmucor.query.expr;

auto splitOnClosingParenthesis(string query)
{
    auto opened = 0;
    auto closed = 0;
    auto i = 0;
    foreach (c; query)
    {
        if (c == '(')
            opened++;
        if (c == ')')
            closed++;
        i++;
        if (opened == closed)
            break;
    }
    if (i == query.length)
        return tuple(query[1 .. $ - 1], "");
    else
        return tuple(query[1 .. i - 1], query[i .. $]);
}

unittest
{
    assert("()test".splitOnClosingParenthesis == tuple("", "test"));
    assert("(())test".splitOnClosingParenthesis == tuple("()", "test"));
    assert("((())))test".splitOnClosingParenthesis == tuple("(())", ")test"));
    assert("(a and b) and (b and c)".splitOnClosingParenthesis == tuple("a and b", " and (b and c)"));
    assert("((a and b) and (b and c))".splitOnClosingParenthesis == tuple(
            "(a and b) and (b and c)", ""));
}

T enumFromStr(T)(ref string myString)
{
    string e;
    foreach (v; cast(string[])[EnumMembers!T])
    {
        if (myString.startsWith(v))
        {
            e = v;
            break;
        }
    }
    if (e == "")
    {
        throw new Exception("Can't convert string to enum:" ~ T.stringof);
    }
    myString = myString[e.length .. $];
    return cast(T) e;
}

unittest
{
    auto s = ">= 2";
    assert(enumFromStr!ValueOp(s) == ValueOp.GTE);
    assert(s == " 2");
}

auto splitOnValueOp(string query)
{
    auto rest = query.findAmong("=><~");
    if (rest.empty)
        return tuple(false, query, ValueOp.init, "");
    auto q = query[0 .. $ - rest.length];
    auto op = enumFromStr!ValueOp(rest);
    return tuple(true, q, op, rest);
}

unittest
{
    assert("test >= 5".splitOnValueOp == tuple(true, "test ", ValueOp.GTE, " 5"));
    assert("test ~ 5".splitOnValueOp == tuple(true, "test ", ValueOp.ApproxEqual, " 5"));
    assert("test".splitOnValueOp == tuple(false, "test", ValueOp.init, ""));
}

auto splitOnLogicalOp(string query)
{
    auto rest = query.findAmong("&|");
    if (rest.empty)
        return tuple(false, query, LogicalOp.init, "");
    auto q = query[0 .. $ - rest.length];
    auto op = enumFromStr!LogicalOp(rest);
    return tuple(true, q, op, rest);
}

unittest
{
    assert("test & 5".splitOnLogicalOp == tuple(true, "test ", LogicalOp.And, " 5"));
    assert("test | 5".splitOnLogicalOp == tuple(true, "test ", LogicalOp.Or, " 5"));
    assert("test".splitOnLogicalOp == tuple(false, "test", LogicalOp.init, ""));
}
