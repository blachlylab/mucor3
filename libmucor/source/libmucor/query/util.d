module libmucor.query.util;

import std.sumtype;
import std.functional : partial;
import std.traits : EnumMembers;
import std.typecons : Tuple, tuple;
import std.string;
import std.algorithm.searching;
import std.conv : ConvException, parse;
import std.range : repeat, take;
import std.algorithm : reverse;
import std.array : array;

// import libmucor.query.primary;
// import libmucor.query.value;
// import libmucor.query.keyvalue;
// import libmucor.query.expr;
import libmucor.query.tokens;
import libmucor.error;

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
        return tuple(query[0 .. $], "");
    else
        return tuple(query[0 .. i], query[i .. $]);
}

unittest
{
    assert("()".splitOnClosingParenthesis == tuple("()", ""));
    assert("()test".splitOnClosingParenthesis == tuple("()", "test"));
    assert("(())test".splitOnClosingParenthesis == tuple("(())", "test"));
    assert("((()))test".splitOnClosingParenthesis == tuple("((()))", "test"));
    assert("(a and b) and (b and c)".splitOnClosingParenthesis == tuple("(a and b)", " and (b and c)"));
    assert("((a and b) and (b and c))".splitOnClosingParenthesis == tuple(
            "((a and b) and (b and c))", ""));
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
        return tuple(false, query, BinaryLogicalOp.init, "");
    auto q = query[0 .. $ - rest.length];
    auto op = enumFromStr!BinaryLogicalOp(rest);
    return tuple(true, q, op, rest);
}

unittest
{
    assert("test & 5".splitOnLogicalOp == tuple(true, "test ", BinaryLogicalOp.And, " 5"));
    assert("test | 5".splitOnLogicalOp == tuple(true, "test ", BinaryLogicalOp.Or, " 5"));
    assert("test".splitOnLogicalOp == tuple(false, "test", BinaryLogicalOp.init, ""));
}


void validateParenthesis(string query) {
    if(!(balancedParens(query, '(', ')'))){
        
        long openIdx = query.countUntil("(");
        assert((openIdx == -1) || (query[openIdx] == '('));    

        long nextOpenIdx = openIdx == -1 ? -1 : query[openIdx+1..$].countUntil("(");
        long nextCloseIdx = query.countUntil(")");
        long lastCloseIdx = query.dup.reverse.countUntil(")");
        lastCloseIdx = lastCloseIdx == -1 ? -1 : (query.length - lastCloseIdx) - 1;
        long closeIdx;
        if(nextCloseIdx == -1)
            closeIdx = -1;
        else if(nextOpenIdx == -1)
            closeIdx = nextCloseIdx;
        else if(nextCloseIdx < nextOpenIdx)
            closeIdx = nextCloseIdx;
        else
            closeIdx = lastCloseIdx;
        
        assert((closeIdx == -1) || (query[closeIdx] == ')'));

        if(openIdx == -1 && closeIdx != -1){
            log_err_no_exit("parseQuery", "Query sytax error!");
            log_err_no_exit("parseQuery", "Missing opening parenthesis");
            log_err_no_exit("parseQuery", "Unmatched ')' here: "~(' '.repeat.take(closeIdx).array.idup)~"v");
            log_err("parseQuery", "QueryFragment:      %s", query);
        }
        if(openIdx != -1 && closeIdx == -1){
            log_err_no_exit("parseQuery", "Query sytax error!");
            log_err_no_exit("parseQuery", "Missing closing parenthesis");
            log_err_no_exit("parseQuery", "Unmatched '(' here: "~(' '.repeat.take(openIdx).array.idup)~"v");
            log_err("parseQuery", "QueryFragment:      %s", query);
        }
        if(closeIdx < openIdx){
            log_err_no_exit("parseQuery", "Query sytax error!");
            log_err_no_exit("parseQuery", "Missing opening parenthesis");
            log_err_no_exit("parseQuery", "Unmatched ')' here: "~(' '.repeat.take(closeIdx).array.idup)~"v");
            log_err("parseQuery", "QueryFragment:      %s", query);
        }
        validateParenthesis(query[openIdx+1 .. closeIdx]);
        if(closeIdx < query.length)
            validateParenthesis(query[closeIdx + 1..$]);
    }
}

unittest
{
    import std.exception: assertThrown;
    assertThrown(validateParenthesis(" ()())"));
    assertThrown(validateParenthesis("(()() "));
    assertThrown(validateParenthesis("(())("));
    assertThrown(validateParenthesis(")())("));
}

string[] splitIntoGroupsByTokens(string query) {
    int opened = 0;
    int negIdx = -1;
    int trailIdx = -1;
    int lastOp = -1;
    string[] arr;
    string curStatement = query.strip;
    auto i = 0;
    while(true){
        if(curStatement.length == 0) break;
        if(curStatement[i] == '(') {
            opened++;
            auto s = splitOnClosingParenthesis(curStatement);
            if(negIdx != -1 && negIdx == i-1)
                arr ~= "!" ~ s[0];
            else
                arr ~= s[0];
            i += s[0].length;
            continue;
        }

        if(query[i] == '!' && negIdx == -1){
            negIdx = cast(int)i;
            continue;
        }

        if(query[i] == '|' || query[i] == '&') {
            if(opened == 0 && i != 0 && negIdx != i-1){
                if(lastOp != -1)
                    arr ~= query[lastOp+1 .. i];
                else
                    arr ~= query[0 .. i];
                trailIdx = -1;
                
            }
            arr ~= query[i .. i+1];
            lastOp = i;
            continue;
        }
        if(query[i] == ' ') continue;
        if(trailIdx == -1)
            trailIdx = cast(int)i;
    }

    if(trailIdx != -1 && opened != 0) {
        arr ~= query[trailIdx .. $];
    }
    if(arr.length == 1) {
        if(arr[0][0] != '!')
            arr[0] = arr[0][1..$-1];

    }
    return arr;
}
