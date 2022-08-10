module libmucor.query.key;

import std.typecons : Tuple;
import std.conv : parse;
import std.meta : AliasSeq;
import std.traits : EnumMembers;
import std.algorithm : findAmong, findSplit;
import std.string : startsWith;

import libmucor.query;
import libmucor.query.tokens;
import std.array : join;

/// key op value
struct Key
{

    string key;

    this(string k)
    {
        this.parse(k);
    }

    const BannedCharacters = ['(', ')'] ~ (cast(string[])[EnumMembers!ValueOp])
        .join ~ (cast(string[])[EnumMembers!BinaryLogicalOp]).join;
    string toString()
    {
        return key;
    }

    /// Parse Key from string like "foo = bar" to return "foo"
    /// input string is expected to have whitespace trimmed from beginning
    auto parse(string query_str)
    {
        auto q = query_str;
        if (q.startsWith('"'))
        {
            if (auto quoteSplit = q[1 .. $].findSplit("\""))
            {
                this.key = quoteSplit[0];
                return;
            }
            else
                queryErr(query_str, 0, "Query key missing closing quotes!");
        }
        else
        {
            auto bad = query_str.idup.findAmong(cast(string) BannedCharacters);
            if (bad != "")
                queryErr(query_str, query_str.length - bad.length,
                        "Token found in key sequence! Please surround with quotes.");

        }
        this.key = query_str;
    }
}

unittest
{
    import std.exception : assertThrown;

    Key k = Key("test");
    assert(k.key == "test");
}
