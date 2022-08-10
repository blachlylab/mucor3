module libmucor.query.tokens;
import std.sumtype;

import libmucor.query.util;
import std.range;
import std.ascii : isWhite;
import std.meta : staticIndexOf;
import libmucor.query;

/// Operators that can be applied to ValueExprs
enum ValueOp : string
{
    Equal = "=",
    GTE = ">=",
    LTE = "<=",
    GT = ">",
    LT = "<",
    ApproxEqual = "~",
    KeyOperator = ":"
}

/// Operators that can be applied to a key with a list of values 
enum BinaryLogicalOp : string
{
    And = "&",
    Or = "|"
}

/// Operators that can be applied to a key with a list of values 
enum UnaryLogicalOp : string
{
    Not = "!"
}

/// Operators that can be applied to a key 
enum KeyOp : string
{
    Exists = "_exists_"
}

/// Operators that can be applied to a key 
enum Parenthesis : string
{
    Left = "(",
    Right = ")",
}

alias Operators = SumType!(Parenthesis, ValueOp, BinaryLogicalOp, UnaryLogicalOp, KeyOp,);

struct KeyOrValue
{
    string data;
}

alias TokenizedTypes = SumType!(Operators, string);

T getInner(T)(TokenizedTypes v)
{
    import std.traits;

    static if (staticIndexOf!(T, Operators.Types) != -1)
    {
        auto matchOp = (Operators o) => o.tryMatch!((T x) => x);
        return v.tryMatch!((Operators x) => matchOp(x));
    }
    else
    {
        return v.tryMatch!((string x) => x);
    }

}

struct Tokenized
{
    TokenizedTypes[] tokens;
    ulong[] idxs;
    string original;

    this(TokenizedTypes[] tokens, ulong[] idxs, string original)
    {
        this.tokens = tokens;
        this.idxs = idxs;
        this.original = original;
    }

    this(string str)
    {
        ulong idx;
        original = str;
        bool isDelim = false;
        string tok;
        while (!str.empty)
        {
            if (tok != "" && isDelim)
            {
                auto tmp = tokens[$ - 1];
                tokens[$ - 1] = TokenizedTypes(tok);
                tokens ~= tmp;
                tok = "";
            }
            if (str[0] == '"')
            {
                auto i = 1;
                for (; i < str.length; i++)
                {
                    if (str[i] == '"')
                        break;
                }
                i++;
                isDelim = false;
                tokens ~= TokenizedTypes(str[0 .. i]);
                str = str[i .. $];
                continue;
            }
            if (isWhite(str[0]))
            {
                idx++;
                str = str[1 .. $];
                continue;
            }
            try
            {
                Parenthesis p = enumFromStr!(Parenthesis)(str);
                idxs ~= idx;
                idx++;
                tokens ~= TokenizedTypes(Operators(p));
                isDelim = true;
            }
            catch (Exception e)
            {
                try
                {
                    UnaryLogicalOp uop = enumFromStr!(UnaryLogicalOp)(str);
                    idxs ~= idx;
                    idx += (cast(string) uop).length;
                    tokens ~= TokenizedTypes(Operators(uop));
                    isDelim = true;
                }
                catch (Exception e)
                {
                    try
                    {
                        BinaryLogicalOp bop = enumFromStr!(BinaryLogicalOp)(str);
                        idxs ~= idx;
                        idx += (cast(string) bop).length;
                        tokens ~= TokenizedTypes(Operators(bop));
                        isDelim = true;
                    }
                    catch (Exception e)
                    {
                        try
                        {
                            KeyOp kop = enumFromStr!(KeyOp)(str);
                            idxs ~= idx;
                            idx += (cast(string) kop).length;
                            tokens ~= TokenizedTypes(Operators(kop));
                            isDelim = true;
                        }
                        catch (Exception e)
                        {
                            try
                            {
                                ValueOp vop = enumFromStr!(ValueOp)(str);
                                idxs ~= idx;
                                idx += (cast(string) vop).length;
                                tokens ~= TokenizedTypes(Operators(vop));
                                isDelim = true;
                            }
                            catch (Exception e)
                            {
                                if (tok == "")
                                {
                                    idxs ~= idx;
                                }
                                idx++;
                                tok ~= str[0];
                                str = str[1 .. $];
                                isDelim = false;
                            }
                        }

                    }

                }

            }

        }
        if (tok != "" && isDelim)
        {
            auto tmp = tokens[$ - 1];
            tokens[$ - 1] = TokenizedTypes(tok);
            tokens ~= tmp;
            tok = "";
        }
        else if (tok != "")
            tokens ~= TokenizedTypes(tok);
    }

    string toString()
    {
        auto matchOp = (Operators x) => match!((Parenthesis x) => cast(string) x,
                (ValueOp x) => cast(string) x, (BinaryLogicalOp x) => cast(string) x,
                (UnaryLogicalOp x) => cast(string) x, (KeyOp x) => cast(string) x,)(x);
        string ret;
        foreach (item; this.tokens)
        {
            item.match!((string x) => ret ~= x, (Operators x) => ret ~= matchOp(x));
        }
        return ret;
    }

    ref auto opIndex(size_t index)
    {
        return this.tokens[index];
    }

    auto opIndex(ulong[2] slice)
    {
        return Tokenized(this.tokens[slice[0] .. slice[1]],
                this.idxs[slice[0] .. slice[1]], original);
    }

    ulong[2] opSlice(size_t dim)(size_t start, size_t end)
    {
        return [start, end];
    }

    auto length()
    {
        return this.tokens.length;
    }

    Tokenized sliceToNextClosingParenthesis()
    {
        assert(this[0].getInner!Parenthesis == Parenthesis.Left);
        auto opened = 0;
        auto i = 0;
        foreach (tok; tokens)
        {
            auto matchOp = (Operators x) => x.match!((Parenthesis x) => x == Parenthesis.Left
                    ? 1 : -1, (_x) => 0,);
            tok.match!((Operators x) { opened += matchOp(x); }, (_x) {},);
            i++;
            if (opened == 0 && i != 0)
                break;
        }
        return Tokenized(this.tokens[0 .. i], this.idxs[0 .. i], original);
    }

    size_t opDollar()
    {
        return this.tokens.length;
    }

    auto getFrontInner(T)()
    {
        try
        {
            return this[0].getInner!T;
        }
        catch (Exception e)
        {
            import std.format;
            import std.traits;

            static if (is(T == string))
                queryErr(original, this.idxs[0], "Unexpected token, Expected key or value");
            else
                queryErr(original, this.idxs[0], format("Unexpected token, Expected one of \"%s\"",
                        cast(string[])[EnumMembers!T]));
            return T.init;
        }
    }

    void popFront()
    {
        this.tokens = this.tokens[1 .. $];
        this.idxs = this.idxs[1 .. $];
    }
}

unittest
{
    import std.stdio;

    string query = "(key >= val) | (key = val)";
    auto tokens = Tokenized(query);
    assert(tokens.toString == "(key>=val)|(key=val)");
    assert(tokens[0].getInner!Parenthesis == Parenthesis.Left);
    assert(tokens[1].getInner!string == "key");
    assert(tokens[2].getInner!ValueOp == ValueOp.GTE);
    assert(tokens[3].getInner!string == "val");
    assert(tokens[4].getInner!Parenthesis == Parenthesis.Right);
    assert(tokens[5].getInner!BinaryLogicalOp == BinaryLogicalOp.Or);
    assert(tokens[6].getInner!Parenthesis == Parenthesis.Left);
    assert(tokens[7].getInner!string == "key");
    assert(tokens[8].getInner!ValueOp == ValueOp.Equal);
    assert(tokens[9].getInner!string == "val");
    assert(tokens[10].getInner!Parenthesis == Parenthesis.Right);
    assert(tokens.idxs == [0, 1, 5, 8, 11, 13, 15, 16, 20, 22, 25]);
    assert(tokens[10].getInner!Parenthesis == Parenthesis.Right);
    assert(tokens.sliceToNextClosingParenthesis.toString == "(key>=val)");

    query = "(key >= val) | (key = (!val | val2))";
    tokens = Tokenized(query);
    assert(tokens.toString == "(key>=val)|(key=(!val|val2))");

    query = "! val";
    tokens = Tokenized(query);
    assert(tokens.toString == "!val");

    query = "\"key 2\" = val";
    tokens = Tokenized(query);
    assert(tokens.toString == "\"key 2\"=val");

    query = "key = \"val 2\"";
    tokens = Tokenized(query);
    assert(tokens.toString == "key=\"val 2\"");
}
