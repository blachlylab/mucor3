module libmucor.query.value;

import mir.algebraic;
import std.functional : partial;
import std.traits : EnumMembers;
import std.typecons : Tuple;
import std.string;
import std.algorithm.searching;
import std.conv : ConvException, to, conv_parse = parse;
import std.math.traits : isNaN, isInfinity;
import htslib.hts_log;

/// Represents Integer (half-open) ranges 1:3 (1,2)
alias LongRange = Tuple!(long, "start", long, "end");
/// Represents Floating ranges (half-open) ranges 1.5:3.5
alias DoubleRange = Tuple!(double, "start", double, "end");

/// Values can be any of string, double, or long
alias ValueExpr = Variant!(bool, double, long, string, DoubleRange, LongRange);

struct Value
{
    ValueExpr* expr;

    this(ValueExpr ex)
    {
        this.expr = new ValueExpr;
        *this.expr = ex;
    }

    this(string query)
    {
        auto q = query;
        if (q.startsWith('"') && q.endsWith('"'))
        {
            this.expr = new ValueExpr(q[1 .. $ - 1]);
            return;
        }
        if (auto split = q.findSplit(".."))
        {
            if (split[0].isNumericStringAnInteger && split[2].isNumericStringAnInteger)
            {
                this.expr = new ValueExpr(LongRange(split[0].to!long, split[2].to!long));
                return;
            }
            else
            {
                try
                {
                    this.expr = new ValueExpr(DoubleRange(conv_parse!double(split[0]),
                            conv_parse!double(split[2])));
                }
                catch (ConvException e)
                {
                    this.expr = new ValueExpr(query);
                }
            }
        }
        else
        {
            if (q.isNumericStringAnInteger)
            {
                this.expr = new ValueExpr(conv_parse!long(q));
                return;
            }
            try
            {
                this.expr = new ValueExpr(conv_parse!double(q));
            }
            catch (ConvException e)
            {
                if (q == "true" || q == "True" || q == "TRUE")
                {
                    this.expr = new ValueExpr(true);
                }
                else if (q == "false" || q == "False" || q == "FALSE")
                {
                    this.expr = new ValueExpr(false);
                }
                else
                {
                    this.expr = new ValueExpr(q);
                }

            }
        }
    }
    /// if value is floating: isNan
    /// else throw
    auto isNan()
    {
        return (*this.expr).match!((double x) => isNaN(x), (_x) => false);
    }

    /// if value is floating: isInf
    /// else throw
    auto isInf()
    {
        return (*this.expr).match!((double x) => isInfinity(x), (_x) => false);
    }

    bool opEquals(const Value other) const
    {
        return match!((const bool a, const bool b) => a == b, (const long a,
                const long b) => a == b, (const double a, const double b) => a == b,
                (const string a, const string b) => a == b, (const DoubleRange a,
                    const DoubleRange b) => a == b, (const LongRange a,
                    const LongRange b) => a == b, (_a, _b) => false,)(*this.expr, *other.expr);
    }

    /// Get inner type for testing
    T getInner(T)()
    {
        import std.traits;

        static if (isSomeString!T)
        {
            return (*this.expr).tryMatch!((string x) { return cast(T) x; });
        }
        else static if (isIntegral!T)
        {
            return (*this.expr).tryMatch!((long x) { return cast(T) x; });
        }
        else static if (isFloatingPoint!T)
        {
            return (*this.expr).tryMatch!((double x) { return cast(T) x; });
        }
        else static if (isBoolean!T)
        {
            return (*this.expr).tryMatch!((bool x) { return x; });
        }
        else static if (isArray!T && isFloatingPoint!(ElementType!T))
        {
            return (*this.expr).tryMatch!((DoubleRange x) {
                return cast(T)[x.start, x.end];
            });
        }
        else static if (isArray!T && isIntegral!(ElementType!T))
        {
            return (*this.expr).tryMatch!((LongRange x) {
                return cast(T)[x.start, x.end];
            });
        }
        else
        {
            static assert(0);
        }

    }

    string toString()
    {
        return (*this.expr).match!((string x) => x,
                (DoubleRange x) => format("%f..%f", x[0], x[1]),
                (LongRange x) => format("%d..%d", x[0], x[1]), (x) => x.to!string,);
    }
}

pragma(inline, true) bool isNumericStringAnInteger(const(char)[] val)
{
    if (val[0] == '-')
        val = val[1 .. $];
    foreach (c; val)
    {
        if (c < '0' || c > '9')
            return false;
    }
    return true;
}

unittest
{
    import std;

    /// Test ints
    assert(Value("1").getInner!long == 1);
    assert(Value("1000").getInner!long == 1000);
    assert(Value("-1000").getInner!long == -1000);

    /// Test floats
    assert(Value("1.0").getInner!double == 1.0);
    assert(Value("0x1p-52").getInner!double == double.epsilon);
    assert(Value("0x1.FFFFFFFFFFFFFp1023").getInner!double == double.max);
    assert(Value("1.175494351e-38F").getInner!double == 1.175494351e-38F);
    assert(Value("-1.175494351e-38F").getInner!double == -1.175494351e-38F);
    assert(Value("nan").isNan);
    assert(Value("inf").isInf);

    /// Test strings
    assert(Value("hello").getInner!string == "hello");
    assert(Value("\"hello\"").getInner!string == "hello");

    /// Test bools
    assert(Value("true").getInner!bool == true);
    assert(Value("True").getInner!bool == true);
    assert(Value("TRUE").getInner!bool == true);
    assert(Value("false").getInner!bool == false);
    assert(Value("False").getInner!bool == false);
    assert(Value("FALSE").getInner!bool == false);
}
