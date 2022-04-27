module libmucor.varquery.query.value;

import std.sumtype;
import std.functional : partial;
import std.traits : EnumMembers;
import std.typecons : Tuple;
import std.string;
import std.algorithm.searching;
import std.conv: parse, ConvException;
import std.math.traits : isNaN, isInfinity;

/// Values can be any of string, double, or long
alias ValueExpr = SumType!(
    double,
    long,
    string
);

auto isValueNan(ValueExpr * val) {
    return (*val).match!(
        (double x) => isNaN(x),
        (_x) => false
    );
}

auto isValueInf(ValueExpr * val) {
    return (*val).match!(
        (double x) => isInfinity(x),
        (_x) => false
    );
}

pragma(inline,true)
bool isNumericStringAnInteger(const(char)[] val)
{
    if(val[0] == '-') val = val[1..$];
    foreach (c; val)
    {
        if(c < '0' || c > '9') return false;
    }
    return true;
}

auto createValueExpr(string s) {
    if(s.startsWith('"') && s.endsWith('"')){
        return new ValueExpr(s[1..$-1]);
    } 
    if(s.isNumericStringAnInteger) {
        return new ValueExpr(parse!long(s)); 
    }
    try {
        return new ValueExpr(parse!double(s));
    } catch (ConvException e) {
        return new ValueExpr(s);
    }
}



unittest {
    /// Test ints
    assert(*createValueExpr("1") == ValueExpr(1));
    assert(*createValueExpr("1000") == ValueExpr(1000));
    assert(*createValueExpr("-1000") == ValueExpr(-1000));
    
    /// Test floats
    assert(*createValueExpr("1.0") == ValueExpr(1.0));
    assert(*createValueExpr("0x1p-52") == ValueExpr(double.epsilon));
    assert(*createValueExpr("0x1.FFFFFFFFFFFFFp1023") == ValueExpr(double.max));
    assert(*createValueExpr("1.175494351e-38F") == ValueExpr(1.175494351e-38F));
    assert(*createValueExpr("-1.175494351e-38F") == ValueExpr(-1.175494351e-38F));
    assert(createValueExpr("nan").isValueNan);
    assert(createValueExpr("inf").isValueInf);

    /// Test strings
    assert(*createValueExpr("hello") == ValueExpr("hello"));
    assert(*createValueExpr("\"hello\"") == ValueExpr("hello"));
}