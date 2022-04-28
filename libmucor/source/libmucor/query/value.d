module libmucor.query.value;

import std.sumtype;
import std.functional : partial;
import std.traits : EnumMembers;
import std.typecons : Tuple;
import std.string;
import std.algorithm.searching;
import std.conv: ConvException, to, parse;
import std.math.traits : isNaN, isInfinity;
import htslib.hts_log;

alias LongRange = Tuple!(long, "start", long, "end");
alias DoubleRange = Tuple!(double, "start", double, "end");

/// Values can be any of string, double, or long
alias ValueExpr = SumType!(
    bool, 
    double,
    long,
    string,
    DoubleRange,
    LongRange
);


struct Value {
    ValueExpr * expr;

    this(T)(T val) {
        this.expr = new ValueExpr(val);
    }

    auto isNan() {
        return (*this.expr).match!(
            (double x) => isNaN(x),
            (_x) => false
        );
    }

    auto isInf() {
        return (*this.expr).match!(
            (double x) => isInfinity(x),
            (_x) => false
        );
    }

    bool opEquals(const Value other) const
    {
        return match!(
            (bool a, bool b) => a == b,
            (long a, long b) => a == b,
            (double a, double b) => a == b,
            (string a, string b) => a == b,
            (DoubleRange a, DoubleRange b) => a == b,
            (LongRange a, LongRange b) => a == b,
            (_a, _b) => false,
        )(*this.expr, *other.expr);
    }

    T getInner(T)(){
        import std.traits;
        static if(isSomeString!T){
            return (*this.expr).tryMatch!(
                (string x) {
                    return cast(T) x;
                }
            );
        } else static if(isIntegral!T){
            return (*this.expr).tryMatch!(
                (long x) {
                    return cast(T) x;
                }
            );
        } else static if(isFloatingPoint!T){
            return (*this.expr).tryMatch!(
                (double x) {
                    return cast(T) x;
                }
            );
        } else static if(isBoolean!T){
            return (*this.expr).tryMatch!(
                (bool x) {
                    return x;
                }
            );
        } else static if(isArray!T && isFloatingPoint!(ElementType!T)){
            return (*this.expr).tryMatch!(
                (DoubleRange x) {
                    return cast(T) [x.start, x.end];
                }
            );
        } else static if(isArray!T && isIntegral!(ElementType!T)){
            return (*this.expr).tryMatch!(
                (LongRange x) {
                    return cast(T) [x.start, x.end];
                }
            );
        } else {
            static assert(0);
        }
            
    }
}

Value parseValue(string query) {
    auto q = query;
    if(q.startsWith('"') && q.endsWith('"')){
        return Value(q[1..$-1]);
    } 
    if(auto split = q.findSplit(":")){
        if(split[0].isNumericStringAnInteger && split[2].isNumericStringAnInteger) {
            return Value(LongRange(split[0].to!long, split[2].to!long));
        } else {
            try {
                return Value(DoubleRange(parse!double(split[0]), parse!double(split[2])));
            } catch (ConvException e) {
                return Value(query);
            }
        }
    } else {
        if(q.isNumericStringAnInteger) {
            return Value(parse!long(q)); 
        }
        try {
            return Value(parse!double(q));
        } catch (ConvException e) {
            if(q == "true" || q == "True" || q == "TRUE"){
                return Value(true);
            } else if(q == "false" || q == "False" || q == "FALSE"){
                return Value(false);
            } else {
                return Value(q);
            }
            
        }
    }
}

string valueToString(Value v) {
    return (*v.expr).match!(
        (string x) => x,
        (DoubleRange x) => format("%f:%f", x[0], x[1]),
        (LongRange x) => format("%d:%d", x[0], x[1]),
        (x) => x.to!string,
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


unittest {
    import std;
    /// Test ints
    assert("1".parseValue.getInner!long == 1);
    assert("1000".parseValue.getInner!long == 1000);
    assert("-1000".parseValue.getInner!long == -1000);
    
    /// Test floats
    assert("1.0".parseValue.getInner!double == 1.0);
    assert("0x1p-52".parseValue.getInner!double == double.epsilon);
    assert("0x1.FFFFFFFFFFFFFp1023".parseValue.getInner!double == double.max);
    assert("1.175494351e-38F".parseValue.getInner!double == 1.175494351e-38F);
    assert("-1.175494351e-38F".parseValue.getInner!double == -1.175494351e-38F);
    assert("nan".parseValue.isNan);
    assert("inf".parseValue.isInf);

    /// Test strings
    assert("hello".parseValue.getInner!string == "hello");
    assert("\"hello\"".parseValue.getInner!string == "hello");

    /// Test bools
    assert("true".parseValue.getInner!bool == true);
    assert("True".parseValue.getInner!bool == true);
    assert("TRUE".parseValue.getInner!bool == true);
    assert("false".parseValue.getInner!bool == false);
    assert("False".parseValue.getInner!bool == false);
    assert("FALSE".parseValue.getInner!bool == false);
}

alias NotValue = Tuple!(Value, "value");

string notValueToString(NotValue kv) {
    return "!" ~ valueToString(kv.value);
}

auto parse(T: NotValue)(string query) {
    return NotValue(parseValue(query));
}

// /// Operators that can be applied to ValueExprs
// enum ValueOp : string {
//     Equal  = "=",
//     GT     = ">",
//     LT     = "<",
//     GTE    = ">=",
//     LTE    = "<=",
//     ApproxEqual = "~"
// }

// /// Operators that can be applied to a key with a list of values 
// enum LogicalOp: string
// {
//     And    = "&",
//     Or     = "|",
//     Not    = "!"
// }

// struct Filter(LogicalOp op, T) {
//     static if(op == LogicalOp.Not) {
//         T value;
//     } else {
//         T[] values;
//     }
// }

// alias ValueFilterExpr = SumType!(
//     Filter!(LogicalOp.Not, Value),
//     Filter!(LogicalOp.Not, This*),
//     Filter!(LogicalOp.And, Value),
//     Filter!(LogicalOp.And, This*),
//     Filter!(LogicalOp.Or, Value),
//     Filter!(LogicalOp.Or, This*),
// );

// alias BasicNot = ValueFilterExpr.Types[0];
// alias ComplexNot = ValueFilterExpr.Types[1];
// alias BasicAnd = ValueFilterExpr.Types[2];
// alias ComplexAnd = ValueFilterExpr.Types[3];
// alias BasicOr = ValueFilterExpr.Types[4];
// alias ComplexOr = ValueFilterExpr.Types[5];

// auto unwrapSubQuery(ref string query) {
//     assert(query.startsWith("("));
//     query = query[1..$];
//     auto split = query.findSplit(")");
//     query = split[2].stripLeft;
//     return split[0];
// }

// auto parse(T: ValueFilterExpr)(string query) {
//     ValueFilterOp op;
//     bool isComplex;
//     bool opSet;
//     bool first;
//     Value[] values;
//     ValueFilterExpr * [] filters;
//     auto q = query;
//     while(true) {
//         try {
//             auto seenOp = parse!LogicalOp(q);
//             if(first && op != LogicalOp.Not) {
//                 hts_log_error(__FUNCTION__, format("Only the ! operator can be used in unary form: %s", query));
//             }
//             if(!opSet) opSet = true, op = seenOp;
//             else{
//                 if(seenOp != op && seenOp != LogicalOp.Not)
//                     hts_log_error(__FUNCTION__, format("Operator ambiguity found, Please disambguate with parentheses: %s", query));

//                 q = q.stripLeft;
//                 if(query.startsWith("(")) {
//                     if(!balancedParens(query, '(', ')')){
//                         hts_log_error(__FUNCTION__, format("Parentheses aren't matched in query: %s", query));
//                     }

//                     isComplex = true;
//                     auto split = q.findSplit(")");
//                     filters ~= parseValueFilter(split[0][1..$]);
//                     q = split[2];
//                 } else {

//                 }
//             }
//         }
//         {
//             if(query.startsWith("(")) {
//                 if(!balancedParens(query, '(', ')')){
//                     hts_log_error(__FUNCTION__, format("Parentheses aren't matched in query: %s", query));
//                 }
//                 isComplex = true;
//                 auto split = q.findSplit(")");
//                 filters ~= parseValueFilter(split[0][1..$]);
//                 q = split[2];
//             }
//         }
//     }
// }
// // auto createKeyFilterExpr(string query_str) {
//     //     string key = parseKey(query_str);
//     //     ValueOp op;
//     //     ValueExpr * [] values;
//     //     try {
//     //         op = query_str.parseValueOp;
//     //         expr = createValueExpr(query_str);
//     //     } catch(ConvException e) {
//     //         hts_log_error(__FUNCTION__, format("Could not find a valid operator in in query portion: %s", query_str));
//     //     }
//     //     return KeyValueExpr(op, key, expr);
//     // }

// alias ValueFilterExpr = 

// // alias KeyFilterExpr = SumType!(

// // )



// // alias PrimaryQuery = SumType!(
// //     KeyValueExpr,
// //     KeyOpExpr,
// //     KeyFilterExpr
// // );