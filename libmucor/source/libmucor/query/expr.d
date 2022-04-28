module libmucor.query.expr;

import std.sumtype;
import std.functional : partial;
import std.traits : EnumMembers;
import std.typecons : Tuple, tuple;
import std.string;
import std.algorithm.searching;
import std.conv: ConvException, parse;

// import libmucor.query.primary;
import libmucor.query.value;
import libmucor.query.keyvalue;
import libmucor.query.util;

/// Operators that can be applied to ValueExprs
enum ValueOp : string {
    Equal  = "=",
    GTE    = ">=",
    LTE    = "<=",
    GT     = ">",
    LT     = "<",
    ApproxEqual = "~"
}

/// Operators that can be applied to a key with a list of values 
enum LogicalOp: string
{
    And    = "&",
    Or     = "|",
    Not    = "!"
}

/// Operators that can be applied to a key 
enum KeyOp: string
{
    Exists = "_exists_"
}


/**
* Logic for parsing string query and filter results using inverted index.
* Uses NOT, AND, and OR operators with key and values represent as key:value
* e.g.: key1=val1 -> get all records where key1=val1
* e.g.: key1=val1 AND key2=val2 -> get all records where key1=val1 and key2=val2
* e.g.: key1=val1 OR key2=val2 -> get all records where key1=val1 or key2=val2
* e.g.: key1=(val1 OR key2) -> get all records where key1=val1 or key1=val2
* can get complicated
* e.g.: NOT (key1:val1 AND key2:(val2 OR val3) AND key3:1-2) OR key4:val4 OR key5:(val5 AND val6)
*
* Operators: =, :, >, >=, <, <=, AND, NOT, OR
* Other specials: (, )
**/

alias Query = SumType!(
    /// key op value
    KeyValue,
    /// key op
    UnaryKeyOp,
    /// Not value
    NotValue,
    Value,
    /// key op query
    Tuple!(ValueOp, "op", string, "lhs", This*, "rhs"),
    /// Not query
    Tuple!(This*, "rhs"),
    /// query op query
    Tuple!(LogicalOp, "op", This*, "lhs", This*, "rhs"),
    /// ( subquery )
    Tuple!(This*, "subquery")
);

alias ComplexKeyValue = Query.Types[4];

string complexKeyValueToString(ComplexKeyValue kv) {
    return kv.lhs ~ cast(string) kv.op ~ queryToString(*kv.rhs);
}

alias NotQuery = Query.Types[5];

string notQueryToString(NotQuery kv) {
    return "!" ~ queryToString(*kv.rhs);
}

auto parseNotQuery(string query) {
    assert(query[0] == '!');
    query = query[1..$];
    if(query[0] == '(') {
        auto split = splitOnClosingParenthesis(query);
        auto rest = split[1].strip;
        import std.stdio;
        writeln(rest);
        try {
            auto op = enumFromStr!LogicalOp(rest);
            /// !(foo | bar | baz) & lame
            return new Query(ComplexQuery(op, new Query(NotQuery(new Query(Subquery(parseQuery(split[0].strip))))), parseQuery(rest.strip)));
        } catch (Exception) {
            /// !(foo | bar | baz)
            return new Query(NotQuery(parseQuery(query.strip)));
        }
    } else {
        return new Query(NotValue(parseValue(query.strip)));
    }
}

auto parseSubQuery(string query) {
    auto split = splitOnClosingParenthesis(query);
    auto rest = split[1].strip;
    try {
        auto op = enumFromStr!LogicalOp(rest);
        /// (some query) & other
        return new Query(ComplexQuery(op, new Query(Subquery(parseQuery(split[0].strip))), parseQuery(rest.strip)));
    } catch (Exception) {
        /// (some query)
        return new Query(Subquery(parseQuery(split[0].strip)));
    }
}

auto parseKeyValue(string key, ValueOp op, string value) {
    auto k = parseKey(key);
    if(value.strip[0] == '(') {
        return new Query(ComplexKeyValue(op, key.strip, parseQuery(value.strip)));
    } else {
        return new Query(KeyValue(op, k.strip, parseValue(value.strip)));
    }
}

alias ComplexQuery = Query.Types[6];

string complexQueryToString(ComplexQuery kv) {
    return queryToString(*kv.lhs) ~ cast(string) kv.op ~ queryToString(*kv.rhs);
}

alias Subquery = Query.Types[7];

string subqueryToString(Subquery kv) {
    return "(" ~ queryToString(*kv.subquery) ~ ")";
}


Query * parseQuery(string query) {
    query = query.strip;
    auto vop = splitOnValueOp(query);
    auto lop = splitOnLogicalOp(query);
    auto uop = query.findAmong(":");
    if(query[0] == '!') {
        return parseNotQuery(query);
    } else if(query[0] == '(') {
        return parseSubQuery(query);
    } else if(vop[0]) {
        return parseKeyValue(vop[1].strip, vop[2], vop[3].strip);
    } else if(lop[0]) {
        return new Query(ComplexQuery(lop[2], parseQuery(lop[1].strip), parseQuery(lop[3].strip)));
    } else if(!uop.empty) {
        return new Query(parseUnaryKeyOp(query));
    } else {
        return new Query(parseValue(query));
    }
}

string queryToString(Query q) {
    return q.match!(
        (KeyValue x) => keyValueToString(x),
        (UnaryKeyOp x) => unaryKeyOpToString(x),
        (NotValue x) => notValueToString(x),
        (Value x) => valueToString(x),
        (ComplexKeyValue x) => complexKeyValueToString(x),
        (NotQuery x) => notQueryToString(x),
        (ComplexQuery x) => complexQueryToString(x),
        (Subquery x) => subqueryToString(x),
    );
}

unittest
{
    import std.stdio;
    auto q = parseQuery("1.0");
    assert(*q == Query(Value(1.0)));

    q = parseQuery("\"key\": _exists_");
    assert(*q == Query(UnaryKeyOp(KeyOp.Exists, "key")));

    q = parseQuery("val1 | 1 | 2.1");
    assert((*q).queryToString() == "val1|1|2.1");

    q = parseQuery("key = val");
    writeln((*q).queryToString());
    assert((*q).queryToString() == "key=val");

    q = parseQuery("(key = val)");
    writeln((*q).queryToString());
    assert((*q).queryToString() == "(key=val)");

    q = parseQuery("!val");
    writeln((*q).queryToString());
    assert((*q).queryToString() == "!val");

    q = parseQuery("!(key = val)");
    writeln((*q).queryToString());
    assert((*q).queryToString() == "!(key=val)");

    q = parseQuery("!(key = val)");
    writeln((*q).queryToString());
    assert((*q).queryToString() == "!(key=val)");

    q = parseQuery("!(key = val) & (foo = bar)");
    writeln((*q).queryToString());
    assert((*q).queryToString() =="!(key=val)&(foo=bar)");

    q = parseQuery("(!(key = 1:2) & (foo = ( bar | 3 | (baz & test & v))))");
    writeln((*q).queryToString());
    assert((*q).queryToString() =="(!(key=1:2)&(foo=(bar|3|(baz&test&v))))");
    // assert(*q == Query(UnaryKeyOp(KeyOp.Exists, "key")));
}


// enum QueryOp : string
// {
//     And    = "&",
//     Or     = "|",
//     Not    = "!",
// }
// alias QueryExpr = SumType!(
//     PrimaryQuery,
//     Tuple!(QueryOp, "op", This*, "lhs", This*, "rhs")
// );

// // Shorthand for Tuple!(QueryOp, "op", This*, "lhs", This*, "rhs")
// // the Tuple type above with QueryExpr substituted for This.
// alias BinOp = QueryExpr.Types[3];

// // Factory function for number expressions
// ValueExpr* floatValue(double value)
// {
//     return new ValueExpr(value);
// }

// // Factory function for number expressions
// ValueExpr* integralValue(long value)
// {
//     return new ValueExpr(value);
// }

// // Factory function for variable expressions
// ValueExpr* var(string name)
// {
//     return new ValueExpr(name);
// }

// // Factory function for binary operation expressions
// QueryExpr* binOp(QueryOp op, QueryExpr* lhs, QueryExpr* rhs)
// {
//     match!(
//         (string a, ValueExpr b) {
            
//         },
//         (Point3D _1, Point3D _2) => true,
//         (_1, _2) => false
//     )
//     return ;
// }

// // Convenience wrappers for creating BinOp expressions
// alias andExpr = partial!(binOp, QueryOp.And);
// alias orExpr = partial!(binOp, QueryOp.Or);
// alias notExpr = partial!(binOp, QueryOp.Not);
// alias equalExpr = partial!(binOp, QueryOp.Equal);
// alias gtExpr = partial!(binOp, QueryOp.GT);
// alias ltExpr = partial!(binOp, QueryOp.LT);
// alias gteExpr = partial!(binOp, QueryOp.GTE);
// alias lteExpr = partial!(binOp, QueryOp.LTE);
// alias RangeExpr = partial!(binOp, QueryOp.Range);



// auto parseQuery(string query) {
//     stripRight(query)
// }

// auto parseNumber(string query) {
    
// }

// auto parseKey(string query) {
    
// }

// auto checkForMatchingParentheses(string q){

// }