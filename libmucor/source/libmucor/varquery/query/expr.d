module libmucor.varquery.query.expr;

import std.sumtype;
import std.functional : partial;
import std.traits : EnumMembers;
import std.typecons : Tuple;
import std.string;
import std.algorithm.searching;

import libmucor.varquery.query.primary;

enum QueryOp : string
{
    And    = "&",
    Or     = "|",
    Not    = "!",
}
alias QueryExpr = SumType!(
    PrimaryQuery,
    Tuple!(QueryOp, "op", This*, "lhs", This*, "rhs")
);

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