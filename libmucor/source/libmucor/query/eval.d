module libmucor.query.eval;

import std.sumtype;
import std.functional : partial;
import std.traits : EnumMembers;
import std.typecons : Tuple, tuple;
import std.string;
import std.algorithm.searching;
import std.conv : ConvException, parse;

// import libmucor.query.primary;
import libmucor.query.value;
import libmucor.query.expr;
import libmucor.query.util;
import libmucor.query.tokens;
import libmucor.khashl;
import libmucor.invertedindex;
import libmucor.error;

khashlSet!(ulong) * evaluateQuery(QueryExpr * queryExpr, InvertedIndex* idx, string lastKey = "")
{
    auto matchBasic = (BasicQueryExpr x, string lk) => x.match!(
        (Value x) {
            if (lk == "")
                log_err(__FUNCTION__, "Key cannot be null");
            return queryValue(lk, x, idx);
        } , 
        (KeyValue x) {
            switch (x.op)
            {
                case ValueOp.Equal:
                    return queryValue(x.lhs.key, x.rhs, idx);
                case ValueOp.ApproxEqual:
                    return queryValue(x.lhs.key, x.rhs, idx);
                default:
                    auto res = queryOpValue(x.lhs.key, x.rhs, idx, cast(string) x.op);
                    return res;
            }
        },
        /// key op
        (UnaryKeyOp x) {
            final switch (x.op)
            {
                case KeyOp.Exists:
                    return new khashlSet!(ulong)(); /// TODO: complete
            }
        }
    );
    return (*queryExpr).match!(
        (BasicQuery x) => matchBasic(*x.expr, lastKey), 
        (ComplexKeyValue x) => evaluateQuery(x.rhs, idx,x.lhs.key),
        (NotQueryExpr x) => idx.queryNOT(evaluateQuery(x.negated, idx, lastKey)), 
        (ComplexQuery x) {
            final switch (x.op)
            {
                case BinaryLogicalOp.And:
                    auto a = evaluateQuery(x.rhs, idx, lastKey);
                    auto b = evaluateQuery(x.lhs, idx, lastKey);
                    return intersectIds(a, b);
                case BinaryLogicalOp.Or:
                    auto a = evaluateQuery(x.rhs, idx, lastKey);
                    auto b = evaluateQuery(x.lhs, idx, lastKey);
                    return unionIds(a, b);
            }
        }, (SubQueryExpr x) => evaluateQuery(x.sub, idx, lastKey),);
}

khashlSet!(const(char)[]) * getQueryFields(QueryExpr * queryExpr, khashlSet!(const(char)[]) * keys)
{
    if(!keys) keys = new khashlSet!(const(char)[]);
    auto matchBasic = (BasicQueryExpr x, khashlSet!(const(char)[]) * k) => x.match!(
        (Value x) {
            return k;
        } , 
        (KeyValue x) {
            k.insert(x.lhs.key);
            return k;
        },
        /// key op
        (UnaryKeyOp x) {
            k.insert(x.lhs.key);
            return k;
        }
    );
    return (*queryExpr).match!(
        (BasicQuery x) => matchBasic(*x.expr, keys),
        (ComplexKeyValue x) {
            keys.insert(x.lhs.key);
            return keys;
        }, 
        (NotQueryExpr x) {
            getQueryFields(x.negated, keys);
            return keys;
        }, 
        (ComplexQuery x) {
            getQueryFields(x.rhs,keys);
            getQueryFields(x.lhs,keys);
            return keys;
        }, 
        (SubQueryExpr x) => getQueryFields(x.sub,keys)
    );
}

khashlSet!(ulong) * queryValue(const(char)[] key, Value value, InvertedIndex* idx)
{

    return (*value.expr).match!((bool x) => idx.query(key, x),
            (long x) => idx.query(key, x), (double x) => idx.query(key, x),
            (string x) => idx.query(key, x),
            (DoubleRange x) => idx.queryRange(key, x[0], x[1]),
            (LongRange x) => idx.queryRange(key, x[0], x[1]),);
}

khashlSet!(ulong) * queryOpValue(const(char)[] key, Value value, InvertedIndex* idx, string op)
{
    alias f = tryMatch!((long x) { return idx.queryOp!long(key, x, op); }, (double x) {
        return idx.queryOp!double(key, x, op);
    });
    return f(*value.expr);
}

khashlSet!(ulong) * unionIds(khashlSet!(ulong) * a, khashlSet!(ulong) * b){
    (*a) |= (*b);
    return a;
}

khashlSet!(ulong) * intersectIds(khashlSet!(ulong) * a, khashlSet!(ulong) * b){
    (*a) &= (*b);
    return a;
}

khashlSet!(ulong) * negateIds(khashlSet!(ulong) * a, khashlSet!(ulong) * b){
    (*a) |= (*b);
    return a;
}
