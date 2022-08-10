module libmucor.query.expr.query;

import std.sumtype;
import std.typecons : Tuple;
import libmucor.query : queryErr;
import libmucor.query.key;
import libmucor.query.value;
import libmucor.query.expr.keyvalue;
import libmucor.query.expr.notvalue;
import libmucor.query.tokens;
import libmucor.query.util;
import libmucor.query.eval;
import libmucor.invertedindex;

alias BasicQueryExpr = SumType!(Value,/// key op value
        KeyValue,/// key op
        UnaryKeyOp);

struct BasicQuery
{
    BasicQueryExpr* expr;

    this(Value ex)
    {
        this.expr = new BasicQueryExpr(ex);
    }

    this(KeyValue ex)
    {
        this.expr = new BasicQueryExpr(ex);
    }

    this(UnaryKeyOp ex)
    {
        this.expr = new BasicQueryExpr(ex);
    }

    this(BasicQueryExpr ex)
    {
        this.expr = new BasicQueryExpr;
        *this.expr = ex;
    }

    this(Tokenized tokens)
    {
        assert(tokens.length == 3 || tokens.length == 1);
        if (tokens.length == 1)
        {
            this.expr = new BasicQueryExpr(Value(tokens.getFrontInner!string));
            return;
        }
        auto op = tokens[1 .. 2].getFrontInner!ValueOp;
        if (op == ValueOp.KeyOperator)
            this.expr = new BasicQueryExpr(UnaryKeyOp(tokens));
        else
            this.expr = new BasicQueryExpr(KeyValue(tokens));
    }

    string toString()
    {
        return (*this.expr).match!((Value x) => x.toString(),
                (KeyValue x) => x.toString(), (UnaryKeyOp x) => x.toString());
    }

    bool opEquals(const BasicQuery other) const
    {
        return match!((const Value a, const Value b) => a == b, (const KeyValue a,
                const KeyValue b) => a == b, (const UnaryKeyOp a,
                const UnaryKeyOp b) => a == b, (_a, _b) => false,)(*this.expr, *other.expr);
    }

    /// Get inner type for testing
    T getInner(T)()
    {
        return (*this.expr).tryMatch!((T x) { return x; });
    }
}

alias QueryExpr = SumType!(BasicQuery, Tuple!(Key, "lhs", This*, "rhs"),
        Tuple!(UnaryLogicalOp, "op", This*, "negated"), Tuple!(This*, "sub"),
        Tuple!(BinaryLogicalOp, "op", This*, "lhs", This*, "rhs"),);

alias ComplexKeyValue = QueryExpr.Types[1];
alias NotQueryExpr = QueryExpr.Types[2];
alias SubQueryExpr = QueryExpr.Types[3];
alias ComplexQuery = QueryExpr.Types[4];

struct Query
{
    QueryExpr* expr;

    this(Value ex)
    {
        this.expr = new QueryExpr(BasicQuery(ex));
    }

    this(KeyValue ex)
    {
        this.expr = new QueryExpr(BasicQuery(ex));
    }

    this(UnaryKeyOp ex)
    {
        this.expr = new QueryExpr(BasicQuery(ex));
    }

    this(BasicQuery ex)
    {
        this.expr = new QueryExpr(ex);
    }

    this(QueryExpr ex)
    {
        this.expr = new QueryExpr;
        *this.expr = ex;
    }

    this(string query_str)
    {
        validateParenthesis(query_str);
        auto tokens = Tokenized(query_str);
        this.expr = parseQueryRecurse(tokens);
    }

    string toString()
    {
        return (*this.expr).match!((BasicQuery x) => x.toString(),
                (ComplexKeyValue x) => x.lhs.key ~ "=" ~ Query(*x.rhs)
                .toString(), (NotQueryExpr x) => "!" ~ Query(*x.negated)
                .toString(), (SubQueryExpr x) => "(" ~ Query(*x.sub)
                .toString() ~ ")", (ComplexQuery x) => Query(*x.lhs)
                .toString() ~ cast(string) x.op ~ Query(*x.rhs).toString(),);
    }

    auto evaluate(ref InvertedIndex idx)
    {
        return evaluateQuery(this.expr, idx);
    }

    bool opEquals(const Query other) const
    {
        return match!((const BasicQuery a, const BasicQuery b) => a == b,
                (const ComplexKeyValue a, const ComplexKeyValue b) => a.lhs == b.lhs
                && a.rhs == b.rhs, (const NotQueryExpr a,
                    const NotQueryExpr b) => a.op == b.op && a.negated == b.negated,
                (const SubQueryExpr a, const SubQueryExpr b) => a.sub == b.sub,
                (const ComplexQuery a, const ComplexQuery b) => a.op == b.op
                && a.lhs == b.lhs && a.rhs == b.rhs, (_a, _b) => false,)(*this.expr, *other.expr);
    }

    /// Get inner type for testing
    T getInner(T)()
    {
        return (*this.expr).tryMatch!((T x) { return x; });
    }
}

QueryExpr* parseQueryRecurse(Tokenized tokens)
{
    if (tokens.length == 0)
    {
        queryErr(tokens.original, 0, "Unexpected end of query");
    }
    auto isParenthesis1 = (Operators v) => v.match!((Parenthesis x) => true, (_x) => false,);
    auto isParenthesis = (TokenizedTypes v) => v.match!(
            (Operators x) => isParenthesis1(x), (_x) => false);
    auto matchOp = (Operators v, Tokenized toks) => v.match!((Parenthesis x) {
        if (toks.length == 1)
            queryErr(toks.original, toks.idxs[0], "Unexpected end of query");
        if (x == Parenthesis.Right)
            queryErr(toks.original, toks.idxs[0], "Unexpected \")\"");
        auto group = toks.sliceToNextClosingParenthesis;
        if (toks.length == group.length)
        {
            return new QueryExpr(SubQueryExpr(parseQueryRecurse(group[1 .. $ - 1])));
        }
        else
        {
            auto lhs = parseQueryRecurse(group[1 .. $ - 1]);
            auto op = toks[group.length .. group.length + 1].getFrontInner!BinaryLogicalOp;
            auto rhs = parseQueryRecurse(toks[group.length + 1 .. $]);
            return new QueryExpr(ComplexQuery(op, lhs, rhs));
        }
    }, (UnaryLogicalOp x) {
        if (toks.length == 1)
            queryErr(toks.original, toks.idxs[0], "Unexpected end of query");
        if (isParenthesis(toks[1]))
        {
            toks.popFront;
            auto group = toks.sliceToNextClosingParenthesis;
            if (toks.length == group.length)
            {
                return new QueryExpr(NotQueryExpr(x, parseQueryRecurse(group)));
            }
            else
            {
                auto lhs = parseQueryRecurse(group);
                auto op = toks[group.length .. group.length + 1].getFrontInner!BinaryLogicalOp;
                auto rhs = parseQueryRecurse(toks[group.length + 1 .. $]);
                return new QueryExpr(ComplexQuery(op, new QueryExpr(NotQueryExpr(x, lhs)), rhs));
            }
        }
        return new QueryExpr(NotQueryExpr(x, parseQueryRecurse(toks[1 .. $])));
    }, (_x) {
        queryErr(toks.original, toks.idxs[0], "Unexpected operator");
        return null;
    });

    auto isValueOp1 = (Operators v) => v.match!((ValueOp x) => true, (_x) => false,);
    auto isValueOp = (TokenizedTypes v) => v.match!((Operators x) => isValueOp1(x), (_x) => false);

    auto isString = (TokenizedTypes v) => v.match!((Operators x) => false, (string x) => true);

    return tokens[0].match!((Operators x) => matchOp(x, tokens), (string x) {
        /// Value or UnaryKeyOp
        if (tokens.length < 3)
        {
            return new QueryExpr(BasicQuery(tokens));
        }
        /// KeyValue or value op value
        else if (tokens.length == 3)
        {
            if (isValueOp(tokens[1]))
            {
                return new QueryExpr(BasicQuery(tokens));
            }
            else
            {
                auto lhs = parseQueryRecurse(tokens[0 .. 1]);
                tokens.popFront;
                auto op = tokens.getFrontInner!BinaryLogicalOp;
                tokens.popFront;
                auto rhs = parseQueryRecurse(tokens[0 .. 1]);
                return new QueryExpr(ComplexQuery(op, lhs, rhs));
            }
        }
        else
        {
            if (isValueOp(tokens[1]) && isParenthesis(tokens[2]))
            {
                auto lhs = Key(tokens.getFrontInner!string);
                tokens.popFront;
                auto op = tokens.getFrontInner!ValueOp;
                if (op != ValueOp.Equal)
                {
                    queryErr(tokens.original, tokens.idxs[0], "Expected \"=\"");
                }
                auto rhs = parseQueryRecurse(tokens[1 .. $]);
                return new QueryExpr(ComplexKeyValue(lhs, rhs));
                /// key = ( other )
            }
            else
            {
                auto lhs = parseQueryRecurse(tokens[0 .. 1]);
                tokens.popFront;
                auto op = tokens.getFrontInner!BinaryLogicalOp;
                tokens.popFront;
                auto rhs = parseQueryRecurse(tokens);
                return new QueryExpr(ComplexQuery(op, lhs, rhs));
            }
        }

    });
}
