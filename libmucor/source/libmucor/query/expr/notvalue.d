module libmucor.query.expr.notvalue;

import std.typecons : Tuple;
import libmucor.query.value;
import libmucor.query.tokens;

struct NotValue
{
    Value value;

    this(Tokenized tokens)
    {
        tokens.popFront;
        this.value = Value(tokens.getFrontInner!string);
    }

    string toString()
    {
        return "!" ~ this.value.toString;
    }
}
