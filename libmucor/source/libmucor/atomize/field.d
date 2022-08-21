module libmucor.atomize.field;

import libmucor.serde.ser;

import std.traits;
import std.meta;

import mir.algebraic;
import mir.ser;

alias FieldTypes = Variant!(long, float, long[], float[], bool, string);

template isArrayNotString(T)
{
    static if (isArray!T && !isSomeString!T)
        enum isArrayNotString = true;
    else
        enum isArrayNotString = false;
}

struct FieldValue
{
    FieldTypes data;
    bool isNull = true;

    void opAssign(T)(T value)
    {
        this.data = value;
        this.isNull = false;
    }

    void reset()
    {
        match!(suit!(isArrayNotString, (x) { x.length = 0; isNull = true; }),
                suit!(templateNot!isArrayNotString, (x) { isNull = true; }),)(this.data);
    }

    void serialize(ref VcfRecordSerializer serializer)
    {
        match!((string x) => serializer.putValue(x),
                (x) => serializeValue(serializer.serializer, x),)(this.data);
    }

}
