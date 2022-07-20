module libmucor.atomize.field;

import std.traits;
import std.meta;

import mir.algebraic;
import mir.ser.interfaces;
import mir.ser;
import mir.serde;

import libmucor.atomize.serde.ser;

alias FieldTypes = Variant!(long, float, long[], float[], bool, string);

template isArrayNotString(T)
{
    static if(isArray!T && !isSomeString!T)
        enum isArrayNotString = true;
    else
        enum isArrayNotString = false;
}

struct FieldValue {
    @serdeIgnore
    FieldTypes data;
    @serdeIgnore
    bool isNull = true;

    void opAssign(T)(T value)
    {
        this.data = value;
        this.isNull = false;
    }

    void reset() {
        match!(
            suit!(isArrayNotString, (x) {
                x.length = 0;
                isNull = true;
            }),
            suit!(templateNot!isArrayNotString, (x) {
                isNull = true;
            }),
        )(this.data);
    }

    void serialize(ref VcfRecordSerializer serializer) {
        match!(
            (x) => serializeValue(serializer.serializer, x),
        )(this.data);
    }

}