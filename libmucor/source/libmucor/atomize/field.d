module libmucor.atomize.field;

import libmucor.serde.ser;

import std.traits;
import std.meta;

import mir.algebraic;
import mir.ser;

// alias FieldTypes = Variant!(long, float, long[], float[], bool, string);

enum FieldTypes {
    Long,
    Float,
    LongArr,
    FloatArr,
    Bool,
    String
}

union Field {
    long l;
    float f;
    bool b;
    const(char)[] s;
    long[] larr;
    float[] farr;
}

struct FieldValue
{
    Field data;
    FieldTypes type;
    bool isNull = true;

    @nogc nothrow @trusted: 
    void opAssign(long[] value)
    {
        this.type = FieldTypes.LongArr;
        this.data.larr = value;
        this.isNull = false;
    }

    void opAssign(float[] value)
    {
        this.type = FieldTypes.FloatArr;
        this.data.farr = value;
        this.isNull = false;
    }

    void opAssign(long value)
    {
        this.type = FieldTypes.Long;
        this.data.l = value;
        this.isNull = false;
    }

    void opAssign(float value)
    {
        this.type = FieldTypes.Float;
        this.data.f = value;
        this.isNull = false;
    }

    void opAssign(const(char)[] value)
    {
        this.type = FieldTypes.String;
        this.data.s = value;
        this.isNull = false;
    }

    void opAssign(bool value)
    {
        this.type = FieldTypes.Bool;
        this.data.b = value;
        this.isNull = false;
    }

    void reset()
    {
        this.isNull = true;
    }

    void serialize(ref VcfRecordSerializer serializer)
    {
        final switch(type) {
            case FieldTypes.Long:
                serializer.putValue(this.data.l);
                return;
            case FieldTypes.Float:
                serializer.putValue(this.data.f);
                return;
            case FieldTypes.String:
                serializer.putValue(this.data.s);
                return;
            case FieldTypes.Bool:
                serializer.putValue(this.data.b);
                return;
            case FieldTypes.LongArr:
                serializeValue(serializer.serializer, this.data.larr);
                return;
            case FieldTypes.FloatArr:
                serializeValue(serializer.serializer, this.data.farr);
                return;
        }
    }

}
