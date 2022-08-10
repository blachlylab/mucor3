module libmucor.option;

import mir.ser;
import mir.ser.interfaces;

template isOption(T)
{
    alias inner(O : Option!(I), I) = I;
    static if (__traits(compiles, inner!T))
        enum isOption = true;
    else
        enum isOption = false;
}

static assert(isOption!(Option!string));
static assert(!isOption!(string));

enum None = null;

struct Option(T)
{
    private T val;
    bool isNone = true;

    void opAssign(Option!T value)
    {
        this.val = value.val;
        this.isNone = value.isNone;
    }

    void opAssign(typeof(null) _null)
    {
        this.isNone = true;
    }

    auto unwrap() @safe const
    {
        assert(!this.isNone, "Tried to unwrap None option");
        return this.val;
    }

    bool serdeIgnoreOut() const
    {
        return this.isNone;
    }

    void serialize(S)(ref S serializer)
    {
        if (!isNone)
            serializer.putValue(this.val);
    }
}

auto Some(T)(T val)
{
    return Option!T(val, false);
}
