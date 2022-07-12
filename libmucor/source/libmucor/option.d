module libmucor.option;

import mir.ser;
import mir.ser.interfaces;

enum None = null;

struct Option(T) {
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

    auto unwrap() {
        assert(!this.isNone, "Tried to unwrap None option");
        return this.val;
    }

    bool serdeIgnoreOut() const
    {
        return this.isNone;
    }


    void serialize(ISerializer serializer) const @safe {
        if(!isNone)
            serializer.putValue(this.val);
    }
}

auto Some(T)(T val) {
    return Option!T(val, false);
}