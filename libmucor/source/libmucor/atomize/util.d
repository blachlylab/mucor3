module libmucor.atomize.util;
import std.traits : EnumMembers;
import mir.serde;
import mir.utility : _expect;

import libmucor.error;

T enumFromStr(T)(ref string myString) @nogc nothrow @safe
{
    T v;
    if (_expect(serdeParseEnum(myString, v), true))
    {
        return v;
    }
    else
    {
        log_err(__FUNCTION__, "Can't convert string to enum: %s -> %s", myString, T.stringof);
        return T.init;
    }
}

string enumToString(T)(T e) @nogc nothrow @safe
{
    final switch (e)
    {
        // dfmt off
        static foreach (key; EnumMembers!T)
        {
            case key:
                return serdeGetKeyOut(e);
        }
        // dfmt on
    }
}
