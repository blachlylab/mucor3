module libmucor.atomize.util;
import std.traits : EnumMembers;
import mir.serde;

T enumFromStr(T)(ref string myString)
{
    T v;
    if(serdeParseEnum(myString, v)){
        return v;
    } else {
        throw new Exception("Can't convert string to enum: " ~ myString ~" -> "~ T.stringof);
    }
}