module jsonvalue;

import std.algorithm: map;
import std.sumtype: SumType, This, match, tryMatch;
import std.range: enumerate;
import std.array: array;
import asdf;
import asdf.serialization: serializeToAsdf;
import jsonlops.basic;
import std.traits: isIntegral, isSomeString, isArray, isFloatingPoint;
import khashl: khashl;

alias JsonTypes = SumType!(string, float, long, This*[], khashl!(string, This*, true, true));
alias JsonObject = JsonTypes.Types[4];
alias JsonArray = JsonTypes.Types[3];

struct JsonValue
{
    JsonTypes * value;

    this(T)(T val)
    {
        value = new JsonTypes(1L);
        *value = val;
    }

    this(JsonTypes * val)
    {
        value = val;
    }

    /// duplicate a JsonValue
    JsonValue dup()
    {
        assert(this.value);
        return (*this.value).match!(
            (long x) => JsonValue(x),
            (float x) => JsonValue(x),
            (string x) => JsonValue(x.idup),
            (JsonArray x) {
                JsonArray arr = new JsonTypes*[x.length];
                arr[] = new JsonTypes(1L);
                foreach (i,v; x)
                {
                    arr[i] = JsonValue(v).dup.value;
                }
                return JsonValue(arr); 
            },
            (JsonObject x) {
                JsonObject obj;
                foreach (kv; x.byKeyValue)
                {
                    obj[kv.key] = JsonValue(kv.value).dup.value;
                }
                return JsonValue(obj);
            }
        );
    }

    @property ulong length()
    {
        return this.asArray.length;
    }

    JsonValue opIndex(ulong i)
    {
        return JsonValue((*this.asArray)[i]);
    }

    JsonValue opIndex(string f)
    {
        return JsonValue((*this.asObject)[f]);
    }

    void opIndexAssign(JsonValue val, string f)
    {
        (*this.asObject)[f] = val.value;
    }

    void opIndexAssign(JsonValue val, ulong i)
    {
        (*this.asArray)[i] = val.value;
    }

    void opAssign(JsonValue val)
    {
        this.value = val.value;
    }

    void opAssign(T)(T val)
    if(isIntegral!T || isFloatingPoint!T || isSomeString!T || is(T == JsonObject) || is(T == JsonArray))
    {
        this = JsonValue(val);
    }

    void opAssign(T)(T val)
    if(isArray!T && !isSomeString!T && !is(T == JsonArray))
    {
        JsonArray arr = new JsonTypes*[val.length];
        foreach(j,elem; val){
            arr[j] = JsonValue(elem).value;
        }
        this = arr;
    }

    bool opEquals(JsonValue val) {
        import std.math: isClose;
        alias check = match!(
            (string a, string b) => a == b,
            // (float a, float b) => a.isClose(b),
            (long a, long b) => a == b,
            (JsonObject a, JsonObject b) {
                foreach(kv; a.byKeyValue){
                    if(!(kv.key in b)) return false;
                    if(JsonValue(kv.value) != JsonValue(b[kv.key]))
                        return false;
                }
                return true;
            },
            (JsonArray a, JsonArray b) {
                if(a.length != b.length) return false;
                foreach(i,v; a){
                    if(JsonValue(v) != JsonValue(b[i]))
                        return false;
                }
                return true;
            },
            (_a, _b) => false
        );
        return check(*this.value, *val.value);
    }

    void opIndexAssign(T)(T val, string f)
    if(isIntegral!T || isFloatingPoint!T || isSomeString!T)
    {
        static if(isIntegral!T)
            long v = cast(long) val;
        else static if(isIntegral!T)
            float v = cast(float) val;
        else
            auto v = val;

        (*this.asObject)[f] = JsonValue(v).value;
    }

    void opIndexAssign(T)(T val, ulong i)
    if(isIntegral!T || isFloatingPoint!T || isSomeString!T)
    {
        static if(isIntegral!T)
            long v = cast(long) val;
        else static if(isIntegral!T)
            float v = cast(float) val;
        else
            auto v = val;

        (*this.asArray)[i] = JsonValue(v).value;
    }

    void opIndexAssign(T)(T val, string f)
    if(isArray!T && !isSomeString!T)
    {
        JsonArray arr = new JsonTypes*[val.length];
        foreach(i,elem; val){
            arr[i] = JsonValue(elem).value;
        }
        (*this.asObject)[f] = JsonValue(arr).value;
    }

    void opIndexAssign(T)(T val, ulong i)
    if(isArray!T && !isSomeString!T)
    {
        JsonArray arr = new JsonTypes*[val.length];
        foreach(j,elem; val){
            arr[j] = new JsonTypes(elem);
        }
        (*this.asArray)[i] = new JsonTypes(arr);
    }


    // JsonValue * copy()
    // {
    //     return JsonValue((*this.value).dup);
    // }

    T asValue(T)()
    if(isIntegral!T || isFloatingPoint!T || isSomeString!T)
    {
        assert(this.value);
        static if(isIntegral!T){
            return cast(T)(*this.value).tryMatch!(
                (ref long x) => x,
            );    
        } else static if(isFloatingPoint!T){
            return cast(T)(*this.value).tryMatch!(
                (ref float x) => x,
            );   
        } else static if(isSomeString!T){
            return (*this.value).tryMatch!(
                (ref string x) => x,
            );   
        } else {}
    }

    JsonArray * asArray()
    {
        assert(this.value);
        return (*this.value).tryMatch!(
            (ref JsonArray x) => &x,
        );
    }

    JsonObject * asObject()
    {
        assert(this.value);
        return (*this.value).tryMatch!(
            (ref JsonObject x) => &x,
        );
    }

    Asdf serializeToAsdf()
    {
        assert(this.value);
        return (*this.value).match!(
            (long x) => x.serializeToAsdf,
            (float x) => x.serializeToAsdf,
            (string x) => x.serializeToAsdf,
            (ref JsonArray x) {
                Asdf[] arr;
                foreach (ref v; x)
                {
                    arr ~= JsonValue(v).serializeToAsdf;
                }
                return makeAsdfArray(arr); 
            },
            (ref JsonObject x) {
                Asdf[string] obj;
                foreach (kv; x.byKeyValue)
                {
                    obj[kv.key] = JsonValue(kv.value).serializeToAsdf;
                }
                return makeAsdfObject(obj); 
            }
        );
    }
}

JsonValue * makeJsonObject()
{
    JsonObject obj;
    return new JsonValue(obj);
}

JsonValue * makeJsonArray(ulong length = 0)
{
    JsonArray arr = new JsonTypes*[length];
    return new JsonValue(arr);
}


unittest
{
    auto a = JsonValue(1L);
    auto b = a.dup;
    b = 2L;

    assert(a.asValue!long == 1L);
    assert(b.asValue!long == 2L);

    a = 0.1f;
    b = a.dup;
    b = 0.2f;

    assert(a.asValue!float == 0.1f);
    assert(b.asValue!float == 0.2f);

    a = "test";
    b = a.dup;
    b = "test2";

    assert(a.asValue!string == "test");
    assert(b.asValue!string == "test2");

    a = *makeJsonObject();
    a["test"] = "test";
    b = a.dup;

    b["test"] = "test2";

    auto a_s = a["test"].asValue!string;

    auto b_s = b["test"].asValue!string;

    assert(a_s == "test");
    assert(b_s == "test2");

    a = ["test", "test2"];
    b = a.dup;

    b[0] = "test2";

    a_s = a[0].asValue!string;

    b_s = b[0].asValue!string;

    assert(a_s == "test");
    assert(b_s == "test2");
}

pragma(inline, true)
/// recursively normalize or flatten asdf objects
JsonValue * normalize(JsonValue * obj, string sep = ['.']){
    
    auto mut = makeJsonObject;
    normalize(obj,[],mut, sep);
    return mut;
}

/// recursively normalize or flatten asdf objects
void normalize(JsonValue * value, string path, JsonValue * mut, string sep){
    (*value.value).match!(
        (JsonArray x) {
            foreach (val; x)
            {
                auto v = new JsonValue(val);
                normalize(v, path, mut, sep);
            }
        },
        (JsonObject x) {
            if(path != [])
                path ~= sep;
            foreach (kv; x.byKeyValue)
            {
                auto v = new JsonValue(kv.value);
                normalize(v,path~kv.key, mut, sep);
            }
        },
        (string y) => normalizeValue(value, path, mut, sep),
        (long y) => normalizeValue(value, path, mut, sep),
        (float y) => normalizeValue(value, path, mut, sep),
        
    );
}

void normalizeValue(JsonValue * value, string path, JsonValue * mut, string sep){
    
    if(path in (*(*mut).asObject)){    
        
        (*(*mut)[path].value).tryMatch!(
            (JsonArray y) {
                y ~= value.value;
            },
            (string y) {
                auto arr = makeJsonArray(2);
                arr[0] = y;
                arr[1] = *value;
                (*mut)[path] = *arr;
            },
            (long y) {
                auto arr = makeJsonArray(2);
                arr[0] = y;
                arr[1] = *value;
                (*mut)[path] = *arr;
            },
            (float y) {
                auto arr = makeJsonArray(2);
                arr[0] = y;
                arr[1] = *value;
                (*mut)[path] = *arr;
            },
        );
    }else{
        (*mut)[path] = *value;
    }
}