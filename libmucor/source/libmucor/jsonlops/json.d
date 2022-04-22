module libmucor.jsonlops.json;

import std.algorithm: map;
import std.sumtype: SumType, This, match, tryMatch;
import std.range: enumerate;
import std.array: array;
import asdf;
import asdf.serialization: serializeToAsdf;
import libmucor.jsonlops.basic;
import std.traits: isIntegral, isSomeString, isArray, isFloatingPoint;
import libmucor.khashl: khashl;

alias JsonTypes = SumType!(string, float, long, This[], khashl!(string, This, true));
alias JsonObject = JsonTypes.Types[4];
alias JsonArray = JsonTypes.Types[3];

struct Json
{
    JsonTypes value;

    this(T)(T val)
    {
        value = val;
    }

    /// duplicate a Json
    Json dup()
    {
        return (this.value).match!(
            (long x) => Json(x),
            (float x) => Json(x),
            (string x) => Json(x.idup),
            (JsonArray x) => Json(x.dup),
            (JsonObject x) => Json(x.dup)
        );
    }

    @property ulong length()
    {
        return (*this.asArrayRef).length;
    }

    Json * opIndex(ulong i)
    {
        return cast(Json *) &((*this.asArrayRef)[i]);
    }

    Json * opIndex(string f)
    {
        return cast(Json *) &((*this.asObjectRef)[f]);
    }

    void opIndexAssign(Json val, string f)
    {
        (*this.asObjectRef)[f] = val.value;
    }

    void opIndexAssign(Json val, ulong i)
    {
        (*this.asArrayRef)[i] = val.value;
    }

    void opAssign(Json val)
    {
        this.value = val.value;
    }

    void opAssign(T)(T val)
    if(isIntegral!T || isFloatingPoint!T || isSomeString!T || is(T == JsonObject) || is(T == JsonArray))
    {
        this = Json(val);
    }

    void opAssign(T)(T val)
    if(isArray!T && !isSomeString!T && !is(T == JsonArray))
    {
        JsonArray arr = new JsonTypes[val.length];
        foreach(j,elem; val){
            arr[j] = Json(elem).value;
        }
        this = arr;
    }

    bool opEquals(Json val) {
        import std.math: isClose;
        alias check = match!(
            (string a, string b) => a == b,
            // (float a, float b) => a.isClose(b),
            (long a, long b) => a == b,
            (JsonObject a, JsonObject b) {
                foreach(kv; a.byKeyValue){
                    if(!(kv.key in b)) return false;
                    if(Json(kv.value) != Json(b[kv.key]))
                        return false;
                }
                return true;
            },
            (JsonArray a, JsonArray b) {
                if(a.length != b.length) return false;
                foreach(i,v; a){
                    if(Json(v) != Json(b[i]))
                        return false;
                }
                return true;
            },
            (_a, _b) => false
        );
        return check(this.value, val.value);
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

        (*this.asObjectRef)[f] = Json(v).value;
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

        (*this.asArrayRef)[i] = Json(v).value;
    }

    void opIndexAssign(T)(T val, string f)
    if(isArray!T && !isSomeString!T)
    {
        JsonArray arr = new JsonTypes[val.length];
        foreach(i,elem; val){
            arr[i] = Json(elem).value;
        }
        (*this.asObjectRef)[f] = Json(arr).value;
    }

    void opIndexAssign(T)(T val, ulong i)
    if(isArray!T && !isSomeString!T)
    {
        JsonArray arr = new JsonTypes[val.length];
        foreach(j,elem; val){
            arr[j] = JsonTypes(elem);
        }
        (*this.asArrayRef)[i] = JsonTypes(arr);
    }


    // Json * copy()
    // {
    //     return Json((*this.value).dup);
    // }

    T asValue(T)()
    if(isIntegral!T || isFloatingPoint!T || isSomeString!T)
    {
        static if(isIntegral!T){
            return cast(T)(this.value).tryMatch!(
                (ref long x) => x,
            );    
        } else static if(isFloatingPoint!T){
            return cast(T)(this.value).tryMatch!(
                (ref float x) => x,
            );   
        } else static if(isSomeString!T){
            return (this.value).tryMatch!(
                (ref string x) => x,
            );   
        } else {}
    }

    JsonArray * asArrayRef()
    {
        return this.value.tryMatch!(
            (ref JsonArray x) => &x,
        );
    }

    JsonObject * asObjectRef()
    {
        return this.value.tryMatch!(
            (ref JsonObject x) => &x,
        );
    }

    Asdf serializeToAsdf()
    {
        return this.value.match!(
            (long x) => x.serializeToAsdf,
            (float x) => x.serializeToAsdf,
            (string x) => x.serializeToAsdf,
            (ref JsonArray x) {
                Asdf[] arr;
                foreach (ref v; x)
                {
                    arr ~= Json(v).serializeToAsdf;
                }
                return makeAsdfArray(arr); 
            },
            (ref JsonObject x) {
                Asdf[string] obj;
                foreach (ref kv; x.byKeyValue)
                {
                    obj[kv.key] = Json(kv.value).serializeToAsdf;
                }
                return makeAsdfObject(obj); 
            }
        );
    }
}

Json makeJsonObject()
{
    JsonObject obj;
    return Json(obj);
}

Json makeJsonArray(ulong length = 0)
{
    JsonArray arr = new JsonTypes[length];
    return Json(arr);
}


unittest
{
    auto a = Json(1L);
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

    a = makeJsonObject();
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
Json normalize(Json obj, string sep = ['.']){
    
    auto mut = makeJsonObject;
    normalize(obj,[],mut, sep);
    return mut;
}

/// recursively normalize or flatten asdf objects
void normalize(const Json value, string path, Json mut, string sep){
    value.value.match!(
        (const JsonArray x) {
            foreach (val; x)
            {
                auto v = Json(val);
                normalize(v, path, mut, sep);
            }
        },
        (const JsonObject x) {
            if(path != [])
                path ~= sep;
            foreach (kv; x.byKeyValue)
            {
                auto v = Json(kv.value);
                normalize(v,path~kv.key, mut, sep);
            }
        },
        (const string y) => normalizeValue(value, path, mut, sep),
        (const long y) => normalizeValue(value, path, mut, sep),
        (const float y) => normalizeValue(value, path, mut, sep),
        
    );
}

void normalizeValue(const Json value, string path, Json mut, string sep){
    
    if(path in (*(mut).asObjectRef)){    
        
        mut[path].value.tryMatch!(
            (JsonArray y) {
                y ~= value.value;
            },
            (string y) {
                auto arr = makeJsonArray(2);
                arr[0] = y;
                arr[1] = value;
                mut[path] = arr;
            },
            (long y) {
                auto arr = makeJsonArray(2);
                arr[0] = y;
                arr[1] = value;
                mut[path] = arr;
            },
            (float y) {
                auto arr = makeJsonArray(2);
                arr[0] = y;
                arr[1] = value;
                mut[path] = arr;
            },
        );
    }else{
        mut[path] = value;
    }
}