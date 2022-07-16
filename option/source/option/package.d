module option;

import std.traits;
import std.conv : to;

/// Indicate a None Option
enum None = null;

/// Indicate a some option
/// dummy type
struct SomeType(T) {
    private T val;
    alias val this;
}

/// Create Some value
auto Some(T)(T val) {
    return SomeType!T(val);
}

/// An Optional value type
struct Option(T) {
    private T val;
    bool isNone = true;

    /// assign with Some!T
    void opAssign(Option!T val)
    {
        this.val = val.val;
        this.isNone = val.isNone;
    }

    /// assign with Some!T
    void opAssign(SomeType!T val)
    {
        this.val = val;
        this.isNone = false;
    }

    /// assign with None
    void opAssign(typeof(null) _null)
    {
        this.isNone = true;
    }

    /// unwrap to inner val
    auto unwrap() @safe {
        assert(!this.isNone, "Tried to unwrap None option");
        return this.val;
    }

    /// check if equals another result
    bool opEquals(Option!T val) const {
        if(!isNone && !val.isNone) return this.val == val.val;
        else if(isNone && val.isNone) return true;
        else return false;
    }

    /// check if equals Some!T
    bool opEquals(SomeType!T value) const {
        if(isNone) return false;
        else return this.val == value;
    }

    /// check if equals None
    bool opEquals(typeof(null) _null) const
    {
        return isNone ? true : false;
    }

    bool serdeIgnoreOut() const
    {
        return this.isNone;
    }

    void serialize(S)(ref S serializer){
        if(!isNone)
            serializer.putValue(this.val);
    }
}

unittest {
    import std.conv;

    Option!int v;
    v = None;
    assert(v == None);
    assert(v.map!(x => x.to!string) == None);
    v = Some(1);
    assert(v == Some(1));
    assert(v.map!(x => x.to!string) == Some("1"));

}

alias OptionValueType(O: Option!(I), I) = I;

/// check if generic type is an Option
template isOption(T) {
    static if(__traits(compiles, OptionValueType!T))
        enum isOption = true;
    else
        enum isOption = false;
}

static assert(isOption!(Option!string));
static assert(!isOption!(string));



/// Indicate a Ok result
/// dummy type
struct OkType(T) {
    private T val;
    alias val this;
}

/// Create Ok value
auto Ok(T)(T val) {
    return OkType!T(val);
}

/// Error result type
struct ErrType(E) {
    E err;

    string toString() const @safe {
        static if(is(Unqual!E == char*)) {
            import std.string;
            assert(this.err, "Error value is null ptr");
            return fromStringz(err);
        } else static if(isSomeString!E) {
            return err.to!string;
        } else {
            return err.toString;
        }
    }
}

/// Create Err value
auto Err(T)(T err) {
    return ErrType!T(err);
}

struct Result(V, E) {

    private V value;
    private ErrType!E err;
    private bool isErr;
    
    void opAssign(Result!(V, E) val)
    {
        this.value = val.value;
        this.err = val.err;
        this.isErr = val.isErr;
    }

    void opAssign(OkType!V val)
    {
        this.value = val;
        this.isErr = false;
    }

    void opAssign(ErrType!E err)
    {
        this.err = err;
        this.isErr = true;
    }

    auto error() {
        assert(this.isErr, "Not an Err!");
        return this.err;
    }

    auto unwrap() @safe {
        assert(!this.isErr, this.err.toString);
        return this.value;
    }

    /// check if equals another result
    bool opEquals(Result!(V, E) val) const {
        if(isErr && val.isErr) return this.err == err;
        else if(!isErr && !val.isErr) return this.value == val.value;
        else return false;
    }

    /// check if equals Ok!T
    bool opEquals(OkType!V val) const {
        if(isErr) return false;
        else return this.value == val;
    }

    /// check if equals None
    bool opEquals(ErrType!E err) const
    {
        if(isErr) return this.err == err;
        else return false;
    }
}

alias ResultValueType(R: Result!(V, E), V, E) = V;
alias ResultErrorType(R: Result!(V, E), V, E) = E;

template isResult(T) {
    static if(__traits(compiles, ResultValueType!T))
        enum isResult = true;
    else
        enum isResult = false;
}

static assert(isResult!(Result!(string, string)));
static assert(!isResult!(string));

/// map inner value of option or Result
template map(alias fun) {
    auto map(T)(T val) {
        static if(isOption!T) {
            alias rt = ReturnType!(fun!T);
            Option!rt ret;
            if(val.isNone) {
                ret = None;
            } else {
                ret = Some(fun(val.unwrap));
            }
            return ret;
        } else static if(isResult!T) {
            alias rt = ReturnType!(fun!(ResultValueType!T));
            Result!(rt, ResultErrorType!T) ret;
            if(val.isErr) {
                ret = val.error;
            } else {
                ret = OkType!rt(fun(val.unwrap));
            }
            return ret;
        }
    }
    
}

/// map Result error type
template mapErr(alias fun){
    auto mapErr(T)(T val)
    {
        static assert(isResult!T);
        alias rt = ReturnType!(fun!(ResultErrorType!T));
        Result!(ResultValueType!T, rt) ret;
        if(val.isErr) {
            ret = ErrType!rt(fun(val.err.err));
        } else {
            ret = OkType!(ResultValueType!T)(val.unwrap);
        }
        return ret;
    }
}

unittest {

    Result!(int, const(char)[]) v;
    v = Err!(const(char)[])("help");
    assert(v == Err!(const(char)[])("help"));
    assert(v.map!(x => x.to!string) == Err!(const(char)[])("help"));
    assert(v.mapErr!(x => x.to!string) == Err("help"));

    v = Ok(1);
    assert(v == Ok(1));
    assert(v.map!(x => x.to!string) == Ok("1"));
    assert(v.mapErr!(x => x.to!string) == Ok(1));

}