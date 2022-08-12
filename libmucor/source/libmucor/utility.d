module libmucor.utility;
import core.stdc.string : strstr, memchr;
import core.stdc.stdio : fprintf, stderr;
import core.stdc.stdlib : exit;
import core.memory : malloc = pureMalloc;
import std.container.array;
import mir.appender: scopedBuffer, ScopedBuffer;
import mir.format;


auto findSplit(const(char)[] val, char splitChar) @nogc nothrow @trusted {
    const(char)[][2] ret;

    auto p = cast(char*)memchr(val.ptr, splitChar, val.length);
    if(!p) return ret;

    auto len = ((cast(ulong)p) - (cast(ulong)val.ptr));
    ret[0] = val[0 .. len];
    ret[1] = cast(const(char)[])p[1 .. val.length - len];

    return ret;
}

auto findSplit(const(char)[] val, const(char)[] splitStr) @nogc nothrow @trusted {
    const(char)[][2] ret;

    auto p = strstr(val.ptr, splitStr.ptr);
    if(!p) return ret;

    auto len = ((cast(ulong)p) - (cast(ulong)val.ptr));
    ret[0] = val[0 .. len];
    ret[1] = cast(const(char)[])p[splitStr.length .. val.length - len];

    return ret;
}

auto format(Args...)(const(char)[] fmt, Args args) @nogc nothrow @trusted {
    auto ret = scopedBuffer!char;
    const(char)[][2] s;
    static foreach(i, arg; args) {
        s = fmt.findSplit('%');
        if(s == ["",""]) {
            debug assert(0, "Unmatched format specifier, too few values to format!");
            else {
                stderr.fprintf("Unmatched format specifier, too few values to format!");
                exit(1);
            }
        } else if(s[1].length == 0){
            debug assert(0, "Format specifier \'%\' at end of string!");
            else {
                stderr.fprintf("Format specifier \'%%\' at end of string!");
                exit(1);
            }
        }
        else if(s[1][0] == 's')
            ret.printVal(s[0]).printVal(arg);
        else if(s[1][0] == 'd' || s[1][0] == 'f') 
            ret.printVal(s[0]).printVal(arg);
        else if(s[1][0] == '%') {}
        else {
            auto err = scopedBuffer!char;
            err.printVal("Unrecognized format specifier").printVal(s[1][0]).printVal("!");
            debug assert(0, err.data);
            else {
                stderr.fprintf(err.data.ptr);
                exit(1);
            }
        }
        fmt = s[1][1..$];
    }
    ret.printVal(fmt);
    auto str = (cast(char*)malloc(ret.data.length + 1))[0..ret.data.length];
    str.ptr[ret.data.length] = '\0';
    str[] = ret.data[];
    return cast(const(char)[])str;
}


ref W printVal(C = char, W, T)(return ref scope W w, const T val) @nogc nothrow @trusted {
    import std.traits : isSomeString;
    static if(isSomeString!T) {
        foreach(c;val) {
            w.put(c);
        }
        return w;
    } else return .print!C(w, val);
    
}

void ewrite(const(char)[] input) @nogc nothrow @trusted {
    stderr.fprintf(cast(char*)input.ptr);
}

unittest {
    auto x = format("hello %s, %f, %d", "lol", 2.0, 3);
    ewrite(x);
}