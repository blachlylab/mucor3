module memory;

import std.traits : isPointer, isSomeFunction, ReturnType, isSafe;
import core.memory : pureRealloc, pureCalloc, pureMalloc, pureFree, GC;
import core.stdc.string : memcpy, memset;
import core.lifetime : move;
import std.typecons : RefCounted, RefCountedAutoInitialize;
import core.atomic;
import libmucor.error;

/// allocate bytes
/// if using GC, zeros bytes and registers with GC
T* allocate(T, bool useGC = false)(size_t n) @trusted @nogc nothrow {
    auto p = cast(T*) pureMalloc(n*T.sizeof);
    static if(useGC) {
        memset(p, 0, n*T.sizeof);
        GC.addRange(p, n*T.sizeof);
    }
    return p;
}

/// allocate bytes
/// if using GC, zeros bytes and registers with GC
T* callocate(T, bool useGC = false)(size_t n) @trusted @nogc nothrow {
    auto p = cast(T*) pureMalloc(n*T.sizeof);
    memset(p, 0, n*T.sizeof);
    static if(useGC) {
        GC.addRange(p, n*T.sizeof);
    }
    return p;
}

/// reallocate bytes, really just allocate, copy, then free
T* reallocate(T, bool useGC = false)(T * p, size_t len, size_t newLen) @trusted @nogc nothrow 
{
    // assert(len < newLen);
    auto np = allocate!(T, useGC)(newLen);
    assert(p != np);
    np[0 .. len] = p[0 .. len];
    free!(T, useGC)(p);
    return np;
}

/// free bytes
/// if using GC, unregisters with GC
void free(T, bool useGC = false)(T * p) @trusted @nogc nothrow {
    static if(useGC) GC.removeRange(p);
    pureFree(cast(void *)p);
}


/// can we use @live for scope checking? 
enum dip1000Enabled = isSafe!((int x) => *&x);

static if(dip1000Enabled)
    pragma(msg, "Using -dip1000 for scope checking and safety");

/// Template struct that wraps an htslib
/// pointer and reference counts it and then
/// destroys with destroyFun when it goes 
/// truly out of scope
struct SafePtr(T, alias destroyFun)
if(!isPointer!T && isSomeFunction!destroyFun)
{
    @safe @nogc nothrow:

    /// data pointer
    T * ptr;
    /// reference counting
    shared int * refct;

    /// ctor that respects scope
    this(T * rawPtr) @trusted return scope
    {
        this.ptr = rawPtr;
        this.refct = cast(shared int *) pureCalloc(int.sizeof, 1);
        (*this.refct) = 1;
    }
    
    /// postblit that respects scope
    this(this) @trusted return scope
    {
        if(this.refct) atomicOp!"+="(*this.refct, 1);
    }

    /// allow SafeHtslibPtr to be used as 
    /// underlying ptr type
    alias getRef this;

    /// get underlying data pointer
    @property nothrow pure @nogc
    ref inout(T*) getRef() inout return
    {
        return ptr;
    }

    /// take ownership of underlying data pointer
    @property nothrow pure @nogc
    T* moveRef()
    {
        T * ptr;
        move(this.getRef, ptr);
        return ptr;
    }

    /// dtor that respects scope
    ~this() @trusted return scope
    {
        if(!this.refct) return;
        if(atomicOp!"-="(*this.refct, 1)) return;
        if(this.ptr){
            pureFree(cast(int*)this.refct);
            /// if destroy function return is void 
            /// just destroy
            /// else if return is int
            /// destroy then check return value 
            /// else don't compile
            static if(is(ReturnType!destroyFun == void))
                destroyFun(this.ptr);
            else static if(is(ReturnType!destroyFun == int))
            {
                auto err = destroyFun(this.ptr);
                if(err != 0) 
                    log_err_no_exit(__FUNCTION__, "Couldn't destroy/close "~T.stringof~" * data using function "~__traits(identifier, destroyFun));
            }else{
                static assert(0, "HtslibPtr doesn't recognize destroy function return type");
            }
        }
    }
}

struct ConstBuffer(T) {

}

struct Buffer(T, bool useGC = false) {
    T * ptr;
    private size_t len;
    private size_t capacity;

    @safe @nogc nothrow @live:

    this(T * ptr, size_t len) @trusted
    {
        this.ptr = ptr;
        this.len = this.capacity = len;
    }

    this(L)(L[] data) @trusted
    {
        this.reserveExactly(data.length);
        this.len = data.length;
        memcpy(this.ptr, data.ptr, data.length * T.sizeof);
    }

    void deallocate() {
        free!T(this.ptr);
        this.ptr = null;
        this.len = this.capacity = 0;
    }

    @property length() const {
        return this.len;
    }

    @property length(size_t size) {
        this.reserve(size);
        this.len = size;
    }

    @property dup() @trusted return scope {
        return Buffer!(T, useGC)(this.ptr[0 .. this.len]);
    }

    void reserve(size_t size) @trusted {
        import htslib.kroundup;
        kroundup_size_t(size);
        this.reserveExactly(size);
    }

    void reserveExactly(size_t size) @trusted {
        
        /// if big enough,
        /// skip
        if(size <= this.capacity)
            return;
        
        /// if capacity 0,
        /// allocate and return
        if(this.capacity == 0) {
            this.ptr = allocate!(T, useGC)(size);
            this.capacity = size;
            return;
        }

        /// else reallocate
        this.ptr = reallocate!(T, useGC)(this.ptr, this.capacity, size);
        this.capacity = size;
    }

    void opOpAssign(string op : "~")(T value) @trusted
    {
        reserve(this.len + 1);
        this.ptr[this.len++] = value;
    }

    void opOpAssign(string op : "~")(T[] value) @trusted
    {
        reserve(this.len + value.length);
        
        this.ptr[this.len .. this.len + value.length] = value[0..$];
        this.len += value.length;
    }

    ref auto opIndex(size_t index) @trusted return scope
    {
        return this.ptr[index];
    }

    ref auto opIndex(size_t index) @trusted return scope const
    {
        return this.ptr[index];
    }

    ref auto opSlice() @trusted return scope
    {
        return this.ptr[0 .. this.len];
    }

    ref auto opSlice() @trusted return scope const
    {
        return this.ptr[0 .. this.len];
    }

    ref auto opSlice(size_t start, size_t end) @trusted return scope
    {
        return this.ptr[start .. end];
    }

    void opSliceAssign(T value) @trusted
    {
        this.ptr[0 .. this.len] = value;
    }

    auto opSliceAssign(T value, size_t start, size_t end) @trusted
    {
        this.ptr[start .. end] = value;
    }

    auto opSliceAssign(T[] value, size_t start, size_t end) @trusted
    {
        this.ptr[start .. end] = value[];
    }

    bool opEquals(const Buffer!T other) const
    {
        return this[] == other[];
    }

    size_t opDollar()
    {
        return len;
    }

    void shrink() {
        if(this.capacity == 0) this.deallocate;
        else {
            this.ptr = reallocate!(T, useGC)(this.ptr, this.len, this.len);
            this.capacity = this.len;
        }
    }
}

unittest {
    Buffer!(char, true) arr;
    arr ~= cast(char[])"char";
    arr ~= cast(char[])"char";
    arr ~= cast(char[])"char";

    assert(arr[] == "charcharchar");

    Buffer!(char, true) a = arr;
    assert(a[] == "charcharchar");
    assert(a.length == 12);
    a.length = 13;
    a[12] = 'd';
    assert(a[] == "charcharchard");
    a[0..5] = cast(char[])"hello";
    assert(a[] == "helloharchard");
}