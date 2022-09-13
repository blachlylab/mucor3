module memory;

import std.traits : isPointer, isSomeFunction, ReturnType, isSafe;
import core.memory : pureRealloc, pureCalloc, pureMalloc, pureFree, GC;
import core.stdc.string : memcpy, memset;
import core.lifetime : move;
import std.typecons : RefCounted, RefCountedAutoInitialize;
import core.atomic;
import libmucor.error;

/// allocates and zeroes bytes
/// if using GC, registers with GC
T* allocate(T, bool useGC = false)(size_t n) @trusted @nogc nothrow {
    auto p = cast(T*) pureCalloc(n, T.sizeof);
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

struct Buffer(T, bool useGC = false) {
    T * ptr;
    private size_t len;
    private size_t capacity;

    @safe @nogc nothrow @live:
    
    /// create from malloc'd pointer
    /// doesn't allocate
    this(T * ptr, size_t len) @trusted
    {
        this.ptr = ptr;
        this.len = this.capacity = len;
    }

    /// create from slice
    /// assumes GC'd slice
    /// allocates exactly to length
    this(L)(L[] data) @trusted
    {
        this.reserveExactly(data.length);
        this.len = data.length;
        memcpy(this.ptr, data.ptr, data.length * T.sizeof);
    }

    /// deallocates data in this buffer
    /// resets buffer for further use
    void deallocate() {
        free!T(this.ptr);
        this.ptr = null;
        this.len = this.capacity = 0;
    }

    /// get length of buffer
    @property length() const {
        return this.len;
    }

    /// sets length of buffer
    /// allocates if bigger than capacity
    @property length(size_t size) {
        this.reserve(size);
        this.len = size;
    }

    /// duplicates buffer
    /// this is nogc
    @property dup() @trusted return scope {
        return Buffer!(T, useGC)(this.ptr[0 .. this.len]);
    }

    /// reserve at least size number of elements of capacity
    void reserve(size_t size) @trusted {
        import htslib.kroundup;
        kroundup_size_t(size);
        this.reserveExactly(size);
    }

    /// reserve exactly size number of elements of capacity
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

    /// append element to buffer
    void opOpAssign(string op : "~")(T value) @trusted
    {
        reserve(this.len + 1);
        this.ptr[this.len++] = value;
    }

    /// append elements to buffer
    void opOpAssign(string op : "~")(T[] value) @trusted
    {
        reserve(this.len + value.length);
        
        this.ptr[this.len .. this.len + value.length] = value[0..$];
        this.len += value.length;
    }

    /// get element
    ref auto opIndex(size_t index) @trusted return scope
    {
        return this.ptr[index];
    }

    /// ditto
    ref auto opIndex(size_t index) @trusted return scope const
    {
        return this.ptr[index];
    }

    /// get slice of all elements
    ref auto opSlice() @trusted return scope
    {
        return this.ptr[0 .. this.len];
    }

    /// ditto
    ref auto opSlice() @trusted return scope const
    {
        return this.ptr[0 .. this.len];
    }

    /// get slice of elements
    ref auto opSlice(size_t start, size_t end) @trusted return scope
    {
        return this.ptr[start .. end];
    }

    /// set slice of all elements to a value
    void opSliceAssign(T value) @trusted
    {
        this.ptr[0 .. this.len] = value;
    }

    /// set slice of elements to a value
    auto opSliceAssign(T value, size_t start, size_t end) @trusted
    {
        this.ptr[start .. end] = value;
    }

    /// set slice of elements to a slice of values
    auto opSliceAssign(T[] value, size_t start, size_t end) @trusted
    {
        this.ptr[start .. end] = value[];
    }

    /// s
    bool opEquals(const Buffer!T other) const
    {
        return this[] == other[];
    }

    /// 
    size_t opDollar()
    {
        return len;
    }

    /// shrink buffer to length
    void shrink() {
        if(this.capacity == 0) this.deallocate;
        else {
            this.ptr = reallocate!(T, useGC)(this.ptr, this.len, this.len);
            this.capacity = this.len;
        }
    }

    int opApply(scope int delegate(ref T) @nogc nothrow @safe dg) @nogc nothrow @trusted
    {
        int result = 0;
    
        for (ulong i;i < len; i++)
        {
            result = dg(this.ptr[i]);
            if (result)
                break;
        }
    
        return result;
    }

    int opApply(scope int delegate(ulong, ref T) @nogc nothrow @safe dg) @nogc nothrow @trusted
    {
        int result = 0;
    
        for (ulong i;i < len; i++)
        {
            result = dg(i, this.ptr[i]);
            if (result)
                break;
        }
    
        return result;
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