module drocks.memory;

import std.traits : isPointer, isSomeFunction, isSafe;
import core.stdc.stdlib : calloc, malloc, free;
import core.lifetime : move;
import core.atomic : atomicOp;

/// can we use @live for scope checking? 
enum dip1000Enabled = isSafe!((int x) => *&x);

static if(dip1000Enabled)
    pragma(msg, "Using -dip1000 for scope checking and safety");

/// Template struct that wraps a
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
    shared int* refct;
    /// initialized?
    bool initialized;

    /// ctor that respects scope
    this(T * rawPtr) @trusted return scope
    {
        this.ptr = rawPtr;
        this.refct = cast(shared int *) calloc(int.sizeof,1);
        (*this.refct) = 1;
        this.initialized = true;
    }
    
    /// postblit that respects scope
    this(this) @trusted return scope
    {
        if(initialized)atomicOp!"+="(*this.refct, 1);
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
        
        if(!this.initialized) return;
        if(atomicOp!"-="(*this.refct, 1)) return;
        if(this.ptr){
            free(cast(int*)this.refct);
            destroyFun(this.ptr);
        }
    }
}

