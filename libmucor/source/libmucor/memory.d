module libmucor.memory;

import std.traits : isPointer, isSomeFunction, ReturnType, isSafe;
import core.stdc.stdlib : calloc, malloc, free;
import core.lifetime : move;
import std.typecons : RefCounted, RefCountedAutoInitialize;
import core.atomic;
import libmucor.error;

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
        this.refct = cast(shared int *) calloc(int.sizeof,1);
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
            free(cast(int*)this.refct);
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