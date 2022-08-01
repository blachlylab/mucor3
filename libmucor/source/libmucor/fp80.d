module libmucor.fp80;

import std.traits;
import std.bitmanip;
import core.bitop;
import core.stdc.stdlib;
import std.math : isNaN, isInfinity;
/**
 * 80-bit floating point value software implementation
 * This allows us to store and sort integers and floating point numbers the same.
 * Specifically because 80-bit "real"s have perfect precision for -2^64 < x < 2^64 
 * This implementation was adapted from the longdouble implmentation used by the DMD 
 * compiler. It is distrubuted under the BSL 1.0 License.
 * 
 * The text below is from that implementation:
 * 
 * 80-bit floating point value implementation if the C/D compiler does not support them natively.
 *
 * Copyright (C) 1999-2022 by The D Language Foundation, All Rights Reserved
 * All Rights Reserved, written by Rainer Schuetze
 * https://www.digitalmars.com
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying file LICENSE or copy at https://www.boost.org/LICENSE_1_0.txt)
 * https://github.com/dlang/dmd/blob/master/src/root/longdouble.d
 */

struct FP80
{
    nothrow @nogc pure @trusted {
        // DMD's x87 `real` on Windows is packed (alignof = 2 -> sizeof = 10).
        align(2) ulong mantissa = 0xC000000000000000UL; // default to qnan
        ushort exp_sign = 0x7fff; // sign is highest bit

        this(ulong m, ushort es) { mantissa = m; exp_sign = es; }
        this(FP80 ld) { mantissa = ld.mantissa; exp_sign = ld.exp_sign; }
        this(int i) {
            this(cast(long)i);
        }
        this(uint i) {
            this(cast(ulong)i);
        }
        this(long i) {
            this.exp_sign = 0;
            auto u = cast(ulong)i;
            if(i < 0) {
                this.exp_sign |= 0x8000;
                u = ~u + 1;
            }
            auto hb = bsr(u);
            exp_sign |= 0x3fff + hb;
            mantissa = u << (63 - hb);
        }

        this(ulong i) {
            this.exp_sign = 0;
            auto hb = bsr(i);
            exp_sign |= 0x3fff + hb;
            mantissa = i << (63 - hb);
        }

        this(float f) {
            FloatRep x;
            x.value = f;
            exp_sign |= cast(ushort)x.sign << 15;
            if(isNaN(f))
                exp_sign |= 0x7FFF;
            else if(x.exponent == 0)
                exp_sign |= 0;
            else
                exp_sign |= (x.exponent - 127) + 0x3fff;
            mantissa = (cast(ulong)x.fraction) << (64 - float.mant_dig);
        }

        this(double f)
        {
            this.exp_sign = 0;
            DoubleRep x;
            x.value = f;
            exp_sign |= cast(ushort)x.sign << 15;
            if(isNaN(f))
                exp_sign |= 0x7FFF;
            else if(x.exponent == 0)
                exp_sign |= 0;
            else
                exp_sign |= cast(ushort)((x.exponent ) + (0x3fff - 1023));
            mantissa = (1L << 63) | (x.fraction << 11);
        }
        
        this(real r)
        {
            static if (real.sizeof > 8)
                *cast(real*)&this = r;
            else
                this(cast(double)r);
        }

        ushort exponent() const { return exp_sign & 0x7fff; }
        bool sign() const { return (exp_sign & 0x8000) != 0; }

        ref FP80 opAssign(FP80 ld) return { mantissa = ld.mantissa; exp_sign = ld.exp_sign; return this; }
        ref FP80 opAssign(T)(T rhs) { this = FP80(rhs); return this; }

        FP80 opUnary(string op)() const
        {
            static if (op == "-") return FP80(mantissa, exp_sign ^ 0x8000);
            else static assert(false, "Operator `"~op~"` is not implemented");
        }

        T opCast(T)() const @trusted
        {
            static      if (is(T == bool))   return mantissa != 0 || (exp_sign & 0x7fff) != 0;
            else static if (is(T == byte))   return cast(T)ld_read(&this);
            else static if (is(T == ubyte))  return cast(T)ld_read(&this);
            else static if (is(T == short))  return cast(T)ld_read(&this);
            else static if (is(T == ushort)) return cast(T)ld_read(&this);
            else static if (is(T == int))    return cast(T)ld_read(&this);
            else static if (is(T == uint))   return cast(T)ld_read(&this);
            else static if (is(T == float))  return cast(T)ld_read(&this);
            else static if (is(T == double)) return cast(T)ld_read(&this);
            else static if (is(T == long))   return ld_readll(&this);
            else static if (is(T == ulong))  return ld_readull(&this);
            else static if (is(T == real))
            {
                // convert to front end real if built with dmd
                if (real.sizeof > 8)
                    return *cast(real*)&this;
                else
                    return ld_read(&this);
            }
            else static assert(false, "usupported type");
        }

        // a qnan
        static FP80 nan() { return FP80(0xC000000000000000UL, 0x7fff); }
        static FP80 infinity() { return FP80(0x8000000000000000UL, 0x7fff); }
        static FP80 zero() { return FP80(0, 0); }
        static FP80 max() { return FP80(0xffffffffffffffffUL, 0x7ffe); }
        static FP80 min_normal() { return FP80(0x8000000000000000UL, 1); }
        static FP80 epsilon() { return FP80(0x8000000000000000UL, 0x3fff - 63); }

        static uint dig() { return 18; }
        static uint mant_dig() { return 64; }
        static uint max_exp() { return 16_384; }
        static uint min_exp() { return -16_381; }
        static uint max_10_exp() { return 4932; }
        static uint min_10_exp() { return -4932; }

    }

    auto toString() const {
        char[32] buffer;
        auto len = ld_sprint(buffer.ptr, 'g', this);
        return buffer[0 .. len].idup;
    }
}

nothrow @nogc pure @trusted: // LDC: LLVM __asm is @system AND requires taking the address of variables

/// convert FP80 to big endian array
/// note: sign is inverted, this is needed for rocksdb
auto nativeToBigEndian(FP80 x) {
    x.exp_sign ^= 0x8000; 
    ubyte[10] ret = (cast(ubyte*)&x)[0 .. 10];
    version(LittleEndian)
        swapEndian(ret);
    return ret;
}

/// convert FP80 to little endian array
/// note: sign is inverted, this is needed for rocksdb
auto nativeToLittleEndian(FP80 x) {
    x.exp_sign ^= 0x8000;
    ubyte[10] ret = (cast(ubyte*)&x)[0 .. 10];
    version(BigEndian)
        swapEndian(ret);
    return ret;
}

/// convert big endian array to FP80
auto bigEndianToNative(ubyte[10] x){
    version(LittleEndian)
        swapEndian(x);
    FP80 ret = *(cast(FP80*)x.ptr);
    ret.exp_sign ^= 0x8000;
    return ret;
}

/// convert little endian array to FP80
auto littleEndianToNative(ubyte[10] x){
    version(BigEndian)
        swapEndian(x);
    FP80 ret = *(cast(FP80*)x.ptr);
    ret.exp_sign ^= 0x8000;
    return ret;
}

/// swap endianess of 10 bytes (size of FP80)
void swapEndian(ref ubyte[10] x) {
    import core.bitop : bswap, byteswap;
    ubyte[4] tmp = x[0..4];
    x[0..4] = x[6..10];
    x[6..10] = tmp;

    auto ip = (cast(uint*)x.ptr);
    *ip = bswap(*ip);
    ip = (cast(uint*)(x.ptr + 6));
    *ip = bswap(*ip);
    auto sp = (cast(ushort*)(x.ptr + 4));
    *sp = byteswap(*sp);
}

static assert(FP80.alignof == 2);
static assert(FP80.sizeof == 10);

double ld_read(const FP80* pthis)
{
    DoubleRep x;
    x.sign = pthis.sign;
    if(_isnan(*pthis))
        x.exponent = 0x7FF;
    else if(pthis.exponent == 0)
        x.exponent = 0;
    else
        x.exponent = cast(ushort)((pthis.exp_sign & 0x7FFF) - (0x3fff - 1023));
    x.fraction = (pthis.mantissa & 0x7fffffffffffffff) >> 11;
    return x.value;
}

long ld_readll(const FP80* pthis)
{
    return ld_readull(pthis);
}

ulong ld_readull(const FP80* pthis)
{
    // somehow the FPU does not respect the CHOP mode of the rounding control
    // in 64-bit mode
    // so we roll our own conversion (it also allows the usual C wrap-around
    // instead of the "invalid value" created by the FPU)
    int expo = pthis.exponent - 0x3fff;
    ulong u;
    if(expo < 0 || expo > 127)
        return 0;
    if(expo < 64)
        u = pthis.mantissa >> (63 - expo);
    else
        u = pthis.mantissa << (expo - 63);
    if(pthis.sign)
        u = ~u + 1;
    return u;
}

int _isnan(FP80 ld)
{
    return (ld.exponent == 0x7fff && ld.mantissa != 0 && ld.mantissa != (1L << 63)); // exclude pseudo-infinity and infinity, but not FP Indefinite
}


//////////////////////////////////////////////////////////////

@safe:

__gshared const
{
    FP80 ld_qnan = FP80(0xC000000000000000UL, 0x7fff);
    FP80 ld_inf  = FP80(0x8000000000000000UL, 0x7fff);

    FP80 ld_zero  = FP80(0, 0);
    FP80 ld_one   = FP80(0x8000000000000000UL, 0x3fff);
    FP80 ld_pi    = FP80(0xc90fdaa22168c235UL, 0x4000);
    FP80 ld_log2t = FP80(0xd49a784bcd1b8afeUL, 0x4000);
    FP80 ld_log2e = FP80(0xb8aa3b295c17f0bcUL, 0x3fff);
    FP80 ld_log2  = FP80(0x9a209a84fbcff799UL, 0x3ffd);
    FP80 ld_ln2   = FP80(0xb17217f7d1cf79acUL, 0x3ffe);

    FP80 ld_pi2     = FP80(0xc90fdaa22168c235UL, 0x4001);
    FP80 ld_piOver2 = FP80(0xc90fdaa22168c235UL, 0x3fff);
    FP80 ld_piOver4 = FP80(0xc90fdaa22168c235UL, 0x3ffe);

    FP80 twoPow63 = FP80(1UL << 63, 0x3fff + 63);
}

//////////////////////////////////////////////////////////////

enum LD_TYPE_OTHER    = 0;
enum LD_TYPE_ZERO     = 1;
enum LD_TYPE_INFINITE = 2;
enum LD_TYPE_SNAN     = 3;
enum LD_TYPE_QNAN     = 4;

int ld_type(FP80 x)
{
    // see https://en.wikipedia.org/wiki/Extended_precision
    if(x.exponent == 0)
        return x.mantissa == 0 ? LD_TYPE_ZERO : LD_TYPE_OTHER; // dnormal if not zero
    if(x.exponent != 0x7fff)
        return LD_TYPE_OTHER;    // normal or denormal
    uint  upper2  = x.mantissa >> 62;
    ulong lower62 = x.mantissa & ((1L << 62) - 1);
    if(upper2 == 0 && lower62 == 0)
        return LD_TYPE_INFINITE; // pseudo-infinity
    if(upper2 == 2 && lower62 == 0)
        return LD_TYPE_INFINITE; // infinity
    if(upper2 == 2 && lower62 != 0)
        return LD_TYPE_SNAN;
    return LD_TYPE_QNAN;         // qnan, indefinite, pseudo-nan
}

// consider sprintf pure
private extern(C) int sprintf(scope char* s, scope const char* format, ...) pure @nogc nothrow;

size_t ld_sprint(char* str, int fmt, FP80 x) @system
{
    // ensure dmc compatible strings for nan and inf
    switch(ld_type(x))
    {
        case LD_TYPE_QNAN:
        case LD_TYPE_SNAN:
            return sprintf(str, "nan");
        case LD_TYPE_INFINITE:
            return sprintf(str, x.sign ? "-inf" : "inf");
        default:
            break;
    }

    // fmt is 'a','A','f' or 'g'
    if(fmt != 'a' && fmt != 'A')
    {
        char[3] format = ['%', cast(char)fmt, 0];
        return sprintf(str, format.ptr, ld_read(&x));
    }

    ushort exp = x.exponent;
    ulong mantissa = x.mantissa;

    if(ld_type(x) == LD_TYPE_ZERO)
        return sprintf(str, fmt == 'a' ? "0x0.0L" : "0X0.0L");

    size_t len = 0;
    if(x.sign)
        str[len++] = '-';
    str[len++] = '0';
    str[len++] = cast(char)('X' + fmt - 'A');
    str[len++] = mantissa & (1L << 63) ? '1' : '0';
    str[len++] = '.';
    mantissa = mantissa << 1;
    while(mantissa)
    {
        int dig = (mantissa >> 60) & 0xf;
        dig += dig < 10 ? '0' : fmt - 10;
        str[len++] = cast(char)dig;
        mantissa = mantissa << 4;
    }
    str[len++] = cast(char)('P' + fmt - 'A');
    if(exp < 0x3fff)
    {
        str[len++] = '-';
        exp = cast(ushort)(0x3fff - exp);
    }
    else
    {
        str[len++] = '+';
        exp = cast(ushort)(exp - 0x3fff);
    }
    size_t exppos = len;
    for(int i = 12; i >= 0; i -= 4)
    {
        int dig = (exp >> i) & 0xf;
        if(dig != 0 || len > exppos || i == 0)
            str[len++] = cast(char)(dig + (dig < 10 ? '0' : fmt - 10));
    }
    str[len] = 0;
    return len;
}

//////////////////////////////////////////////////////////////

@system unittest
{
    import core.stdc.string;
    import core.stdc.stdio;

    char[32] buffer;
    ld_sprint(buffer.ptr, 'a', ld_pi);
    assert(strcmp(buffer.ptr, "0x1.921fb54442d1846ap+1") == 0);

    auto len = ld_sprint(buffer.ptr, 'g', FP80(2.0));
    assert(buffer[0 .. len] == "2.00000" || buffer[0 .. len] == "2"); // Win10 - 64bit

    ld_sprint(buffer.ptr, 'g', FP80(1_234_567.89));
    assert(strcmp(buffer.ptr, "1.23457e+06") == 0);

    ld_sprint(buffer.ptr, 'g', ld_inf);
    assert(strcmp(buffer.ptr, "inf") == 0);

    ld_sprint(buffer.ptr, 'g', ld_qnan);
    assert(strcmp(buffer.ptr, "nan") == 0);

    FP80 ldb = FP80(0.4);
    long b = cast(long)ldb;
    assert(b == 0);
    assert(cast(double)ldb == 0.4);

    b = cast(long)FP80(0.9);
    assert(b == 0);
    assert(cast(double)FP80(0.9) == 0.9);

    long x = 0x12345678abcdef78L;
    FP80 ldx = FP80(x);
    // assert(ldx > ld_zero);
    long y = cast(long)ldx;
    assert(x == y);

    x = -0x12345678abcdef78L;
    ldx = FP80(x);
    // assert(ldx < ld_zero);
    y = cast(long)ldx;
    assert(x == y);

    ulong u = 0x12345678abcdef78L;
    FP80 ldu = FP80(u);
    // assert(ldu > ld_zero);
    ulong v = cast(ulong)ldu;
    assert(u == v);

    u = 0xf234567812345678UL;
    ldu = FP80(u);
    // assert(ldu > ld_zero);
    v = cast(ulong)ldu;
    assert(u == v);
}

unittest {
    ubyte[10] arr = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    swapEndian(arr);
    assert(arr == [9, 8, 7, 6, 5, 4, 3, 2, 1, 0]);
}
