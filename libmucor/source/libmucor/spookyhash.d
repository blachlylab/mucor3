module libmucor.spookyhash;
// Spooky Hash
// A 128-bit noncryptographic hash, for checksums and table lookup
// By Bob Jenkins.  Public domain.
//   Oct 31 2010: published framework, disclaimer ShortHash isn't right
//   Nov 7 2010: disabled ShortHash
//   Oct 31 2011: replace End, ShortMix, ShortEnd, enable ShortHash again
//   April 10 2012: buffer overflow on platforms without unaligned reads
//   July 12 2012: was passing out variables in final to in/out in short
//   July 30 2012: I reintroduced the buffer overflow
//   August 5 2012: SpookyV2: d = should be d += in short hash, and remove extra mix from long hash

// #include <memory.h>
// #include "SpookyV2.h"

// #define ALLOW_UNALIGNED_READS 1

//
// SpookyHash: a 128-bit noncryptographic hash function
// By Bob Jenkins, public domain
//   Oct 31 2010: alpha, framework + SpookyHash::Mix appears right
//   Oct 31 2011: alpha again, Mix only good to 2^^69 but rest appears right
//   Dec 31 2011: beta, improved Mix, tested it for 2-bit deltas
//   Feb  2 2012: production, same bits as beta
//   Feb  5 2012: adjusted definitions of uint* to be more portable
//   Mar 30 2012: 3 bytes/cycle, not 4.  Alpha was 4 but wasn't thorough enough.
//   August 5 2012: SpookyV2 (different results)
// 
// Up to 3 bytes/cycle for long messages.  Reasonably fast for short messages.
// All 1 or 2 bit deltas achieve avalanche within 1% bias per output bit.
//
// This was developed for and tested on 64-bit x86-compatible processors.
// It assumes the processor is little-endian.  There is a macro
// controlling whether unaligned reads are allowed (by default they are).
// This should be an equally good hash on big-endian machines, but it will
// compute different results on them than on little-endian machines.
//
// Google's CityHash has similar specs to SpookyHash, and CityHash is faster
// on new Intel boxes.  MD4 and MD5 also have similar specs, but they are orders
// of magnitude slower.  CRCs are two or more times slower, but unlike 
// SpookyHash, they have nice math for combining the CRCs of pieces to form 
// the CRCs of wholes.  There are also cryptographic hashes, but those are even 
// slower than MD5.
//

// #include <stddef.h>

// #ifdef _MSC_VER
// # define pragma(inline, true) __forcepragma(inline, true)
//   typedef  unsigned __int64 ulong;
//   typedef  unsigned __int32 uint;
//   typedef  unsigned __int16 uint16;
//   typedef  unsigned __int8  ubyte;
// #else
// # include <stdint.h>
// # define pragma(inline, true) pragma(inline, true)
//   typedef  ulong_t  ulong;
//   typedef  uint_t  uint;
//   typedef  uint16_t  uint16;
//   typedef  ubyte_t   ubyte;
// #endif
import core.stdc.stdlib : malloc, free;
import core.stdc.string : memcpy, memset;
import libmucor.hts_endian : HTS_ALLOW_UNALIGNED;
import std.stdio;

struct SpookyHash
{
public:
    //
    // SpookyHash: hash a single message in one call, produce 128-bit output
    //
    // do the whole hash in one call
    static void Hash128(const(void)* message, // message to hash
            size_t length, // length of message in bytes
            ulong* hash1, // in/out: in seed 1, out hash value 1
            ulong* hash2) // in/out: in seed 2, out hash value 2
            {
        if (length < sc_bufSize)
        {
            Short(message, length, hash1, hash2);
            return;
        }

        ulong h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11;
        ulong[sc_numVars] buf;
        ulong* end;
        union U1
        {
            const(ubyte)* p8;
            ulong* p64;
            size_t i;
        }

        size_t remainder;

        h0 = h3 = h6 = h9 = *hash1;
        h1 = h4 = h7 = h10 = *hash2;
        h2 = h5 = h8 = h11 = sc_const;
        U1 u;
        u.p8 = cast(const(ubyte)*) message;
        end = u.p64 + (length / sc_blockSize) * sc_numVars;

        // handle all whole sc_blockSize blocks of bytes
        if (HTS_ALLOW_UNALIGNED || ((u.i & 0x7) == 0))
        {
            while (u.p64 < end)
            {
                Mix(u.p64, h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
                u.p64 += sc_numVars;
            }
        }
        else
        {
            while (u.p64 < end)
            {
                memcpy(buf.ptr, u.p64, sc_blockSize);
                Mix(buf.ptr, h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
                u.p64 += sc_numVars;
            }
        }

        // handle the last partial block of sc_blockSize bytes
        remainder = (length - (cast(const(ubyte)*) end - cast(const(ubyte)*) message));
        memcpy(buf.ptr, end, remainder);
        memset((cast(ubyte*) buf) + remainder, 0, sc_blockSize - remainder);
        (cast(ubyte*) buf.ptr)[sc_blockSize - 1] = *(cast(ubyte*)&remainder);

        // do some final mixing 
        End(buf.ptr, h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
        *hash1 = h0;
        *hash2 = h1;
    }
    //
    // Hash64: hash a single message in one call, return 64-bit output
    //
    static ulong Hash64(const(void)* message, // message to hash
            size_t length, // length of message in bytes
            ulong seed) // seed
            {
        ulong hash1 = seed;
        Hash128(message, length, &hash1, &seed);
        return hash1;
    }

    //
    // Hash32: hash a single message in one call, produce 32-bit output
    //
    static uint Hash32(const(void)* message, // message to hash
            size_t length, // length of message in bytes
            uint seed) // seed
            {
        ulong hash1 = seed, hash2 = seed;
        Hash128(message, length, &hash1, &hash2);
        return cast(uint) hash1;
    }

    //
    // Init: initialize the context of a SpookyHash
    //
    void Init(ulong seed1, // any 64-bit value will do, including 0
            ulong seed2) // different seeds produce independent hashes
            {
        m_length = 0;
        m_remainder = 0;
        m_state[0] = seed1;
        m_state[1] = seed2;
    }
    //
    // Update: add a piece of a message to a SpookyHash state
    //
    void Update(const(void)* message, // message fragment
            size_t length) // length of message fragment in bytes
            {
        ulong h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11;
        size_t newLength = length + m_remainder;
        ubyte remainder;
        union U2
        {
            const(ubyte)* p8;
            ulong* p64;
            size_t i;
        }

        const(ulong)* end;
        U2 u;
        // Is this message fragment too short?  If it is, stuff it away.
        if (newLength < sc_bufSize)
        {
            memcpy(&(cast(ubyte*) m_data)[m_remainder], message, length);
            m_length = length + m_length;
            m_remainder = cast(ubyte) newLength;
            return;
        }

        // init the variables
        if (m_length < sc_bufSize)
        {
            h0 = h3 = h6 = h9 = m_state[0];
            h1 = h4 = h7 = h10 = m_state[1];
            h2 = h5 = h8 = h11 = sc_const;
        }
        else
        {
            h0 = m_state[0];
            h1 = m_state[1];
            h2 = m_state[2];
            h3 = m_state[3];
            h4 = m_state[4];
            h5 = m_state[5];
            h6 = m_state[6];
            h7 = m_state[7];
            h8 = m_state[8];
            h9 = m_state[9];
            h10 = m_state[10];
            h11 = m_state[11];
        }
        m_length = length + m_length;

        // if we've got anything stuffed away, use it now
        if (m_remainder)
        {
            ubyte prefix = cast(ubyte)(sc_bufSize - m_remainder);
            memcpy(&((cast(ubyte*) m_data)[m_remainder]), message, prefix);
            u.p64 = m_data.ptr;
            Mix(u.p64, h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
            Mix(&u.p64[sc_numVars], h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
            u.p8 = (cast(const(ubyte)*) message) + prefix;
            length -= prefix;
        }
        else
        {
            u.p8 = cast(const(ubyte)*) message;
        }

        // handle all whole blocks of sc_blockSize bytes
        end = u.p64 + (length / sc_blockSize) * sc_numVars;
        remainder = cast(ubyte)(length - (cast(const(ubyte)*) end - u.p8));
        if (HTS_ALLOW_UNALIGNED || (u.i & 0x7) == 0)
        {
            while (u.p64 < end)
            {
                Mix(u.p64, h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
                u.p64 += sc_numVars;
            }
        }
        else
        {
            while (u.p64 < end)
            {
                memcpy(m_data.ptr, u.p8, sc_blockSize);
                Mix(m_data.ptr, h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
                u.p64 += sc_numVars;
            }
        }

        // stuff away the last few bytes
        m_remainder = remainder;
        memcpy(m_data.ptr, end, remainder);

        // stuff away the variables
        m_state[0] = h0;
        m_state[1] = h1;
        m_state[2] = h2;
        m_state[3] = h3;
        m_state[4] = h4;
        m_state[5] = h5;
        m_state[6] = h6;
        m_state[7] = h7;
        m_state[8] = h8;
        m_state[9] = h9;
        m_state[10] = h10;
        m_state[11] = h11;
    }

    //
    // Final: compute the hash for the current SpookyHash state
    //
    // This does not modify the state; you can keep updating it afterward
    //
    // The result is the same as if SpookyHash() had been called with
    // all the pieces concatenated into one message.
    //
    void Final(ulong* hash1, // out only: first 64 bits of hash value.
            ulong* hash2) // out only: second 64 bits of hash value.
            {
        // init the variables
        if (m_length < sc_bufSize)
        {
            *hash1 = m_state[0];
            *hash2 = m_state[1];
            Short(m_data.ptr, m_length, hash1, hash2);
            return;
        }

        const(ulong)* data = cast(const ulong*) m_data;
        ubyte remainder = m_remainder;

        ulong h0 = m_state[0];
        ulong h1 = m_state[1];
        ulong h2 = m_state[2];
        ulong h3 = m_state[3];
        ulong h4 = m_state[4];
        ulong h5 = m_state[5];
        ulong h6 = m_state[6];
        ulong h7 = m_state[7];
        ulong h8 = m_state[8];
        ulong h9 = m_state[9];
        ulong h10 = m_state[10];
        ulong h11 = m_state[11];

        if (remainder >= sc_blockSize)
        {
            // m_data can contain two blocks; handle any whole first block
            Mix(data, h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
            data += sc_numVars;
            remainder -= sc_blockSize;
        }

        // mix in the last partial block, and the length mod sc_blockSize
        memset(&(cast(ubyte*) data)[remainder], 0, (sc_blockSize - remainder));

        (cast(ubyte*) data)[sc_blockSize - 1] = remainder;

        // do some final mixing
        End(data, h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);

        *hash1 = h0;
        *hash2 = h1;
    }
    //
    // left rotate a 64-bit value by k bytes
    //
    pragma(inline, true) static ulong Rot64(ulong x, int k)
    {
        return (x << k) | (x >> (64 - k));
    }

    //
    // This is used if the input is 96 bytes long or longer.
    //
    // The internal state is fully overwritten every 96 bytes.
    // Every input bit appears to cause at least 128 bits of entropy
    // before 96 other bytes are combined, when run forward or backward
    //   For every input bit,
    //   Two inputs differing in just that input bit
    //   Where "differ" means xor or subtraction
    //   And the base value is random
    //   When run forward or backwards one Mix
    // I tried 3 pairs of each; they all differed by at least 212 bits.
    //
    pragma(inline, true) static void Mix(const ulong* data, ref ulong s0,
            ref ulong s1, ref ulong s2, ref ulong s3, ref ulong s4, ref ulong s5,
            ref ulong s6, ref ulong s7, ref ulong s8, ref ulong s9, ref ulong s10, ref ulong s11)
    {
        s0 += data[0];
        s2 ^= s10;
        s11 ^= s0;
        s0 = Rot64(s0, 11);
        s11 += s1;
        s1 += data[1];
        s3 ^= s11;
        s0 ^= s1;
        s1 = Rot64(s1, 32);
        s0 += s2;
        s2 += data[2];
        s4 ^= s0;
        s1 ^= s2;
        s2 = Rot64(s2, 43);
        s1 += s3;
        s3 += data[3];
        s5 ^= s1;
        s2 ^= s3;
        s3 = Rot64(s3, 31);
        s2 += s4;
        s4 += data[4];
        s6 ^= s2;
        s3 ^= s4;
        s4 = Rot64(s4, 17);
        s3 += s5;
        s5 += data[5];
        s7 ^= s3;
        s4 ^= s5;
        s5 = Rot64(s5, 28);
        s4 += s6;
        s6 += data[6];
        s8 ^= s4;
        s5 ^= s6;
        s6 = Rot64(s6, 39);
        s5 += s7;
        s7 += data[7];
        s9 ^= s5;
        s6 ^= s7;
        s7 = Rot64(s7, 57);
        s6 += s8;
        s8 += data[8];
        s10 ^= s6;
        s7 ^= s8;
        s8 = Rot64(s8, 55);
        s7 += s9;
        s9 += data[9];
        s11 ^= s7;
        s8 ^= s9;
        s9 = Rot64(s9, 54);
        s8 += s10;
        s10 += data[10];
        s0 ^= s8;
        s9 ^= s10;
        s10 = Rot64(s10, 22);
        s9 += s11;
        s11 += data[11];
        s1 ^= s9;
        s10 ^= s11;
        s11 = Rot64(s11, 46);
        s10 += s0;
    }

    //
    // Mix all 12 inputs together so that h0, h1 are a hash of them all.
    //
    // For two inputs differing in just the input bits
    // Where "differ" means xor or subtraction
    // And the base value is random, or a counting value starting at that bit
    // The final result will have each bit of h0, h1 flip
    // For every input bit,
    // with probability 50 +- .3%
    // For every pair of input bits,
    // with probability 50 +- 3%
    //
    // This does not rely on the last Mix() call having already mixed some.
    // Two iterations was almost good enough for a 64-bit result, but a
    // 128-bit result is reported, so End() does three iterations.
    //
    pragma(inline, true) static void EndPartial(ref ulong h0, ref ulong h1,
            ref ulong h2, ref ulong h3, ref ulong h4, ref ulong h5, ref ulong h6,
            ref ulong h7, ref ulong h8, ref ulong h9, ref ulong h10, ref ulong h11)
    {
        h11 += h1;
        h2 ^= h11;
        h1 = Rot64(h1, 44);
        h0 += h2;
        h3 ^= h0;
        h2 = Rot64(h2, 15);
        h1 += h3;
        h4 ^= h1;
        h3 = Rot64(h3, 34);
        h2 += h4;
        h5 ^= h2;
        h4 = Rot64(h4, 21);
        h3 += h5;
        h6 ^= h3;
        h5 = Rot64(h5, 38);
        h4 += h6;
        h7 ^= h4;
        h6 = Rot64(h6, 33);
        h5 += h7;
        h8 ^= h5;
        h7 = Rot64(h7, 10);
        h6 += h8;
        h9 ^= h6;
        h8 = Rot64(h8, 13);
        h7 += h9;
        h10 ^= h7;
        h9 = Rot64(h9, 38);
        h8 += h10;
        h11 ^= h8;
        h10 = Rot64(h10, 53);
        h9 += h11;
        h0 ^= h9;
        h11 = Rot64(h11, 42);
        h10 += h0;
        h1 ^= h10;
        h0 = Rot64(h0, 54);
    }

    pragma(inline, true) static void End(const ulong* data, ref ulong h0,
            ref ulong h1, ref ulong h2, ref ulong h3, ref ulong h4, ref ulong h5,
            ref ulong h6, ref ulong h7, ref ulong h8, ref ulong h9, ref ulong h10, ref ulong h11)
    {
        h0 += data[0];
        h1 += data[1];
        h2 += data[2];
        h3 += data[3];
        h4 += data[4];
        h5 += data[5];
        h6 += data[6];
        h7 += data[7];
        h8 += data[8];
        h9 += data[9];
        h10 += data[10];
        h11 += data[11];
        EndPartial(h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
        EndPartial(h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
        EndPartial(h0, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, h11);
    }

    //
    // The goal is for each bit of the input to expand into 128 bits of 
    //   apparent entropy before it is fully overwritten.
    // n trials both set and cleared at least m bits of h0 h1 h2 h3
    //   n: 2   m: 29
    //   n: 3   m: 46
    //   n: 4   m: 57
    //   n: 5   m: 107
    //   n: 6   m: 146
    //   n: 7   m: 152
    // when run forwards or backwards
    // for all 1-bit and 2-bit diffs
    // with diffs defined by either xor or subtraction
    // with a base of all zeros plus a counter, or plus another bit, or random
    //
    pragma(inline, true) static void ShortMix(ref ulong h0, ref ulong h1, ref ulong h2, ref ulong h3)
    {
        h2 = Rot64(h2, 50);
        h2 += h3;
        h0 ^= h2;
        h3 = Rot64(h3, 52);
        h3 += h0;
        h1 ^= h3;
        h0 = Rot64(h0, 30);
        h0 += h1;
        h2 ^= h0;
        h1 = Rot64(h1, 41);
        h1 += h2;
        h3 ^= h1;
        h2 = Rot64(h2, 54);
        h2 += h3;
        h0 ^= h2;
        h3 = Rot64(h3, 48);
        h3 += h0;
        h1 ^= h3;
        h0 = Rot64(h0, 38);
        h0 += h1;
        h2 ^= h0;
        h1 = Rot64(h1, 37);
        h1 += h2;
        h3 ^= h1;
        h2 = Rot64(h2, 62);
        h2 += h3;
        h0 ^= h2;
        h3 = Rot64(h3, 34);
        h3 += h0;
        h1 ^= h3;
        h0 = Rot64(h0, 5);
        h0 += h1;
        h2 ^= h0;
        h1 = Rot64(h1, 36);
        h1 += h2;
        h3 ^= h1;
    }

    //
    // Mix all 4 inputs together so that h0, h1 are a hash of them all.
    //
    // For two inputs differing in just the input bits
    // Where "differ" means xor or subtraction
    // And the base value is random, or a counting value starting at that bit
    // The final result will have each bit of h0, h1 flip
    // For every input bit,
    // with probability 50 +- .3% (it is probably better than that)
    // For every pair of input bits,
    // with probability 50 +- .75% (the worst case is approximately that)
    //
    pragma(inline, true) static void ShortEnd(ref ulong h0, ref ulong h1, ref ulong h2, ref ulong h3)
    {
        h3 ^= h2;
        h2 = Rot64(h2, 15);
        h3 += h2;
        h0 ^= h3;
        h3 = Rot64(h3, 52);
        h0 += h3;
        h1 ^= h0;
        h0 = Rot64(h0, 26);
        h1 += h0;
        h2 ^= h1;
        h1 = Rot64(h1, 51);
        h2 += h1;
        h3 ^= h2;
        h2 = Rot64(h2, 28);
        h3 += h2;
        h0 ^= h3;
        h3 = Rot64(h3, 9);
        h0 += h3;
        h1 ^= h0;
        h0 = Rot64(h0, 47);
        h1 += h0;
        h2 ^= h1;
        h1 = Rot64(h1, 54);
        h2 += h1;
        h3 ^= h2;
        h2 = Rot64(h2, 32);
        h3 += h2;
        h0 ^= h3;
        h3 = Rot64(h3, 25);
        h0 += h3;
        h1 ^= h0;
        h0 = Rot64(h0, 63);
        h1 += h0;
    }

private:

    //
    // Short is used for messages under 192 bytes in length
    // Short has a low startup cost, the normal mode is good for long
    // keys, the cost crossover is at about 192 bytes.  The two modes were
    // held to the same quality bar.
    //

    //
    // short hash ... it could be used on any message, 
    // but it's used by Spooky just for short messages.
    //

    static void Short(const(void)* message, // message (array of bytes, not necessarily aligned)
            size_t length, // length of message (in bytes)
            ulong* hash1, // in/out: in the seed, out the hash value
            ulong* hash2) // in/out: in the seed, out the hash value
            {
        ulong[2 * sc_numVars] buf;
        union U3
        {
            const(ubyte)* p8;
            uint* p32;
            ulong* p64;
            size_t i;
        }

        U3 u;
        u.p8 = cast(const(ubyte)*) message;

        if (!HTS_ALLOW_UNALIGNED && (u.i & 0x7))
        {
            memcpy(buf.ptr, message, length);
            u.p64 = buf.ptr;
        }

        size_t remainder = length % 32;
        ulong a = *hash1;
        ulong b = *hash2;
        ulong c = sc_const;
        ulong d = sc_const;

        if (length > 15)
        {
            const ulong* end = u.p64 + (length / 32) * 4;

            // handle all complete sets of 32 bytes
            for (; u.p64 < end; u.p64 += 4)
            {
                c += u.p64[0];
                d += u.p64[1];
                ShortMix(a, b, c, d);
                a += u.p64[2];
                b += u.p64[3];
            }

            //Handle the case of 16+ remaining bytes.
            if (remainder >= 16)
            {
                c += u.p64[0];
                d += u.p64[1];
                ShortMix(a, b, c, d);
                u.p64 += 2;
                remainder -= 16;
            }
        }

        // Handle the last 0..15 bytes, and its length
        d += (cast(ulong) length) << 56;
        switch (remainder)
        {
        case 15:
            d += (cast(ulong) u.p8[14]) << 48;
            goto case 14;
        case 14:
            d += (cast(ulong) u.p8[13]) << 40;
            goto case 13;
        case 13:
            d += (cast(ulong) u.p8[12]) << 32;
            goto case 12;
        case 12:
            d += u.p32[2];
            c += u.p64[0];
            break;
        case 11:
            d += (cast(ulong) u.p8[10]) << 16;
            goto case 10;
        case 10:
            d += (cast(ulong) u.p8[9]) << 8;
            goto case 9;
        case 9:
            d += cast(ulong) u.p8[8];
            goto case 8;
        case 8:
            c += u.p64[0];
            break;
        case 7:
            c += (cast(ulong) u.p8[6]) << 48;
            goto case 6;
        case 6:
            c += (cast(ulong) u.p8[5]) << 40;
            goto case 5;
        case 5:
            c += (cast(ulong) u.p8[4]) << 32;
            goto case 4;
        case 4:
            c += u.p32[0];
            break;
        case 3:
            c += (cast(ulong) u.p8[2]) << 16;
            goto case 2;
        case 2:
            c += (cast(ulong) u.p8[1]) << 8;
            goto case 1;
        case 1:
            c += cast(ulong) u.p8[0];
            break;
        case 0:
            c += sc_const;
            d += sc_const;
            break;
        default:
            assert(0);
        }
        ShortEnd(a, b, c, d);
        *hash1 = a;
        *hash2 = b;
    }

    // number of ulong's in internal state
    static const size_t sc_numVars = 12;

    // size of the internal state
    static const size_t sc_blockSize = sc_numVars * 8;

    // size of buffer of unhashed data, in bytes
    static const size_t sc_bufSize = 2 * sc_blockSize;

    //
    // sc_const: a constant which:
    //  * is not zero
    //  * is odd
    //  * is a not-very-regular mix of 1's and 0's
    //  * does not need any other special mathematical properties
    //
    static ulong sc_const = 0xdeadbeefdeadbeef;

    ulong[2 * sc_numVars] m_data; // unhashed data, for partial messages
    ulong[sc_numVars] m_state; // internal state of the hash
    size_t m_length; // total length of the input so far
    ubyte m_remainder; // length of unhashed data stashed in m_data
}

struct Random
{
public:
    pragma(inline, true) ulong Value()
    {
        ulong e = m_a - Rot64(m_b, 23);
        m_a = m_b ^ Rot64(m_c, 16);
        m_b = m_c + Rot64(m_d, 11);
        m_c = m_d + e;
        m_d = e + m_a;
        return m_d;
    }

    pragma(inline, true) void Init(ulong seed)
    {
        m_a = 0xdeadbeef;
        m_b = m_c = m_d = seed;
        for (int i = 0; i < 20; ++i)
            cast(void) Value();
    }

private:
    pragma(inline, true) static ulong Rot64(ulong x, int k)
    {
        return (x << k) | (x >> (64 - (k)));
    }

    ulong m_a;
    ulong m_b;
    ulong m_c;
    ulong m_d;
};

// fastest conceivable hash function (for comparison)
static void Add(const(void)* data, size_t length, ulong* hash1, ulong* hash2)
{
    ulong* p64 = cast(ulong*) data;
    ulong* end = p64 + length / 8;
    ulong hash = *hash1 + *hash2;
    while (p64 < end)
    {
        hash += *p64;
        ++p64;
    }
    *hash1 = hash;
    *hash2 = hash;
}

void TestResults()
{
    enum BUFSIZE = 512;
    writeln("\ntesting results ...\n");
    const ulong[BUFSIZE] expected = [
        0x6bf50919, 0x70de1d26, 0xa2b37298, 0x35bc5fbf, 0x8223b279, 0x5bcb315e,
        0x53fe88a1, 0xf9f1a233, 0xee193982, 0x54f86f29, 0xc8772d36,
        0x9ed60886, 0x5f23d1da, 0x1ed9f474, 0xf2ef0c89, 0x83ec01f9,
        0xf274736c, 0x7e9ac0df, 0xc7aed250, 0xb1015811, 0xe23470f5, 0x48ac20c4,
        0xe2ab3cd5, 0x608f8363, 0xd0639e68, 0xc4e8e7ab, 0x863c7c5b,
        0x4ea63579, 0x99ae8622, 0x170c658b, 0x149ba493, 0x027bca7c,
        0xe5cfc8b6, 0xce01d9d7, 0x11103330, 0x5d1f5ed4, 0xca720ecb, 0xef408aec,
        0x733b90ec, 0x855737a6, 0x9856c65f, 0x647411f7, 0x50777c74,
        0xf0f1a8b7, 0x9d7e55a5, 0xc68dd371, 0xfc1af2cc, 0x75728d0a,
        0x390e5fdc, 0xf389b84c, 0xfb0ccf23, 0xc95bad0e, 0x5b1cb85a, 0x6bdae14f,
        0x6deb4626, 0x93047034, 0x6f3266c6, 0xf529c3bd, 0x396322e7,
        0x3777d042, 0x1cd6a5a2, 0x197b402e, 0xc28d0d2b, 0x09c1afb4,

        0x069c8bb7, 0x6f9d4e1e, 0xd2621b5c, 0xea68108d, 0x8660cb8f, 0xd61e6de6,
        0x7fba15c7, 0xaacfaa97, 0xdb381902, 0x4ea22649, 0x5d414a1e,
        0xc3fc5984, 0xa0fc9e10, 0x347dc51c, 0x37545fb6, 0x8c84b26b,
        0xf57efa5d, 0x56afaf16, 0xb6e1eb94, 0x9218536a, 0xe3cc4967, 0xd3275ef4,
        0xea63536e, 0x6086e499, 0xaccadce7, 0xb0290d82, 0x4ebfd0d6,
        0x46ccc185, 0x2eeb10d3, 0x474e3c8c, 0x23c84aee, 0x3abae1cb,
        0x1499b81a, 0xa2993951, 0xeed176ad, 0xdfcfe84c, 0xde4a961f, 0x4af13fe6,
        0xe0069c42, 0xc14de8f5, 0x6e02ce8f, 0x90d19f7f, 0xbca4a484,
        0xd4efdd63, 0x780fd504, 0xe80310e3, 0x03abbc12, 0x90023849,
        0xd6f6fb84, 0xd6b354c5, 0x5b8575f0, 0x758f14e4, 0x450de862, 0x90704afb,
        0x47209a33, 0xf226b726, 0xf858dab8, 0x7c0d6de9, 0xb05ce777,
        0xee5ff2d4, 0x7acb6d5c, 0x2d663f85, 0x41c72a91, 0x82356bf2,

        0x94e948ec, 0xd358d448, 0xeca7814d, 0x78cd7950, 0xd6097277, 0x97782a5d,
        0xf43fc6f4, 0x105f0a38, 0x9e170082, 0x4bfe566b, 0x4371d25f,
        0xef25a364, 0x698eb672, 0x74f850e4, 0x4678ff99, 0x4a290dc6,
        0x3918f07c, 0x32c7d9cd, 0x9f28e0af, 0x0d3c5a86, 0x7bfc8a45, 0xddf0c7e1,
        0xdeacb86b, 0x970b3c5c, 0x5e29e199, 0xea28346d, 0x6b59e71b,
        0xf8a8a46a, 0x862f6ce4, 0x3ccb740b, 0x08761e9e, 0xbfa01e5f,
        0xf17cfa14, 0x2dbf99fb, 0x7a0be420, 0x06137517, 0xe020b266, 0xd25bfc61,
        0xff10ed00, 0x42e6be8b, 0x029ef587, 0x683b26e0, 0xb08afc70,
        0x7c1fd59e, 0xbaae9a70, 0x98c8c801, 0xb6e35a26, 0x57083971,
        0x90a6a680, 0x1b44169e, 0x1dce237c, 0x518e0a59, 0xccb11358, 0x7b8175fb,
        0xb8fe701a, 0x10d259bb, 0xe806ce10, 0x9212be79, 0x4604ae7b,
        0x7fa22a84, 0xe715b13a, 0x0394c3b2, 0x11efbbae, 0xe13d9e19,

        0x77e012bd, 0x2d05114c, 0xaecf2ddd, 0xb2a2b4aa, 0xb9429546, 0x55dce815,
        0xc89138f8, 0x46dcae20, 0x1f6f7162, 0x0c557ebc, 0x5b996932,
        0xafbbe7e2, 0xd2bd5f62, 0xff475b9f, 0x9cec7108, 0xeaddcffb,
        0x5d751aef, 0xf68f7bdf, 0xf3f4e246, 0x00983fcd, 0x00bc82bb, 0xbf5fd3e7,
        0xe80c7e2c, 0x187d8b1f, 0xefafb9a7, 0x8f27a148, 0x5c9606a9,
        0xf2d2be3e, 0xe992d13a, 0xe4bcd152, 0xce40b436, 0x63d6a1fc,
        0xdc1455c4, 0x64641e39, 0xd83010c9, 0x2d535ae0, 0x5b748f3e, 0xf9a9146b,
        0x80f10294, 0x2859acd4, 0x5fc846da, 0x56d190e9, 0x82167225,
        0x98e4daba, 0xbf7865f3, 0x00da7ae4, 0x9b7cd126, 0x644172f8,
        0xde40c78f, 0xe8803efc, 0xdd331a2b, 0x48485c3c, 0x4ed01ddc, 0x9c0b2d9e,
        0xb1c6e9d7, 0xd797d43c, 0x274101ff, 0x3bf7e127, 0x91ebbc56,
        0x7ffeb321, 0x4d42096f, 0xd6e9456a, 0x0bade318, 0x2f40ee0b,

        0x38cebf03, 0x0cbc2e72, 0xbf03e704, 0x7b3e7a9a, 0x8e985acd, 0x90917617,
        0x413895f8, 0xf11dde04, 0xc66f8244, 0xe5648174, 0x6c420271,
        0x2469d463, 0x2540b033, 0xdc788e7b, 0xe4140ded, 0x0990630a,
        0xa54abed4, 0x6e124829, 0xd940155a, 0x1c8836f6, 0x38fda06c, 0x5207ab69,
        0xf8be9342, 0x774882a8, 0x56fc0d7e, 0x53a99d6e, 0x8241f634,
        0x9490954d, 0x447130aa, 0x8cc4a81f, 0x0868ec83, 0xc22c642d,
        0x47880140, 0xfbff3bec, 0x0f531f41, 0xf845a667, 0x08c15fb7, 0x1996cd81,
        0x86579103, 0xe21dd863, 0x513d7f97, 0x3984a1f1, 0xdfcdc5f4,
        0x97766a5e, 0x37e2b1da, 0x41441f3f, 0xabd9ddba, 0x23b755a9,
        0xda937945, 0x103e650e, 0x3eef7c8f, 0x2760ff8d, 0x2493a4cd, 0x1d671225,
        0x3bf4bd4c, 0xed6e1728, 0xc70e9e30, 0x4e05e529, 0x928d5aa6,
        0x164d0220, 0xb5184306, 0x4bd7efb3, 0x63830f11, 0xf3a1526c,

        0xf1545450, 0xd41d5df5, 0x25a5060d, 0x77b368da, 0x4fe33c7e, 0xeae09021,
        0xfdb053c4, 0x2930f18d, 0xd37109ff, 0x8511a781, 0xc7e7cdd7,
        0x6aeabc45, 0xebbeaeaa, 0x9a0c4f11, 0xda252cbb, 0x5b248f41,
        0x5223b5eb, 0xe32ab782, 0x8e6a1c97, 0x11d3f454, 0x3e05bd16, 0x0059001d,
        0xce13ac97, 0xf83b2b4c, 0x71db5c9a, 0xdc8655a6, 0x9e98597b,
        0x3fcae0a2, 0x75e63ccd, 0x076c72df, 0x4754c6ad, 0x26b5627b,
        0xd818c697, 0x998d5f3d, 0xe94fc7b2, 0x1f49ad1a, 0xca7ff4ea, 0x9fe72c05,
        0xfbd0cbbf, 0xb0388ceb, 0xb76031e3, 0xd0f53973, 0xfb17907c,
        0xa4c4c10f, 0x9f2d8af9, 0xca0e56b0, 0xb0d9b689, 0xfcbf37a3,
        0xfede8f7d, 0xf836511c, 0x744003fc, 0x89eba576, 0xcfdcf6a6, 0xc2007f52,
        0xaaaf683f, 0x62d2f9ca, 0xc996f77f, 0x77a7b5b3, 0x8ba7d0a4,
        0xef6a0819, 0xa0d903c0, 0x01b27431, 0x58fffd4c, 0x4827f45c,

        0x44eb5634, 0xae70edfc, 0x591c740b, 0x478bf338, 0x2f3b513b, 0x67bf518e,
        0x6fef4a0c, 0x1e0b6917, 0x5ac0edc5, 0x2e328498, 0x077de7d5,
        0x5726020b, 0x2aeda888, 0x45b637ca, 0xcf60858d, 0x3dc91ae2,
        0x3e6d5294, 0xe6900d39, 0x0f634c71, 0x827a5fa4, 0xc713994b, 0x1c363494,
        0x3d43b615, 0xe5fe7d15, 0xf6ada4f2, 0x472099d5, 0x04360d39,
        0x7f2a71d0, 0x88a4f5ff, 0x2c28fac5, 0x4cd64801, 0xfd78dd33,
        0xc9bdd233, 0x21e266cc, 0x9bbf419d, 0xcbf7d81d, 0x80f15f96, 0x04242657,
        0x53fb0f66, 0xded11e46, 0xf2fdba97, 0x8d45c9f1, 0x4eeae802,
        0x17003659, 0xb9db81a7, 0xe734b1b2, 0x9503c54e, 0xb7c77c3e,
        0x271dd0ab, 0xd8b906b5, 0x0d540ec6, 0xf03b86e0, 0x0fdb7d18, 0x95e261af,
        0xad9ec04e, 0x381f4a64, 0xfec798d7, 0x09ea20be, 0x0ef4ca57,
        0x1e6195bb, 0xfd0da78b, 0xcea1653b, 0x157d9777, 0xf04af50f,

        0xad7baa23, 0xd181714a, 0x9bbdab78, 0x6c7d1577, 0x645eb1e7, 0xa0648264,
        0x35839ca6, 0x2287ef45, 0x32a64ca3, 0x26111f6f, 0x64814946,
        0xb0cddaf1, 0x4351c59e, 0x1b30471c, 0xb970788a, 0x30e9f597,
        0xd7e58df1, 0xc6d2b953, 0xf5f37cf4, 0x3d7c419e, 0xf91ecb2d, 0x9c87fd5d,
        0xb22384ce, 0x8c7ac51c, 0x62c96801, 0x57e54091, 0x964536fe,
        0x13d3b189, 0x4afd1580, 0xeba62239, 0xb82ea667, 0xae18d43a,
        0xbef04402, 0x1942534f, 0xc54bf260, 0x3c8267f5, 0xa1020ddd, 0x112fcc8a,
        0xde596266, 0xe91d0856, 0xf300c914, 0xed84478e, 0x5b65009e,
        0x4764da16, 0xaf8e07a2, 0x4088dc2c, 0x9a0cad41, 0x2c3f179b,
        0xa67b83f7, 0xf27eab09, 0xdbe10e28, 0xf04c911f, 0xd1169f87, 0x8e1e4976,
        0x17f57744, 0xe4f5a33f, 0x27c2e04b, 0x0b7523bd, 0x07305776,
        0xc6be7503, 0x918fa7c9, 0xaf2e2cd9, 0x82046f8e, 0xcc1c8250
    ];

    ubyte[BUFSIZE] buf;
    uint[BUFSIZE] saw;
    for (int i = 0; i < BUFSIZE; ++i)
    {
        buf[i] = cast(ubyte)(i + 128);
        saw[i] = SpookyHash.Hash32(buf.ptr, i, 0);
        if (saw[i] != expected[i])
        {
            writefln("%3d: saw 0x%.8lx, expected 0x%.8lx\n", i, saw[i], expected[i]);
        }
    }
}

// void DoTimingBig(int seed)
// {
//     enum NUMBUF = (1<<10);
//     enum BUFSIZE = (1<<20);
//     writeln("\ntesting time to hash 2^^30 bytes ...\n");

//     char[NUMBUF] buf;
//     for (int i=0; i<NUMBUF; ++i)
//     {
//         buf[i] = cast(char *)malloc(BUFSIZE);
//         memset(buf[i], cast(char)seed, BUFSIZE);
//     }

//     ulong a = GetTickCount();
//     ulong hash1 = seed;
//     ulong hash2 = seed;
//     for (ulong i=0; i<NUMBUF; ++i)
//     {
// 	SpookyHash.Hash128(buf[i], BUFSIZE, &hash1, &hash2);
//     }
//     ulong z = GetTickCount();
//     writefln("SpookyHash::Hash128, uncached: time is %4lld milliseconds\n", z-a);

//     a = GetTickCount();
//     for (ulong i=0; i<NUMBUF; ++i)
//     {
// 	Add(buf[i], BUFSIZE, &hash1, &hash2);
//     }
//     z = GetTickCount();
//     printf("Addition           , uncached: time is %4lld milliseconds\n", z-a);

//     a = GetTickCount();
//     for (ulong i=0; i<NUMBUF*BUFSIZE/1024; ++i)
//     {
// 	SpookyHash.Hash128(buf[0], 1024, &hash1, &hash2);
//     }
//     z = GetTickCount();
//     printf("SpookyHash::Hash128,   cached: time is %4lld milliseconds\n", z-a);

//     a = GetTickCount();
//     for (ulong i=0; i<NUMBUF*BUFSIZE/1024; ++i)
//     {
// 	Add(buf[0], 1024, &hash1, &hash2);
//     }
//     z = GetTickCount();
//     printf("Addition           ,   cached: time is %4lld milliseconds\n", z-a);

//     for (int i=0; i<NUMBUF; ++i)
//     { 
//         free(buf[i]);
//         buf[i] = 0;
//     }
// }

// void DoTimingSmall(int seed)
// {
//     enum BUFSIZE = (1<<14);
//     enum NUMITER = 10000000;
//     writefln("\ntesting timing of hashing up to %d cached aligned bytes %d times ...\n",
//            BUFSIZE, NUMITER);

//     ulong[BUFSIZE/8] buf;
//     for (int i=0; i<BUFSIZE/8; ++i)
//     {
//         buf[i] = i+seed;
//     }

//     for (int i=1; i <= BUFSIZE; i <<= 1)
//     {
//         ulong a = GetTickCount();
//         ulong hash1 = seed;
//         ulong hash2 = seed+i;
//         for (int j=0; j<NUMITER; ++j)
//         {
//             SpookyHash.Hash128(cast(char *)buf, i, &hash1, &hash2);
//         }
//         ulong z = GetTickCount();
//         writefln("%d bytes: hash is %.16llx %.16llx, time is %lld\n", 
//                i, hash1, hash2, z-a);
//     }
// }

void TestAlignment()
{
    enum BUFSIZE = 1024;
    writeln("\ntesting alignment ...\n");

    char[BUFSIZE] buf;
    ulong[8] hash;
    for (int i = 0; i < BUFSIZE - 16; ++i)
    {
        for (int j = 0; j < 8; ++j)
        {
            buf[j] = cast(char)(i + j);
            for (int k = 1; k <= i; ++k)
            {
                buf[j + k] = cast(char) k;
            }
            buf[j + i + 1] = cast(char)(i + j);
            hash[j] = SpookyHash.Hash64(cast(const(void)*)(buf.ptr + j + 1), i, 0);
        }
        for (int j = 1; j < 8; ++j)
        {
            if (hash[0] != hash[j])
            {
                writeln("alignment problems: %d %d\n", i, j);
            }
        }
    }
}

// test that all deltas of one or two input bits affect all output bits

void TestDeltas(int seed)
{
    enum BUFSIZE = 256;
    enum TRIES = 50;
    enum MEASURES = 6;
    writefln("\nall 1 or 2 bit input deltas get %d tries to flip every output bit ...\n", TRIES);

    Random random;
    random.Init(cast(ulong) seed);

    // for messages 0..BUFSIZE-1 bytes
    for (int h = 0; h < BUFSIZE; ++h)
    {
        int maxk = 0;
        // first bit to set
        for (int i = 0; i < h * 8; ++i)
        {
            // second bit to set, or don't have a second bit
            for (int j = 0; j <= i; ++j)
            {
                ulong[2][MEASURES] measure;
                ulong[2][MEASURES] counter;
                for (int l = 0; l < 2; ++l)
                {
                    for (int m = 0; m < MEASURES; ++m)
                    {
                        counter[m][l] = 0;
                    }
                }

                // try to hit every output bit TRIES times
                int k;
                for (k = 0; k < TRIES; ++k)
                {
                    ubyte[BUFSIZE] buf1;
                    ubyte[BUFSIZE] buf2;
                    int done = 1;
                    for (int l = 0; l < h; ++l)
                    {
                        buf1[l] = buf2[l] = cast(ubyte) random.Value();
                    }
                    buf1[i / 8] ^= (1 << (i % 8));
                    if (j != i)
                    {
                        buf1[j / 8] ^= (1 << (j % 8));
                    }
                    SpookyHash.Hash128(buf1.ptr, h, &measure[0][0], &measure[0][1]);
                    SpookyHash.Hash128(buf2.ptr, h, &measure[1][0], &measure[1][1]);
                    for (int l = 0; l < 2; ++l)
                    {
                        measure[2][l] = measure[0][l] ^ measure[1][l];
                        measure[3][l] = ~(measure[0][l] ^ measure[1][l]);
                        measure[4][l] = measure[0][l] - measure[1][l];
                        measure[4][l] ^= (measure[4][l] >> 1);
                        measure[5][l] = measure[0][l] + measure[1][l];
                        measure[5][l] ^= (measure[5][l] >> 1);
                    }
                    for (int l = 0; l < 2; ++l)
                    {
                        for (int m = 0; m < MEASURES; ++m)
                        {
                            counter[m][l] |= measure[m][l];
                            if (~counter[m][l])
                                done = 0;
                        }
                    }
                    if (done)
                        break;
                }
                if (k == TRIES)
                {
                    writefln("failed %d %d %d\n", h, i, j);
                }
                else if (k > maxk)
                {
                    maxk = k;
                }
            }
        }
        writefln("passed for buffer size %d  max %d\n", h, maxk);
    }
}

// test that hashing pieces has the same behavior as hashing the whole
void TestPieces()
{
    enum BUFSIZE = 1024;
    writeln("\ntesting pieces ...\n");
    char[BUFSIZE] buf;
    for (int i = 0; i < BUFSIZE; ++i)
    {
        buf[i] = cast(char) i;
    }
    for (int i = 0; i < BUFSIZE; ++i)
    {
        ulong a, b, c, d, seed1 = 1, seed2 = 2;
        SpookyHash state;

        // all as one call
        a = seed1;
        b = seed2;
        SpookyHash.Hash128(buf.ptr, i, &a, &b);

        // all as one piece
        c = 0xdeadbeefdeadbeef;
        d = 0xbaceba11baceba11;
        state.Init(seed1, seed2);
        state.Update(buf.ptr, i);
        state.Final(&c, &d);

        if (a != c)
        {
            writefln("wrong a %d: %.16llx %.16llx\n", i, a, c);
        }
        if (b != d)
        {
            writefln("wrong b %d: %.16llx %.16llx\n", i, b, d);
        }

        // all possible two consecutive pieces
        for (int j = 0; j < i; ++j)
        {
            c = seed1;
            d = seed2;
            state.Init(c, d);
            state.Update(&buf[0], j);
            state.Update(&buf[j], i - j);
            state.Final(&c, &d);
            if (a != c)
            {
                writefln("wrong a %d %d: %.16llx %.16llx\n", j, i, a, c);
            }
            if (b != d)
            {
                writefln("wrong b %d %d: %.16llx %.16llx\n", j, i, b, d);
            }
        }
    }
}

unittest
{
    TestResults();
    TestAlignment();
    TestPieces();
    // DoTimingBig(argc);
    // DoTimingSmall(argc);
    // TestDeltas(argc);
}
