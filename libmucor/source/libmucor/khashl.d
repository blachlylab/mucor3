module libmucor.khashl;

/* The MIT License
   Copyright (c) 2019 by Attractive Chaos <attractor@live.co.uk>
   Copyright (c) 2019 James S Blachly, MD <james.blachly@gmail.com>
   
   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:
   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.
   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

import std.traits : isNumeric, isSomeString, isSigned, hasMember, isArray;
import core.stdc.stdint; // uint32_t, etc.
import core.memory; // GC

import asdf;
import libmucor.wideint;
import libmucor.jsonlops.jsonvalue : JSONValue;
import std.sumtype : match;

/*!
  @header
  Generic hash table library.
 */

enum AC_VERSION_KHASHL_H = "0.1";

import core.stdc.stdlib;
import core.stdc.string;
import core.stdc.limits;

/* compiler specific configuration */

alias khint32_t = uint;

alias khint64_t = ulong;

alias khint_t = khint32_t;
alias khiter_t = khint_t;

pragma(inline, true)
{
    auto __kh_used(T)(const(khint32_t)[] flag, T i)
    {
        return (flag[i >> 5] >> (i & 0x1fU) & 1U);
    }

    void __kh_set_used(T)(khint32_t[] flag, T i)
    {
        (flag[i >> 5] |= 1U << (i & 0x1fU));
    }

    void __kh_set_unused(T)(khint32_t[] flag, T i)
    {
        (flag[i >> 5] &= ~(1U << (i & 0x1fU)));
    }

    khint_t __kh_h2b(khint_t hash, khint_t bits)
    {
        return hash * 2654435769U >> (32 - bits);
    }

    auto __kh_fsize(khint_t m)
    {
        return ((m) < 32 ? 1 : (m) >> 5);
    }
}

/*  Straight port of khashl's generic C approach
    Khashl is hash table that performs deletions without tombstones 
    The main benefit over khash is that it uses less memory however it is 
    also faster than khash if no deletions are involved. 
    Can use cached-hashes for faster comparison of string key hashes 
    Attractive chaos has this to say about caching hash values (https://attractivechaos.wordpress.com/):
        When we use long strings as keys, comparing two keys may take significant time.
        This comparison is often unnecessary. Note that the hash of a string is a good 
        summary of the string. If two strings are different, their hashes are often 
        different. We can cache the hash and only compare two keys when their hashes are
        equal. It is possible to implement the idea with any hash table implementations. 
    
    If hash-caching is used we use compile time statements to change the bucket type to include
    the hash member. We also change the logic to make equality statements check hash equality before 
    checking the key equalitys and the put and get methods to make sure they don't recompute the hashes.
**/

template khashlSet(KT, bool cached = false)
{
    alias khashlSet = khashl!(KT, ubyte, false, cached);
}

alias StringSet = khashlSet!(string, true);

struct khashl(KT, VT, bool kh_is_map = true, bool cached = false) if (!isSigned!KT) // @suppress(dscanner.style.phobos_naming_convention)
{
    static assert(kh_is_map || is(VT == ubyte));

    alias __hash_func = kh_hash!KT.kh_hash_func;
    alias __hash_equal = kh_equal!(Bucket, cached).kh_hash_equal;

    alias kh_t = khashl; /// klib uses 'kh_t' struct name

    struct Bucket
    {
        KT key;
        static if (kh_is_map)
            VT val;
        static if (cached)
            khint_t hash;
    }

    khint_t bits, count;
    khint32_t[] used;
    Bucket[] keys;

pragma(inline, true):
    // ~this()
    // {
    //     //kh_destroy(&this); // the free(this) at the end of kh_destroy will SIGSEGV
    //     static if (useGC) {
    //         GC.removeRange(this.keys);
    //     }
    //     kfree(cast(void*) this.keys);
    //     kfree(cast(void*) this.used);
    // }

    typeof(this) dup()
    {
        return typeof(this)(this.bits, this.count, this.used.dup, this.keys.dup);
    }

    /// Lookup by key
    bool opBinaryRight(string op : "in")(const(KT) key) const
    {
        static if (cached)
            const(Bucket) ins = Bucket(*cast(KT*)&key, __hash_func(key));
        else
            const(Bucket) ins = Bucket(*cast(KT*)&key);
        auto x = this.kh_get(ins);
        if (x == this.kh_end())
        {
            return false;
        }
        return true;
    }

    /// remove key/value pair
    void remove(KT key)
    {
        Bucket ins;
        ins.key = key;
        static if (cached)
            ins.hash = __hash_func(ins.key); //cache the hash
        auto x = this.kh_get(ins);
        this.kh_del(x);
    }

    static if (kh_is_map)
    {

        /// Lookup by key
        VT* opBinaryRight(string op : "in")(const(KT) key)
        {
            Bucket ins;
            ins.key = key;
            static if (cached)
                ins.hash = __hash_func(ins.key); //cache the hash
            auto x = this.kh_get(ins);
            if (x == this.kh_end())
                return null;
            return &this.keys[x].val;
        }
        /// Lookup by key
        ref const(VT) opIndex(const(KT) key) const
        {
            Bucket ins;
            ins.key = key;
            static if (cached)
                ins.hash = __hash_func(ins.key); //cache the hash
            auto x = this.kh_get(ins);
            return this.keys[x].val;
        }

        /// Assign by key
        void opIndexAssign(const(VT) val, const(KT) key)
        {
            int absent;
            Bucket ins;
            ins.key = key;
            static if (cached)
                ins.hash = __hash_func(ins.key); //cache the hash
            auto x = this.kh_put(ins, &absent);
            this.keys[x].val = cast(VT) val;
            static if (cached)
                this.keys[x].hash = ins.hash; //cache the hash
        }

        /// Get or create if does not exist; mirror built-in hashmap
        /// https://dlang.org/spec/hash-map.html#inserting_if_not_present
        VT* require(const(KT) key, lazy const(VT) initval)
        {
            static assert(kh_is_map == true, "require() not sensible in a hash set");
            Bucket ins;
            ins.key = key;
            static if (cached)
                ins.hash = __hash_func(ins.key); //cache the hash
            auto x = this.kh_get(ins);
            if (x == this.kh_end())
            {
                // not present
                int absent;
                x = this.kh_put(ins, &absent);
                this.keys[x].val = cast(VT) initval;
                static if (cached)
                    this.keys[x].hash = ins.hash; //cache the hash
            }
            return &this.keys[x].val;
        }

        auto byKeyValue() const
        {
            /** Manipulating the hash table during iteration results in undefined behavior */
            struct KeyValueRange
            {
                import std.typecons : Tuple;

                alias KV = Tuple!(const(KT), "key", const(VT), "value");
                const(kh_t)* kh;
                khint_t itr;
                bool empty() // non-const as may call popFront
                {
                    //return (this.itr == kh_end(this.kh));
                    if (this.itr == kh.kh_end())
                        return true;
                    // Handle the case of deleted keys
                    else if (__kh_used(this.kh.used, this.itr) == 0)
                    {
                        while (__kh_used(this.kh.used, this.itr) == 0)
                        {
                            this.popFront();
                            if (this.itr == kh.kh_end())
                                return true;
                        }
                        return false;
                    }
                    return false;
                }

                const(KV) front()
                {
                    const(KV) ret = KV(kh.keys[this.itr].key, kh.keys[this.itr].val);
                    return ret;
                }

                void popFront()
                {
                    if (this.itr < kh.kh_end())
                    {
                        this.itr++;
                    }
                }
            }

            return KeyValueRange(&this);
        }

    }
    else
    {
        pragma(inline, true)
        void insert(const(KT) key)
        {
            int absent;
            static if (cached)
                const Bucket ins = Bucket(*cast(KT*)&key, __hash_func(key));
            else
                const Bucket ins = Bucket(*cast(KT*)&key);
            auto x = this.kh_put(ins, &absent);
        }

        pragma(inline, true)
        kh_t intersection(const(kh_t) other) const
        {
            kh_t ret;
            foreach (k; this.byKey)
            {
                if (k in other)
                    ret.insert(k);
            }
            return ret;
        }

        pragma(inline, true)
        kh_t difference(const(kh_t) other) const
        {
            kh_t ret;
            foreach (k; other.byKey)
            {
                if (!(k in this))
                    ret.insert(k);
            }
            return ret;
        }

        pragma(inline, true)
        kh_t set_union(const(kh_t) other) const
        {
            kh_t ret;
            foreach (k; this.byKey)
                ret.insert(k);
            foreach (k; other.byKey)
                ret.insert(k);
            return ret;
        }

        auto opBinaryRight(string op)(const(kh_t) other) const
        {
            static if (op == "&")
            {
                return this.intersection(other);
            }
            else static if (op == "-")
            {
                return this.difference(other);
            }
            else static if (op == "|")
            {
                return this.set_union(other);
            }
            else
            {
                static assert(0, "op not implemented");
            }
        }

        void opOpAssign(string op)(ref const(kh_t) other)
        {
            import std.array: array;
            static if (op == "&")
            {
                foreach (k; this.byKey.array)
                {
                    if (!(k in other))
                        this.remove(k);
                }
                
            }
            else static if (op == "-")
            {
                foreach (k; this.byKey.array)
                {
                    if (k in other)
                        this.remove(k);
                }
            }
            else static if (op == "|")
            {
                foreach (k; other.byKey)
                    this.insert(k);
            }
            else
            {
                static assert(0, "op not implemented");
            }
        }
    }

    /// Return an InputRange over the keys.
    /// Manipulating the hash table during iteration results in undefined behavior.
    /// Returns: Voldemort type
    auto byKey() const
    {
        /** Manipulating the hash table during iteration results in undefined behavior */
        struct KeyRange
        {
            const(kh_t)* kh;
            khint_t itr;
            bool empty() // non-const as may call popFront
            {
                //return (this.itr == kh_end(this.kh));
                if (this.itr == kh.kh_end())
                    return true;
                // Handle the case of deleted keys
                else if (__kh_used(this.kh.used, this.itr) == 0)
                {
                    while (__kh_used(this.kh.used, this.itr) == 0)
                    {
                        this.popFront();
                        if (this.itr == kh.kh_end())
                            return true;
                    }
                    return false;
                }
                return false;
            }

            ref const(KT) front()
            {
                return kh.keys[this.itr].key;
            }

            void popFront()
            {
                if (this.itr < kh.kh_end())
                {
                    this.itr++;
                }
            }
        }

        return KeyRange(&this);
    }

    void kh_clear()
    {
        if (this.used)
        {
            uint32_t n_buckets = 1U << this.bits;
            this.used[] = 0;
            this.count = 0;
        }
    }

    void kh_release()
    {
        this.kh_clear;
        import std.algorithm : move;

        KT key;
        VT val;
        khint_t itr;
        while (itr < this.kh_end())
        {
            move(this.keys[itr].key, key);
            static if (kh_is_map)
                move(this.keys[itr].val, val);
            itr++;
        }
    }

    khint_t kh_getp(const(Bucket)* key) const
    {
        khint_t i, last, n_buckets, mask;
        if (this.keys == [])
            return 0;
        n_buckets = 1U << this.bits;
        mask = n_buckets - 1U;

        /// if using caching, don't rehash key
        static if (cached)
            i = last = __kh_h2b((*key).hash, this.bits);
        else
            i = last = __kh_h2b(__hash_func((*key).key), this.bits);

        while (__kh_used(this.used, i) && !__hash_equal!(Bucket)(this.keys[i], *key))
        {
            i = (i + 1U) & mask;
            if (i == last)
                return n_buckets;
        }
        return !__kh_used(this.used, i) ? n_buckets : i;
    }

    khint_t kh_get(const(Bucket) key) const
    {
        return this.kh_getp(&key);
    }

    int kh_resize(khint_t new_n_buckets)
    {
        khint32_t[] new_used;
        khint_t j = 0, x = new_n_buckets, n_buckets, new_bits, new_mask;
        while ((x >>= 1) != 0)
            ++j;
        if (new_n_buckets & (new_n_buckets - 1))
            ++j;
        new_bits = j > 2 ? j : 2;
        new_n_buckets = 1U << new_bits;
        if (this.count > (new_n_buckets >> 1) + (new_n_buckets >> 2))
            return 0; /* requested size is too small */
        new_used = new khint32_t[__kh_fsize(new_n_buckets)];
        // memset(new_used, 0, __kh_fsize(new_n_buckets) * khint32_t.sizeof);
        if (!new_used.ptr)
            return -1; /* not enough memory */
        n_buckets = this.keys.ptr ? 1U << this.bits : 0U;
        if (n_buckets < new_n_buckets)
        { /* expand */
            this.keys.length = new_n_buckets;
            // if (!new_keys) { kfree(new_used); return -1; }
            // this.keys = new_keys;
        } /* otherwise shrink */
        new_mask = new_n_buckets - 1;
        for (j = 0; j != n_buckets; ++j)
        {
            Bucket key;
            if (!__kh_used(this.used, j))
                continue;
            key = this.keys[j];
            __kh_set_unused(this.used, j);
            while (1)
            { /* kick-out process; sort of like in Cuckoo hashing */
                khint_t i;

                /// if using caching, don't rehash key
                static if (cached)
                    i = __kh_h2b(key.hash, new_bits);
                else
                    i = __kh_h2b(__hash_func(key.key), new_bits);

                while (__kh_used(new_used, i))
                    i = (i + 1) & new_mask;
                __kh_set_used(new_used, i);
                if (i < n_buckets && __kh_used(this.used, i))
                { /* kick out the existing element */
                    {
                        Bucket tmp = this.keys[i];
                        this.keys[i] = key;
                        key = tmp;
                    }
                    __kh_set_unused(this.used, i); /* mark it as deleted in the old hash table */
                }
                else
                { /* write the element and jump out of the loop */
                    this.keys[i] = key;
                    break;
                }
            }
        }
        if (n_buckets > new_n_buckets) /* shrink the hash table */
            this.keys.length = new_n_buckets;
        // kfree(this.used); /* free the working space */
        this.used = new_used, this.bits = new_bits;
        return 0;
    }

    khint_t kh_putp(const(Bucket)* key, int* absent)
    {
        khint_t n_buckets, i, last, mask;
        n_buckets = this.keys.ptr ? 1U << this.bits : 0U;
        *absent = -1;
        if (this.count >= (n_buckets >> 1) + (n_buckets >> 2))
        { /* rehashing */
            if (this.kh_resize(n_buckets + 1U) < 0)
                return n_buckets;
            n_buckets = 1U << this.bits;
        } /* TODO: to implement automatically shrinking; resize() already support shrinking */
        mask = n_buckets - 1;

        /// if using caching, don't rehash key
        static if (cached)
            i = last = __kh_h2b((*key).hash, this.bits);
        else
            i = last = __kh_h2b(__hash_func((*key).key), this.bits);

        while (__kh_used(this.used, i) && !__hash_equal(this.keys[i], *key))
        {
            i = (i + 1U) & mask;
            if (i == last)
                break;
        }
        if (!__kh_used(this.used, i))
        { /* not present at all */
            this.keys[i] = *(cast(Bucket*) key);
            __kh_set_used(this.used, i);
            ++this.count;
            *absent = 1;
        }
        else
            *absent = 0; /* Don't touch this.keys[i] if present */
        return i;
    }

    khint_t kh_put(const(Bucket) key, int* absent)
    {
        return this.kh_putp(&key, absent);
    }

    int kh_del(khint_t i)
    {
        khint_t j = i, k, mask, n_buckets;
        if (this.keys == null)
            return 0;
        n_buckets = 1U << this.bits;
        mask = n_buckets - 1U;
        while (1)
        {
            j = (j + 1U) & mask;
            if (j == i || !__kh_used(this.used, j))
                break; /* j==i only when the table is completely full */

            /// if using caching, don't rehash key
            static if (cached)
                k = __kh_h2b(this.keys[j].hash, this.bits);
            else
                k = __kh_h2b(__hash_func(this.keys[j].key), this.bits);

            if ((j > i && (k <= i || k > j)) || (j < i && (k <= i && k > j)))
                this.keys[i] = this.keys[j], i = j;
        }
        __kh_set_unused(this.used, i);
        --this.count;
        return 1;
    }

    auto kh_bucket(khint_t x)
    {
        return this.keys[x];
    }

    auto kh_key(khint_t x)
    {
        return this.keys[x].key;
    }

    static if (kh_is_map)
    {
        auto kh_val(khint_t x)
        {
            return this.keys[x].val;
        }
    }

    auto kh_end() const
    {
        return this.kh_capacity();
    }

    auto kh_size()
    {
        return this.count;
    }

    auto kh_capacity() const
    {
        return this.keys.ptr ? 1U << this.bits : 0U;
    }

}

/** --- BEGIN OF HASH FUNCTIONS --- */
template kh_hash(T)
{
    pragma(inline, true)
    {
        auto kh_hash_func(T)(const(T) key)
                if (is(T == uint) || is(T == uint32_t) || is(T == khint32_t))
        {
            uint k = key;
            k += ~(k << 15);
            k ^= (k >> 10);
            k += (k << 3);
            k ^= (k >> 6);
            k += ~(k << 11);
            k ^= (k >> 16);
            return k;
        }

        auto kh_hash_func(T)(const(T) key)
                if (is(T == ulong) || is(T == uint64_t) || is(T == khint64_t))
        {
            ulong k = key;
            k = ~k + (k << 21);
            k = k ^ k >> 24;
            k = (k + (k << 3)) + (k << 8);
            k = k ^ k >> 14;
            k = (k + (k << 2)) + (k << 4);
            k = k ^ k >> 28;
            k = k + (k << 31);
            return cast(khint_t) k;
        }

        auto kh_hash_func(T)(const(T) key) if (is(T == uint128))
        {
            ulong k = key.toHash;
            k = ~k + (k << 21);
            k = k ^ k >> 24;
            k = (k + (k << 3)) + (k << 8);
            k = k ^ k >> 14;
            k = (k + (k << 2)) + (k << 4);
            k = k ^ k >> 28;
            k = k + (k << 31);
            return cast(khint_t) k;
        }

        khint_t kh_hash_str(const(char)* s)
        {
            khint_t h = cast(khint_t)*s;
            if (h)
                for (++s; *s; ++s)
                    h = (h << 5) - h + cast(khint_t)*s;
            return h;
        }

        auto kh_hash_func(T)(const(T)* key)
                if (is(T == char) || is(T == const(char)) || is(T == immutable(char)))
        {
            return kh_hash_str(key);
        }

        auto kh_hash_func(T)(const(T) key) if (isSomeString!T || isArray!T)
        {
            // rewrite __ac_X31_hash_string for D string/smart array
            if (key.length == 0)
                return 0;
            khint_t h = key[0];
            for (int i = 1; i < key.length; ++i)
                h = (h << 5) - h + cast(khint_t) key[i];
            return h;
        }

        auto kh_hash_func(T : Asdf)(const(T) key)
        {
            return kh_hash_func(key.data);
        }

        auto kh_hash_func(T : JSONValue)(const(T) key)
        {
            return (key.val).match!((bool x) => kh_hash_func!uint(x),
                    (long x) => kh_hash_func!ulong(cast(ulong) x),
                    (double x) => kh_hash_func!ulong(cast(ulong) x),
                    (const(char)[] x) => kh_hash_func!(const(char)[])(x));
        }

    } // end pragma(inline, true)
} // end template kh_hash

/// In order to take advantage of cached-hashes
/// our equality function will actually take the bucket type as opposed to just the key.
/// This allows it to access both the store hash and the key itself.
template kh_equal(T, bool cached)
{
    pragma(inline, true)
    {
        /// Assert that we are using a bucket type with key member
        static assert(hasMember!(T, "key"));

        /// Assert that we are using a bucket type with hash member if using hash-caching
        static if (cached)
            static assert(hasMember!(T, "hash"));

        bool kh_hash_equal(T)(const(T) a, const(T) b)
                if (isNumeric!(typeof(__traits(getMember, T, "key"))))
        {
            /// There is no benefit to caching hashes for integer keys (I think)
            static assert(cached == false, "No reason to cache hash for integer keys");
            return (a.key == b.key);
        }

        bool kh_hash_equal(T)(const(T) a, const(T) b)
                if (is(typeof(__traits(getMember, T, "key")) == uint128))
        {
            /// There is no benefit to caching hashes for integer keys (I think)
            static assert(cached == false, "No reason to cache hash for integer keys");
            return (a.key == b.key);
        }

        bool kh_hash_equal(T)(const(T)* a, const(T)* b)
                if (is(typeof(__traits(getMember, T, "key")) == char)
                    || is(typeof(__traits(getMember, T, "key")) == const(char))
                    || is(typeof(__traits(getMember, T, "key")) == immutable(char)))
        {
            /// If using hash-caching we check equality of the hashes first 
            /// before checking the equality of keys themselves 
            static if (cached)
                return (a.hash == b.hash) && (strcmp(a, b) == 0);
            else
                return (strcmp(a.key, b.key) == 0);
        }

        bool kh_hash_equal(T)(const(T) a, const(T) b)
                if (isSomeString!(typeof(__traits(getMember, T, "key"))))
        {
            /// If using hash-caching we check equality of the hashes first 
            /// before checking the equality of keys themselves 
            static if (cached)
                return (a.hash == b.hash) && (a.key == b.key);
            else
                return (a.key == b.key);
        }

        auto kh_hash_equal(T)(const(T) a, const(T) b)
                if (is(typeof(__traits(getMember, T, "key")) == Asdf))
        {
            return a.key.data == b.key.data;
        }

        bool kh_hash_equal(T)(const(T) a, const(T) b)
                if (is(typeof(__traits(getMember, T, "key")) == JSONValue))
        {
            static if (cached)
                return (a.hash == b.hash) && (a.key == b.key);
            else
                return a.key == b.key;
        }
    } // end pragma(inline, true)
} // end template kh_equal
/* --- END OF HASH FUNCTIONS --- */

unittest
{
    import std.stdio : writeln, writefln;

    writeln("khashl unit tests");

    // test: numeric key type must be unsigned
    assert(__traits(compiles, khashl!(int, int)) is false);
    assert(__traits(compiles, khashl!(uint, int)) is true);

    //    auto kh = khash!(uint, char).kh_init();

    //int absent;
    //auto k = khash!(uint, char).kh_put(kh, 5, &absent);
    ////khash!(uint, char).kh_value(kh, k) = 10;
    //kh.vals[k] = 'J';

    //    (*kh)[5] = 'J';
    //    writeln("Entry value:", (*kh)[5]);

    //    khash!(uint, char).kh_destroy(kh);

    auto kh = khashl!(uint, char)();
    kh[5] = 'J';
    assert(kh[5] == 'J');

    kh[1] = 'O';
    kh[99] = 'N';

    // test: foreach by key
    /*foreach(k; kh.byKey())
        writefln("Key: %s", k);*/
    import std.array : array;

    assert(kh.byKey().array == [1, 99, 5]);

    // test: byKey on Empty hash table
    auto kh_empty = khashl!(uint, char)(); // @suppress(dscanner.suspicious.unmodified)
    assert(kh_empty.byKey.array == []);

    // test: keytype string
    auto kh_string = khashl!(string, int)();
    kh_string["test"] = 5;
    assert(kh_string["test"] == 5);

    // test: valtype string
    auto kh_valstring = khashl!(uint, string)();
    kh_valstring[42] = "Adams";
    assert(kh_valstring[42] == "Adams");

    // test: require
    const auto fw = kh_string.require("flammenwerfer", 21);
    assert(*fw == 21);
}

unittest {
    import std.array : array;

    khashlSet!(string) a;
    khashlSet!(string) b;

    a.insert("test1");
    a.insert("test2");
    a.insert("test2");
    a.insert("test3");

    b.insert("test1");
    b.insert("test3");
    b.insert("test4");
    b.insert("test4");

    assert((a & b).byKey.array == ["test1", "test3"]);
    assert((a | b).byKey.array == ["test4", "test1", "test3", "test2"]);
    assert((a - b).byKey.array == ["test2"]);
    assert((b - a).byKey.array == ["test4"]);
}