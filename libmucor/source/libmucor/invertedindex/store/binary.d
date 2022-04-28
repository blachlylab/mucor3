module libmucor.invertedindex.store.binary;

import dhtslib.file;
import htslib.hts;
import htslib.bgzf;
import htslib.hfile;
import libmucor.error;
import libmucor.wideint;
import libmucor.jsonlops.jsonvalue;
import libmucor.invertedindex.metadata;
import libmucor.invertedindex.store.file;
import libmucor.invertedindex.store : serialize, deserialize, sizeDeserialized, sizeSerialized;
import std.traits;
import std.stdio : File;
import libmucor.hts_endian;
import core.sync.mutex : Mutex;
import core.atomic;
import core.stdc.stdlib : malloc, free;
import core.stdc.stdio : fprintf, stderr;

alias UlongStore = BinaryStore!ulong;
alias LongStore = BinaryStore!long;
alias DoubleStore = BinaryStore!double;
alias StringStore = BinaryStore!(const(char)[]);
alias MD5Store = BinaryStore!uint128;
alias KeyMetaStore = BinaryStore!KeyMetaData;
alias JsonMetaStore = BinaryStore!JsonKeyMetaData;

struct SmallsIds
{
    uint128 key;
    ulong[] ids;
}

/// Universal buffered type store
/// 
/// BUFFER NOT THREAD SAFE
struct BinaryStore(T)
{
    /// file backing store
    StoreFile file;

    /// is file writeable
    bool isWrite;

    /// buffer for reading and writing
    ubyte* buffer;

    ulong bufLen;

    ulong bufSize = 65536;

    // invariant {
    //     assert(this.bufLen <= this.bufSize);
    // }

    this(string fn, string mode)
    {
        this.buffer = cast(ubyte*) malloc(this.bufSize);
        this.bufLen = 0;
        this.file = StoreFile(fn, mode);
        foreach (c; mode)
        {
            if (c == 'w' || c == 'a')
                this.isWrite = true;
        }
    }

    pragma(inline, true) auto vacancies()
    {
        return this.bufSize - this.bufLen;
    }

    // @disable this(this);
    // this(this) {
    //     file = file;
    // }

    // ~this() {
    //     this.close;
    // }

    void close()
    {
        import std.format;

        if (isWrite && this.file.bgzf)
        {
            log_debug(__FUNCTION__, "closing %s", this.file.fn);
            this.flush;
        }
        this.bufLen = 0;
        if (this.buffer)
        {
            free(this.buffer);
        }
        this.file.close;
        // this.buffer = null;
    }

    /// flush buffer and reset
    void flush()
    {
        if (this.bufLen > 0)
            this.file.writeRaw(cast(ubyte[]) this.buffer[0 .. this.bufLen]);
        this.bufLen = 0;
    }

    ulong tell()
    {
        return this.file.tell + this.bufLen;
    }

    void seek(ulong pos)
    {
        return this.file.seek(pos);
    }

    bool isEOF()
    {
        return this.file.eof;
    }

    void write(T val)
    {
        auto valSize = val.sizeSerialized;
        if (this.bufSize < valSize)
        {
            /// allocate tmp array
            ubyte* tmp = cast(ubyte*) malloc(valSize);
            auto p = tmp;
            /// serialize
            val.serialize(p);
            /// flush existing buffer
            this.flush;

            /// write to disk in bufsize chunks
            p = tmp;
            foreach (i; 0 .. (valSize / this.bufSize))
            {
                this.file.writeRaw(p[0 .. this.bufSize]);
                p += this.bufSize;
            }

            /// buffer the extra
            auto remaining = valSize % this.bufSize;
            this.buffer[0 .. remaining] = p[0 .. remaining];
            this.bufLen = remaining;
            free(tmp);
        }
        else if (this.vacancies < valSize)
        {
            /// allocate tmp array
            ubyte* tmp = cast(ubyte*) malloc(valSize);
            auto p = tmp;
            /// serialize
            val.serialize(p);
            auto part = this.vacancies;
            /// fill buffer
            this.buffer[this.bufLen .. this.bufSize] = tmp[0 .. part];
            this.bufLen = this.bufSize;
            /// flush
            this.flush;
            /// buffer the extra
            this.buffer[0 .. valSize - part] = tmp[part .. valSize];
            this.bufLen = valSize - part;
            free(tmp);
        }
        else
        {
            auto p = this.buffer + this.bufLen;
            val.serialize(p);
            this.bufLen += valSize;
        }
    }

    void write(T[] vals)
    {
        foreach (item; vals)
        {
            this.write(item);
        }
    }

    void readRaw(ubyte[] buf)
    {
        return this.file.readRaw(buf);
    }

    T read()
    {
        static if (isSomeString!T)
        {
            ubyte[8] buf;
            this.file.readRaw(buf);
            auto length = sizeDeserialized!T(buf.ptr);
            auto s = new char[length];
            this.file.readRaw(cast(ubyte[]) s);
            return cast(T) s;
        }
        else static if (is(T == SmallsIds))
        {
            ubyte[24] buf;
            this.file.readRaw(buf);
            SmallsIds ret;
            ret.key.hi = le_to_u64(buf.ptr);
            ret.key.lo = le_to_u64(buf.ptr + 8);
            auto length = le_to_u64(buf.ptr + 16);
            // import std.stdio;
            // import core.bitop: bswap;
            // writeln();
            // writefln("%016x", bswap(ret.key.hi));
            // writefln("%016x", bswap(ret.key.lo));
            // writefln("%016x", bswap(length));
            ret.ids = new ulong[length];
            this.file.readRaw(cast(ubyte[]) ret.ids);
            foreach (ref id; ret.ids)
            {
                id = le_to_u64(cast(ubyte*)&id);
                // writefln("%016x", bswap(id));
            }
            return ret;
        }
        else
        {
            T ret;
            this.file.readRaw((cast(ubyte*)&ret)[0 .. T.sizeof]);
            auto p = cast(ubyte*)&ret;
            return deserialize!T(p);
        }
    }

    T readFromPosition(ulong pos)
    {
        this.file.seek(pos);
        return this.read();
    }

    auto getAll()
    {
        struct GetAll
        {
            BinaryStore!T* store;
            T front;
            bool empty;
            this(BinaryStore!T* s)
            {
                this.store = s;
                this.store.file.seek(0);
                this.popFront;
            }

            void popFront()
            {
                if (!this.empty)
                {
                    front = this.store.read;
                }
                this.empty = this.store.isEOF;
                if (this.empty)
                {
                    this.store.file.seek(0);
                    this.store.file.eof = false;
                }
            }

        }

        return GetAll(&this);
    }
}

unittest
{
    import std.stdio;
    import std.array : array;
    {

        auto store = new UlongStore("/tmp/ulong.store", "wb");
        store.write(1);
        assert(store.bufLen == 8);
        store.write(2);
        assert(store.bufLen == 16);
        store.write(3);
        assert(store.bufLen == 24);
        store.close;
    }

    {
        auto store = new UlongStore("/tmp/ulong.store", "rb");
        writeln(store.getAll().array);
        assert(store.getAll().array == [1, 2, 3]);
    }

    {

        auto store = new StringStore("/tmp/string.store", "wb");
        store.write("HERES a bunch of text");

        store.write("HERES a bunch of text plus some extra");

        store.write("addendum: some more text");

        store.write("note: text");
        store.close;

    }

    {

        auto store = new StringStore("/tmp/string.store", "rb");
        assert(store.getAll().array == [
                "HERES a bunch of text", "HERES a bunch of text plus some extra",
                "addendum: some more text", "note: text"
                ]);
    }

    {
        auto store = new KeyMetaStore("/tmp/keymeta.store", "wb");
        store.write(KeyMetaData(uint128(0), 1, 3));
        assert(store.bufLen == 32);
        store.write(KeyMetaData(uint128(1), 3, 5));
        assert(store.bufLen == 64);
        store.write(KeyMetaData(uint128(2), 7, 8));
        assert(store.bufLen == 96);
        store.close;
    }

    {
        auto store = new KeyMetaStore("/tmp/keymeta.store", "rb");
        assert(store.getAll().array == [
                KeyMetaData(uint128(0), 1, 3), KeyMetaData(uint128(1), 3, 5),
                KeyMetaData(uint128(2), 7, 8)
                ]);
    }

    {

        auto store = new JsonMetaStore("/tmp/json.store", "wb");
        store.write(JsonKeyMetaData(uint128(0), 0, 0, 1, 3));
        assert(store.bufLen == 48);
        store.write(JsonKeyMetaData(uint128(1), 1, 2, 3, 5));
        assert(store.bufLen == 96);
        store.write(JsonKeyMetaData(uint128(2), 3, 0, 7, 8));
        assert(store.bufLen == 144);
        store.close;
    }

    {
        auto store = new JsonMetaStore("/tmp/json.store", "rb");
        assert(store.getAll().array == [
                JsonKeyMetaData(uint128(0), 0, 0, 1, 3),
                JsonKeyMetaData(uint128(1), 1, 2, 3, 5),
                JsonKeyMetaData(uint128(2), 3, 0, 7, 8)
                ]);
    }

}
