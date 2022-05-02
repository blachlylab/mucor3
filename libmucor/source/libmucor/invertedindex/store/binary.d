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
import std.typecons: Tuple;
import std.range;

alias UlongStore = BinaryStore!ulong;
alias LongStore = BinaryStore!long;
alias DoubleStore = BinaryStore!double;
alias StringStore = BinaryStore!(const(char)[]);
alias MD5Store = BinaryStore!uint128;
alias KeyMetaStore = BinaryStore!KeyMetaData;
alias JsonMetaStore = BinaryStore!JsonKeyMetaData;

alias OffsetTuple = Tuple!(ulong, "offset", ulong, "length");

/// Universal buffered type store
/// 
/// NOT THREAD SAFE
struct BinaryStore(T)
{
    /// file backing store
    StoreFile file;

    /// is file writeable
    bool isWrite;

    /// malloc'd buffer for reading and writing
    ubyte* buffer;

    /// buffer capacity in bytes, should never change
    ulong capacity;

    /// if writing, it is the length of the buffered data
    /// if reading, it equals capacity unless at eof
    ulong bufLen;

    /// when reading this tells us at what point we are in the buffer
    /// should never be > buflen
    ulong bufferCursor;

    /// have we read the last block of data
    bool isLastBlock;
    
    long lastBlockAddress;

    invariant {
        assert(this.bufLen <= this.capacity);
    }

    /// open for reading or writing
    this(string fn, string mode)
    {
        this.capacity = BGZF_BLOCK_SIZE;
        /// allocate buffer
        this.buffer = cast(ubyte*) malloc(this.capacity);
        this.file = StoreFile(fn, mode);
        foreach (c; mode)
        {
            if (c == 'w' || c == 'a')
                this.isWrite = true;
        }
    }

    /// close underlying file and free buffer
    void close()
    {
        import std.format;

        if (isWrite && this.file.bgzf)
        {
            // log_debug(__FUNCTION__, "closing %s", this.file.fn);
            this.flush;
        }
        this.bufLen = 0;
        this.bufferCursor = 0;
        if (this.buffer)
        {
            free(this.buffer);
        }
        this.file.close;
        // this.buffer = null;
    }

    /// position of bufferCursor/buffer end with respect to file tell
    /// if writing, reports file tell + bufferlen (when writing file tell is always behind)
    /// if reading, reports file tell - bufferCursor (when reading file tell is always ahead)
    auto tell() {
        if(isWrite) {
            return (this.file.bgzf.block_address << 16) | (this.bufLen & 0xFFFF);
        } else {
            return (this.lastBlockAddress << 16) | (this.bufferCursor & 0xFFFF);
        }
    }

    ///// READ ONLY METHODS /////

    /// position of beginning of buffer with respect to file tell
    auto tellBufferStart() {
        assert(!isWrite);
        return this.file.tell - this.bufLen;
    }

    /// position of beginning of buffer with respect to file tell
    auto tellBufferEnd() {
        assert(!isWrite);
        return this.file.tell;
    }

    /// is store EOF
    bool isEOF()
    {
        assert(!isWrite);
        return (this.file.checkIfEmpty && this.bufferCursor == this.bufLen) || (isLastBlock && this.bufferCursor == this.bufLen);
    }

    /// reset to beginning of file/store
    void reset(){
        assert(!isWrite);
        this.isLastBlock = false;
        this.bufferCursor = this.bufLen = 0;
        this.file.seekToStart;
    }

    /// load next block of data to buffer
    void loadToBuffer(){
        this.lastBlockAddress = this.file.bgzf.block_address;
        log_debug(__FUNCTION__, "Loading block of size %d", this.capacity);
        assert(!isWrite);
        // assert(!this.isLastBlock);
        // assert(!this.file.eof);

        /// read block
        auto len = this.file.readRaw(this.buffer[0..this.capacity]);

        this.bufLen = len;
        this.bufferCursor = 0;

        /// if didn't load full buffersize 
        /// underlying file is eof
        if(this.bufLen != this.capacity) {
            log_debug(__FUNCTION__, "Loaded last block of size %d", this.bufLen);
            // assert(this.file.eof);
            this.isLastBlock = true;
        }
        assert(this.bufLen <= this.capacity);
    }

    /// load next block of data from position
    void loadToBuffer(ulong seekToPos){
        log_debug(__FUNCTION__, "Loading next block from offset %x", seekToPos);
        assert(!isWrite);

        this.file.seek(seekToPos);
        this.isLastBlock = false;
        this.loadToBuffer;
    }

    /// read bytes from buffer or load next buffer
    /// moves bufferCursors
    ubyte[] readBytes(ulong numBytes)
    {
        assert(!isWrite);
        ubyte[] ret;
        if(this.bufferCursor == this.bufLen && this.isLastBlock)
            assert(0, "Trying to read from EOF file");

        /// if bufferCursor at capacity
        /// load data
        if(this.bufferCursor == this.capacity){
            this.loadToBuffer;
        }

        /// data requested is partially buffered
        /// seek back to beginning and load buffer
        if(this.bufferCursor + numBytes > this.bufLen){
            auto newPosition = this.file.tell - (this.bufLen - this.bufferCursor);
            this.file.seek(newPosition);
            this.loadToBuffer;
        }

        /// return slice of data from buffer
        ret = this.buffer[this.bufferCursor .. this.bufferCursor + numBytes];
        this.bufferCursor += numBytes;
        return ret;
    }

    /// read bytes from buffer or load next buffer
    /// moves bufferCursor
    ubyte[] readBytes(ulong numBytes, ulong seekToPos)
    {
        assert(!isWrite);
        /// if reading from before or after buffer offsets
        /// seek forward/back and load
        /// else data is (atleast partially) in buffer
        if(seekToPos < this.tellBufferStart || seekToPos > this.tellBufferEnd) {
            this.loadToBuffer(seekToPos);
            return this.readBytes(numBytes);
        } else {
            this.bufferCursor = this.tell - seekToPos;
            return this.readBytes(numBytes);
        }
    }

    /// if T is statically sized we don't need to provide 
    /// size when reading
    ///
    /// string and SmallsIds are variable length 
    /// so reading requires size to read
    static if (!isSomeString!T) {

        /// read type if statically sized
        T read()
        {
            auto buf = this.readBytes(T.sizeof);
            auto p = buf.ptr;
            return deserialize!T(p);
        }

        /// read type if statically sized
        /// with position offset
        T read(ulong pos)
        {
            auto buf = this.readBytes(T.sizeof, pos);
            auto p = buf.ptr;
            return deserialize!T(p);
        }

        /// read types from file if statically sized
        /// with range of position offsets
        auto getFromOffsets(R)(R offsets)
        if(is(ElementType!R == ulong))
        {
            struct GetFromOffsets
            {
                BinaryStore!T* store;
                T front;
                bool empty;
                Range range;
                this(BinaryStore!T* s, R range)
                {
                    this.store = s;
                    this.range = range;
                    this.popFront;
                }

                void popFront()
                {
                    this.empty = this.store.isEOF || this.range.empty;
                    if (this.empty)
                    {
                        this.store.reset();
                    } else {
                        front = this.store.read(this.range.front);
                        this.range.popFront;
                    }
                }
            }
            return GetFromOffsets(&this, offsets);
        }

        /// read types from file if statically sized
        /// with range of position offsets
        auto getArrayFromOffsets(R)(R offsets)
        if(is(ElementType!R == OffsetTuple))
        {
            struct GetArrayFromOffsets
            {
                BinaryStore!T* store;
                T[] front;
                bool empty;
                R range;
                this(BinaryStore!T* s, R range)
                {
                    this.store = s;
                    this.range = range;
                    this.popFront;
                }

                void popFront()
                {
                    this.empty = this.store.isEOF || this.range.empty;
                    if (this.empty)
                    {
                        this.store.reset();
                    } else {
                        import std.stdio;
                        log_debug(__FUNCTION__, "%d", this.range.front.length);
                        log_debug(__FUNCTION__, "%x", this.range.front.offset);
                        front = [];
                        foreach(i; 0..this.range.front.length)
                            front ~= this.store.read(this.range.front.offset + (i*T.sizeof));
                        this.range.popFront;
                    }
                }
            }
            return GetArrayFromOffsets(&this, offsets);
        }

        /// read types from file if statically sized
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
                    this.store.reset;
                    this.popFront;
                }

                void popFront()
                {
                    this.empty = this.store.isEOF;
                    if (this.empty)
                    {
                        this.store.reset();
                    } else 
                        front = this.store.read();
                    
                }
            }

            return GetAll(&this);
        }

    } else {

        /// read type if size is variable
        T read(ulong length) {
            static if (isSomeString!T)
            {
                auto buf = this.readBytes(length);
                auto s = new char[length];
                s[] = cast(char[])buf[];
                return cast(T) s;
            }
            else static if (is(T == SmallsIds))
            {
                auto buf = this.readBytes(24);
                SmallsIds ret;
                ret.key.hi = le_to_u64(buf.ptr);
                ret.key.lo = le_to_u64(buf.ptr + 8);
                auto len = le_to_u64(buf.ptr + 16);
                // import std.stdio;
                // import core.bitop: bswap;
                // writeln();
                // writefln("%016x", bswap(ret.key.hi));
                // writefln("%016x", bswap(ret.key.lo));
                // writefln("%016x", bswap(length));
                ret.ids = new ulong[len];
                buf = this.readBytes(8*length);
                auto p = buf.ptr;
                foreach (ref id; ret.ids)
                {
                    id = le_to_u64(p);
                    p += 8;
                }
                return ret;
            } else {
                static assert(0);
            }
        }

        /// read type if size is variable
        /// with position offset
        T read(ulong length, ulong pos) {
            static if (isSomeString!T)
            {
                auto buf = this.readBytes(length, pos);
                auto s = new char[length];
                s[] = cast(char[])buf[];
                return cast(T) s;
            }
            else static if (is(T == SmallsIds))
            {
                auto buf = this.readBytes(24, pos);
                SmallsIds ret;
                ret.key.hi = le_to_u64(buf.ptr);
                ret.key.lo = le_to_u64(buf.ptr + 8);
                auto len = le_to_u64(buf.ptr + 16);
                // import std.stdio;
                // import core.bitop: bswap;
                // writeln();
                // writefln("%016x", bswap(ret.key.hi));
                // writefln("%016x", bswap(ret.key.lo));
                // writefln("%016x", bswap(length));
                ret.ids = new ulong[len];
                buf = this.readBytes(8*length, pos);
                auto p = buf.ptr;
                foreach (ref id; ret.ids)
                {
                    id = le_to_u64(p);
                    p += 8;
                }
                return ret;
            } else {
                static assert(0);
            }
        }

        /// read types from file if variable
        /// with range of Tuple!(offset, length)
        auto getFromOffsets(R)(R offsets)
        if(is(ElementType!R == OffsetTuple))
        {
            struct GetFromOffsetsVariable
            {
                BinaryStore!T* store;
                T front;
                bool empty;
                Range range;
                this(BinaryStore!T* s, R range)
                {
                    this.store = s;
                    this.range = range;
                    this.popFront;
                }

                void popFront()
                {
                    this.empty = this.store.isEOF || this.range.empty;
                    if (this.empty)
                    {
                        this.store.reset();
                    } else {
                        auto off = this.range.front;
                        front = this.store.read(off.offset, off.length);
                        this.range.popFront;
                    }
                }
            }
            return GetFromOffsetsVariable(&this, offsets);
        }

        /// read types from file if variablely sized
        /// with range of lengths
        auto getAll(R)(R lengths)
        if((isInputRange!R || isArray!R) && is(ElementType!R == ulong))
        {
            struct GetAll
            {
                BinaryStore!T* store;
                T front;
                bool empty;
                R range;
                this(BinaryStore!T* s, R range)
                {
                    this.store = s;
                    this.store.file.seek(0);
                    this.range = range;
                    this.popFront;
                }

                void popFront()
                {
                    this.empty = this.store.isEOF || this.range.empty;
                    if (this.empty)
                    {
                        this.store.reset();
                    } else {
                        front = this.store.read(this.range.front);
                        this.range.popFront;
                    }
                    
                }
            }

            return GetAll(&this, lengths);
        }
    }

    ///// WRITE ONLY METHODS /////


    /// availiable bytes in buffer
    pragma(inline, true) auto vacancies()
    {
        return this.capacity - this.bufLen;
    }

    /// flush buffer and reset
    void flush()
    {
        assert(isWrite);
        if (this.bufLen > 0)
            this.file.writeRaw(cast(ubyte[]) this.buffer[0 .. this.bufLen]);
        this.bufLen = 0;
    }    

    /// write value
    void write(T val)
    {
        assert(isWrite);
        auto valSize = val.sizeSerialized;
        if (this.capacity < valSize)
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
            foreach (i; 0 .. (valSize / this.capacity))
            {
                this.file.writeRaw(p[0 .. this.capacity]);
                p += this.capacity;
            }

            /// buffer the extra
            auto remaining = valSize % this.capacity;
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
            this.buffer[this.bufLen .. this.capacity] = tmp[0 .. part];
            this.bufLen = this.capacity;
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

    /// write values
    void write(T[] vals)
    {
        assert(isWrite);
        foreach (item; vals)
        {
            this.write(item);
        }
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

        import std.range;
        auto store = new StringStore("/tmp/string.store", "rb");
        ulong[] lengths = [21, 37, 24, 10];
        assert(store.getAll(lengths).array == [
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

unittest {
    import std.stdio;
    import std.array : array;
    import htslib.bgzf;
    import libmucor.hts_endian;
    ulong off;
    {

        auto store = new UlongStore("/tmp/ulong_test.store", "wb");
        // auto block = store.file.bgzf.block_address;
        // writefln("First block %d",block);
        // foreach(i; 0..65_536) {
        //     ubyte[8] arr;
        //     u64_to_le(i, arr.ptr);
        //     store.file.writeRaw(arr);
        //     if(store.file.bgzf.block_address != block){
        //         block = store.file.bgzf.block_address;
        //         writefln("Block changed with #%d, new block %d",i, block);
        //     }
        // }

        foreach(i; 0..65_536) {
            store.write(i);    
        }
        foreach(i; 0..65_536) {
            store.write(i);    
        }
        off = store.tell;
        store.write(1);
        store.close;
    }
    {

        auto store = new UlongStore("/tmp/ulong_test.store", "rb");
        auto r = store.read(off);
        writeln(r);
        assert(r == 1);
    }
}