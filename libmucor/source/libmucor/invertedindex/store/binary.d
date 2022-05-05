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
import core.stdc.string : memset;
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
            assert((this.file.tell & 0xFFFF) == 0);
            return this.file.tell | (this.bufLen & 0xFFFF);
        } else {
            return this.file.tell | (this.bufferCursor & 0xFFFF);
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
        log_debug(__FUNCTION__, "Loading next block from offset %x", seekToPos & 0xFFFFFFFFFFFF0000);
        assert(!isWrite);
        if((this.file.tell & 0xFFFFFFFFFFFF0000) != (seekToPos & 0xFFFFFFFFFFFF0000)){
            this.file.seek(seekToPos & 0xFFFFFFFFFFFF0000);
            this.isLastBlock = false;
            this.loadToBuffer;
        }
        this.bufferCursor = seekToPos & 0xFFFF;
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
            // log_err(__FUNCTION__, )
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
        this.loadToBuffer(seekToPos);
        return this.readBytes(numBytes);
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
                        front ~= this.store.read(this.range.front.offset);
                        foreach(i; 1..this.range.front.length)
                            front ~= this.store.read();
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
        /// with position offset
        T read(ulong pos, ulong length) {
            static if (isSomeString!T)
            {
                auto buf = this.readBytes(length, pos);
                auto s = new char[length];
                s[] = cast(char[])buf[];
                return cast(T) s;
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
                        auto off = this.range.front;
                        front = this.store.read(off.offset, off.length);
                        this.range.popFront;
                    }
                }
            }
            return GetFromOffsetsVariable(&this, offsets);
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

    static if(isSomeString!T) {
        ulong writeString(T val)
        {
            auto valSize = val.sizeSerialized;
            if (this.capacity < valSize) 
                log_err(__FUNCTION__, "String was greater than 65280 bytes");
            else if (this.vacancies < valSize) {
                /// NUL fill rest of buffer to complete bgzf block
                memset(this.buffer + this.bufLen, '\0', this.vacancies);
                this.bufLen += this.vacancies;
                assert(this.bufLen == this.capacity);
                this.flush;
                auto ret = this.tell;
                auto p = this.buffer + this.bufLen;
                /// serialize
                val.serialize(p);

                this.bufLen += valSize;
                return ret;
            } else {
                auto p = this.buffer + this.bufLen;
                auto ret = this.tell;
                val.serialize(p);
                this.bufLen += valSize;
                return ret;
            }
            return 0;
        }
    } else {
        /// write value
        void write(T val)
        {
            if(this.bufLen == this.capacity) this.flush;
            auto valSize = val.sizeSerialized;
            auto p = this.buffer + this.bufLen;
            val.serialize(p);
            this.bufLen += valSize;
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
    OffsetTuple[] offs;
    {

        auto store = new StringStore("/tmp/string.store", "wb");
        offs ~= OffsetTuple(store.writeString("HERES a bunch of text"), 21);

        offs ~= OffsetTuple(store.writeString("HERES a bunch of text plus some extra"), 37);

        offs ~= OffsetTuple(store.writeString("addendum: some more text"), 24);

        offs ~= OffsetTuple(store.writeString("note: text"), 10);
        store.close;

    }

    {

        import std.range;
        auto store = new StringStore("/tmp/string.store", "rb");
        writeln(offs);
        writeln(store.getFromOffsets(offs).array);
        assert(store.getFromOffsets(offs).array == [
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
    ulong[] offsets;
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
        
        foreach(i; 0..8160/2) {
            offsets ~= store.tell;
            store.write(i);    
        }
        assert(store.tell == 4080 * 8);
        foreach(i; 0..8160/2) {
            offsets ~= store.tell;
            store.write(i);    
        }
        assert(store.tell == 65280);
        foreach(i; 0..8160) {
            offsets ~= store.tell;
            store.write(i);    
        }
        assert(store.tell != 65280);
        assert((store.tell & 0xFFFF) == 65280);
        foreach(i; 0..435) {
            offsets ~= store.tell;
            store.write(i);    
        }
        assert((store.tell & 0xFFFF) == 435*8);
        off = store.tell;
        offsets ~= store.tell;
        store.write(435);
        store.close;
    }
    {

        auto store = new UlongStore("/tmp/ulong_test.store", "rb");
        auto r = store.read(off);
        writeln(r);
        assert(r == 435);
    }
    {

        auto store = new UlongStore("/tmp/ulong_test.store", "rb");
        import std.range: iota;
        import std.conv: to;
        ulong[] res = (iota(0, 8160/2).array ~ iota(0, 8160/2).array ~ iota(0, 8160).array ~ iota(0, 436).array).to!(ulong[]);
        assert(store.getAll().array == res);
    }

    {

        auto store = new UlongStore("/tmp/ulong_test.store", "rb");
        import std.range: iota;
        import std.conv: to;
        ulong[] res = (iota(0, 8160/2).array ~ iota(0, 8160/2).array ~ iota(0, 8160).array ~ iota(0, 436).array).to!(ulong[]);
        assert(store.getFromOffsets(offsets).array == res);
    }
    {

        auto store = new UlongStore("/tmp/ulong_test.store", "rb");
        import std.range: iota;
        import std.conv: to;
        auto offs = [OffsetTuple(offsets[0], 8160), OffsetTuple(offsets[8160], 8000), OffsetTuple(offsets[8160+8000], 160+436)]; 
        ulong[][] res = 
            [
                (iota(0, 8160/2).array ~ iota(0, 8160/2).array).to!(ulong[]), 
                iota(0, 8000).array.to!(ulong[]),
                (iota(8000, 8160).array ~ iota(0, 436).array).to!(ulong[]), 
            ];
        assert(store.getArrayFromOffsets(offs).array == res);
    }
}