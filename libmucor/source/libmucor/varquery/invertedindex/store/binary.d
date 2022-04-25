module libmucor.varquery.invertedindex.store.binary;

import dhtslib.file;
import htslib.hts;
import htslib.bgzf;
import htslib.hfile;
import htslib.hts_log;
import libmucor.wideint;
import libmucor.varquery.invertedindex.jsonvalue;
import libmucor.varquery.invertedindex.metadata;
import libmucor.varquery.invertedindex.store.file;
import std.traits;
import std.stdio: File;
import htslib.hts_log;
import libmucor.hts_endian;
import core.sync.mutex: Mutex;
import core.atomic;
import core.stdc.stdlib: malloc, free;
import core.stdc.stdio: fprintf, stderr;

alias UlongStore = BinaryStore!ulong;
alias LongStore = BinaryStore!long;
alias DoubleStore = BinaryStore!double;
alias StringStore = BinaryStore!(const(char)[]);
alias MD5Store = BinaryStore!uint128;
alias KeyMetaStore = BinaryStore!KeyMetaData;
alias JsonMetaStore = BinaryStore!JsonKeyMetaData;

struct SmallsIds {
    uint128 key;
    ulong[] ids;

    ubyte[] serialize() {
        ubyte[] ret = new ubyte[16 + 8 + (this.ids.length*8)];
        auto p = ret.ptr;
        u64_to_le(this.key.hi, p);
        p += 8;
        u64_to_le(this.key.lo, p);
        p += 8;
        u64_to_le(this.ids.length, p);
        p += 8;
        foreach(id; ids){
            u64_to_le(id, p);
            p += 8;
        }
        return ret;
    }
}

/// Universal buffered type store
/// 
/// BUFFER NOT THREAD SAFE
struct BinaryStore(T) {
    /// file backing store
    StoreFile file;

    /// is file writeable
    bool isWrite;

    /// buffer for reading and writing
    ubyte * buffer;

    ulong bufLen;

    ulong bufSize = 65536;

    // invariant {
    //     assert(this.bufLen <= this.bufSize);
    // }

    this(string fn, string mode) {
        this.buffer = cast(ubyte*)malloc(this.bufSize);
        this.bufLen = 0;
        this.file = StoreFile(fn, mode);
        foreach(c; mode) {
            if(c =='w' || c == 'a')
                this.isWrite = true;
        }       
    }
    
    // @disable this(this);
    // this(this) {
    //     file = file;
    // }

    // ~this() {
    //     this.close;
    // }

    void close() {
        import std.format;
        
        if(isWrite && this.file.bgzf) {
            hts_log_debug(__FUNCTION__, format("closing %s", this.file.fn.ptr));
            this.flush;
        }
        this.bufLen = 0;
        if(this.buffer) {
            free(this.buffer);
        }
        this.file.close;
        // this.buffer = null;
    }

    /// flush buffer and reset
    void flush() {
        if(this.bufLen > 0)
            this.file.writeRaw(cast(ubyte[])this.buffer[0..this.bufLen]);
        this.bufLen = 0;
    }

    /// add these bytes to the buffer
    void bufferBytes(ubyte[] bytes) {
        assert(bytes.length + this.bufLen <= this.bufSize);
        this.buffer[this.bufLen..this.bufLen + bytes.length] = bytes[];
        this.bufLen = this.bufLen + bytes.length;
    }

    ulong tell(){
        return this.file.tell + this.bufLen;
    }

    void seek(ulong pos){
        return this.file.seek(pos);
    }

    bool isEOF(){
        return this.file.eof;
    }

    /// write raw byte array and return offset
    void writeRaw(ubyte[] bytes) {
        if(!isWrite) {
            hts_log_error(__FUNCTION__, "File is not writeable");
            return;
        }
        /// if buffer full, write and reset
        if(this.bufLen >= this.bufSize) {
            this.flush;
        }

        /// if data bigger than buffer,
        /// flush buffer
        /// write data in 4kb chunks
        /// then buffer remaining
        if(bytes.length > this.bufSize) {
            this.flush;
            foreach(i; 0..(bytes.length / this.bufSize)) {
                this.file.writeRaw(bytes[0..this.bufSize]);
                bytes = bytes[this.bufSize..$];
            }
            this.bufferBytes(bytes);
        } else if(bytes.length + this.bufLen > this.bufSize) {
            auto part = this.bufSize - this.bufLen;
            this.bufferBytes(bytes[0 .. part]);
            this.flush;
            this.bufferBytes(bytes[part .. $]);
        }else {
            this.bufferBytes(bytes);
        }
        static if(isSomeString!T){
            free(bytes.ptr);
        }
    }

    auto getItemAsBytes(T item)
    {   
        static if(isIntegral!T || isFloatingPoint!T || is(T == uint128)){
            ubyte[T.sizeof] buf;
            static if(is(T == ulong)){
                u64_to_le(item, buf.ptr);
            }else static if(is(T == long)){
                i64_to_le(item, buf.ptr);
            }else static if(is(T == double)){
                double_to_le(item, buf.ptr);
            } else static if(is(T == uint128)){
                u64_to_le(item.hi, buf.ptr);
                u64_to_le(item.lo, buf.ptr+8);
            }
            return buf;
        } else static if(is(T == JsonKeyMetaData)){
            return item.serialize;
        } else static if(is(T == KeyMetaData)){
            return item.serialize; 
        } else static if(isSomeString!T){
            ubyte[] buf = (cast(ubyte*)malloc(8 + item.length))[0..8 + item.length];
            u64_to_le(item.length, buf.ptr);
            buf[8..$] = cast(ubyte[])item;
            return buf;
        } else static if(is(T == SmallsIds)) {
            return item.serialize;
        } else {
            static assert(0, "Not a valid store type!");
        }
    }
    
    void write(T val)
    {
        this.writeRaw(this.getItemAsBytes(val));
    }

    void write(T[] vals) {
        foreach(item; vals){
            this.writeRaw(this.getItemAsBytes(item));
        }
    }

    void readRaw(ubyte[] buf) {
        return this.file.readRaw(buf);
    }

    T read() {
        static if(isIntegral!T || isFloatingPoint!T || is(T == uint128)){
            ubyte[T.sizeof] buf;
            this.file.readRaw(buf);
            static if(is(T == ulong)){
                return le_to_u64(buf.ptr);
            }else static if(is(T == long)){
                return le_to_i64(buf.ptr);
            }else static if(is(T == double)){
                return le_to_double(buf.ptr);
            } else static if(is(T == uint128)){
                uint128 ret;
                ret.hi = le_to_u64(buf.ptr);
                ret.lo = le_to_u64(buf.ptr+8);
                return ret;
            }
        }else static if(is(T == JsonKeyMetaData)){
            ubyte[48] buf;
            this.file.readRaw(buf);
            return JsonKeyMetaData(buf);
        } else static if(is(T == KeyMetaData)){
            ubyte[32] buf;
            this.file.readRaw(buf);
            return KeyMetaData(buf); 
        } else static if(isSomeString!T){
            ubyte[8] buf;
            this.file.readRaw(buf);
            auto length = le_to_u64(buf.ptr);
            char[] s = (cast(char*)malloc(length))[0..length];
            this.file.readRaw(cast(ubyte[])s);
            return cast(T)s;
        } else static if(is(T == SmallsIds)) {
            ubyte[24] buf;
            this.file.readRaw(buf);
            SmallsIds ret;
            ret.key.hi = le_to_u64(buf.ptr);
            ret.key.lo = le_to_u64(buf.ptr + 8);
            ret.ids.length = le_to_u64(buf.ptr + 16);
            ubyte[] data = new ubyte[ret.ids.length*8];
            this.file.readRaw(data);
            auto p = data.ptr;
            foreach (ref id; ret.ids)
            {
                le_to_u64(p);   
                p += 8;
            }
            return ret;
        } else {
            static assert(0, "Not a valid store type!");
        }
    }

    T readFromPosition(ulong pos) {
        this.file.seek(pos);
        return this.read();
    }

    auto getAll() {
        struct GetAll {
            BinaryStore!T * store;
            T front;
            bool empty;
            this(BinaryStore!T * s) {
                this.store = s;
                this.store.file.seek(0);
                this.popFront;
            }

            void popFront() {
                if(!this.empty) {
                    front = this.store.read;
                }
                this.empty = this.store.isEOF;
                if(this.empty) {
                    this.store.file.seek(0);
                    this.store.file.eof = false;
                }
            }

        }
        return GetAll(&this);
    }
}

unittest {
    import std.stdio;
    import std.array: array;
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
        assert(store.getAll().array == ["HERES a bunch of text", "HERES a bunch of text plus some extra", "addendum: some more text", "note: text"]);
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
        assert(store.getAll().array == [KeyMetaData(uint128(0), 1, 3), KeyMetaData(uint128(1), 3, 5), KeyMetaData(uint128(2), 7, 8)]);
    }

    {
        
        auto store = new JsonMetaStore("/tmp/json.store", "wb");
        store.write(JsonKeyMetaData(uint128(0), 0, 0, 1, 3));
        assert(store.bufLen == 48);
        store.write(JsonKeyMetaData(uint128(1), 1, 2, 3, 5));
        assert(store.bufLen == 96);
        store.write(JsonKeyMetaData(uint128(2), 3, 0 ,7, 8));
        assert(store.bufLen == 144);
        store.close;   
    }

    {
        auto store = new JsonMetaStore("/tmp/json.store", "rb");
        assert(store.getAll().array == [JsonKeyMetaData(uint128(0), 0, 0, 1, 3), JsonKeyMetaData(uint128(1), 1, 2, 3, 5), JsonKeyMetaData(uint128(2), 3, 0, 7, 8)]);
    }

}