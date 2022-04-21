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
import htslib.hts_endian;
import core.sync.mutex: Mutex;
import core.atomic;
import core.stdc.stdlib: calloc, free;

alias UlongStore = BinaryStore!ulong;
alias LongStore = BinaryStore!long;
alias DoubleStore = BinaryStore!double;
alias StringStore = BinaryStore!(const(char)[]);
alias MD5Store = BinaryStore!uint128;
alias KeyMetaStore = BinaryStore!KeyMetaData;
alias JsonMetaStore = BinaryStore!JsonKeyMetaData;

/// Universal buffered type store
/// 
/// BUFFER NOT THREAD SAFE
struct BinaryStore(T) {
    /// file backing store
    StoreFile file;

    /// is file writeable
    bool isWrite;

    /// buffer for reading and writing
    ubyte[] buffer;

    invariant {
        assert(this.buffer.length <= 65_536);
    }

    this(string fn, string mode) {
        this.buffer.reserve(65_536);
        this.file = StoreFile(fn, mode);
        foreach(c; mode) {
            if(c =='w')
                this.isWrite = true;
        }       
    }
    
    // @disable this(this);
    this(this) {
        file = file;
    }

    // ~this() {
    //     this.close;
    // }

    void close() {
        import std.format;
        
        if(isWrite && this.file.bgzf.getRef) {
            hts_log_debug(__FUNCTION__, format("closing %s", this.file.fn));
            this.flush;
        }
        this.file = StoreFile.init;
    }

    /// flush buffer and reset
    void flush() {
        if(this.buffer.length > 0)
            this.file.writeRaw(cast(ubyte[])this.buffer[0..this.buffer.length]);
        this.buffer.length = 0;
    }

    /// add these bytes to the buffer
    void bufferBytes(ubyte[] bytes) {
        assert(bytes.length + this.buffer.length <= 65_536);
        this.buffer ~= bytes[];
    }

    ulong tell(){
        return this.file.tell + this.buffer.length;
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
        if(this.buffer.length == 65_536) {
            this.flush;
        }

        /// if data bigger than buffer,
        /// flush buffer
        /// write data in 4kb chunks
        /// then buffer remaining
        if(bytes.length > 65_536) {
            this.flush;
            foreach(i; 0..(bytes.length / 65_536)) {
                this.file.writeRaw(bytes[0..65_536]);
                bytes = bytes[65_536..$];
            }
            this.bufferBytes(bytes);
        } else {
            this.bufferBytes(bytes);
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
            ubyte[] buf = new ubyte[8 + item.length];
            u64_to_le(item.length, buf.ptr);
            buf[8..$] = cast(ubyte[])item;
            return buf;
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
            char[] s;
            s.length = length;
            this.file.readRaw(cast(ubyte[])s);
            return cast(T)s;
        } else {
            static assert(0, "Not a valid store type!");
        }
    }

    T readFromPosition(ulong pos) {
        this.file.seek(pos);
        return this.read();
    }

    T[] getAll() {
        T[] ret;
        do {
            ret ~= this.read;
        } while(!this.isEOF);
        this.file.seek(0);
        this.file.eof = false;
        return ret[0..$-1];
    }
}

unittest {
    import std.stdio;
    {
        
        auto store = new UlongStore("/tmp/ulong.store", "wb");
        store.write(1);
        assert(store.buffer.length == 8);
        store.write(2);
        assert(store.buffer.length == 16);
        store.write(3);
        assert(store.buffer.length == 24);   
        store.close;
    }

    {
        auto store = new UlongStore("/tmp/ulong.store", "rb");
        assert(store.getAll() == [1, 2, 3]);
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
        assert(store.getAll() == ["HERES a bunch of text", "HERES a bunch of text plus some extra", "addendum: some more text", "note: text"]);
    }

    {
        auto store = new KeyMetaStore("/tmp/keymeta.store", "wb");
        store.write(KeyMetaData(uint128(0), 1, 3));
        assert(store.buffer.length == 32);
        store.write(KeyMetaData(uint128(1), 3, 5));
        assert(store.buffer.length == 64);
        store.write(KeyMetaData(uint128(2), 7, 8));
        assert(store.buffer.length == 96);   
        store.close;
    }

    {
        auto store = new KeyMetaStore("/tmp/keymeta.store", "rb");
        assert(store.getAll() == [KeyMetaData(uint128(0), 1, 3), KeyMetaData(uint128(1), 3, 5), KeyMetaData(uint128(2), 7, 8)]);
    }

    {
        
        auto store = new JsonMetaStore("/tmp/json.store", "wb");
        store.write(JsonKeyMetaData(uint128(0), 0, 0, 1, 3));
        assert(store.buffer.length == 48);
        store.write(JsonKeyMetaData(uint128(1), 1, 2, 3, 5));
        assert(store.buffer.length == 96);
        store.write(JsonKeyMetaData(uint128(2), 3, 0 ,7, 8));
        assert(store.buffer.length == 144);
        store.close;   
    }

    {
        auto store = new JsonMetaStore("/tmp/json.store", "rb");
        assert(store.getAll() == [JsonKeyMetaData(uint128(0), 0, 0, 1, 3), JsonKeyMetaData(uint128(1), 1, 2, 3, 5), JsonKeyMetaData(uint128(2), 3, 0, 7, 8)]);
    }

}