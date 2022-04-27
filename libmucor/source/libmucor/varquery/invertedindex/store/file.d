module libmucor.varquery.invertedindex.store.file;
import htslib.hfile;
import htslib.hts;
import htslib.bgzf;
import htslib.hts_log;
import dhtslib.memory;
import core.stdc.stdio: SEEK_SET, SEEK_CUR, SEEK_END, fprintf, stderr;
import core.stdc.stdlib: malloc, free;
import core.stdc.string: strerror;
import core.stdc.errno: errno;
import std.string: fromStringz;
import std.format: format;
import std.stdio;
import libmucor: global_pool;

struct StoreFile { 
    char[] mode;
    char[] fn;
    BGZF * bgzf;
    bool eof;

    this(string fn, string mode){
        /// allocate mode string
        this.mode = (cast(char*)malloc(mode.length + 1))[0..mode.length + 1];
        this.mode[0..mode.length] = mode[];
        this.mode[this.mode.length - 1] = '\0';

        /// allocate file string
        this.fn = (cast(char*)malloc(fn.length + 1))[0..fn.length + 1];
        this.fn[0..fn.length] = fn[];
        this.fn[this.fn.length - 1] = '\0';

        auto hf = hopen(this.fn.ptr, this.mode.ptr);
        if(!hf){
            throw new Exception(format("Either file not found or error opening: %s, %s", fromStringz(this.fn.ptr), fromStringz(strerror(errno))));
        }
        this.bgzf = bgzf_hopen(hf, this.mode.ptr);
        if(global_pool && this.bgzf.is_write){
            bgzf_thread_pool(this.bgzf, cast(htslib.bgzf.hts_tpool*) global_pool.pool, global_pool.qsize);
        }
        assert(this.bgzf);
    }

    void close() {
        if(this.bgzf) {
            free(this.fn.ptr);
            free(this.mode.ptr);
            bgzf_close(this.bgzf);
        }
        this.bgzf = null;
    }

    ulong tell() {
        return bgzf_tell(this.bgzf);
    }

    void seek(ulong pos) {
        auto err = bgzf_seek(this.bgzf, pos, SEEK_SET);
        if(err < 0) fprintf(stderr, "Error seeking file %s\n", fn.ptr);
    }

    void seekFromCur(ulong pos) {
        auto err = bgzf_seek(this.bgzf, pos, SEEK_CUR);
        if(err < 0) fprintf(stderr, "Error seeking file %s\n", fn.ptr);
    }

    void seekToEnd() {
        auto err = bgzf_seek(this.bgzf, 0, SEEK_END);
        if(err < 0) fprintf(stderr, "Error seeking file %s\n", fn.ptr);
    }

    void readRaw(ubyte[] buf) {
        long bytes = bgzf_read(this.bgzf, buf.ptr, buf.length);
        if(bytes < 0) fprintf(stderr, "Error reading data for file %s\n", fn.ptr);
        if(bytes == 0) this.eof = true;
    }
    
    void writeRaw(ubyte[] buf) {
        long bytes = bgzf_write(this.bgzf, buf.ptr, buf.length);
        if(bytes < 0) fprintf(stderr, "Error writing data for file %s\n", fn.ptr);
    }
    
}

