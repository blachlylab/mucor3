module libmucor.varquery.invertedindex.store.file;
import htslib.hfile;
import htslib.hts;
import htslib.bgzf;
import htslib.hts_log;
import dhtslib.memory;
import core.stdc.stdio: SEEK_SET, SEEK_CUR, SEEK_END, printf;
import core.stdc.stdlib: malloc, free;
import std.format: format;
import std.stdio;

struct StoreFile { 
    char[] mode;
    char[] fn;
    BGZF * bgzf;
    bool eof;

    @nogc: 
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
            printf("Either file not found or error opening: %s\n", fn.ptr);
        }
        this.bgzf = bgzf_hopen(hf, this.mode.ptr);
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
        if(err < 0) printf("Error seeking file %s\n", fn.ptr);
    }

    void seekFromCur(ulong pos) {
        auto err = bgzf_seek(this.bgzf, pos, SEEK_CUR);
        if(err < 0) printf("Error seeking file %s\n", fn.ptr);
    }

    void seekToEnd() {
        auto err = bgzf_seek(this.bgzf, 0, SEEK_END);
        if(err < 0) printf("Error seeking file %s\n", fn.ptr);
    }

    void readRaw(ubyte[] buf) {
        long bytes = bgzf_read(this.bgzf, buf.ptr, buf.length);
        if(bytes < 0) printf("Error reading data for file %s\n", fn.ptr);
        if(bytes == 0) this.eof = true;
    }
    
    void writeRaw(ubyte[] buf) {
        long bytes = bgzf_write(this.bgzf, buf.ptr, buf.length);
        if(bytes < 0) printf("Error writing data for file %s\n", fn.ptr);
    }
    
}

