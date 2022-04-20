module libmucor.varquery.invertedindex.store.file;
import htslib.hfile;
import htslib.hts;
import htslib.bgzf;
import htslib.hts_log;
import dhtslib.memory;
import core.stdc.stdio: SEEK_SET, SEEK_CUR;
import std.format: format;

struct StoreFile {
    char[] mode;
    char[] fn;
    Bgzf bgzf;

    this(string fn, string mode){
        this.fn = fn.dup ~ '\0';
        this.mode = mode.dup ~ '\0';

        auto hf = hopen(this.fn.ptr, this.mode.ptr);
        if(!hf){
            throw new Exception(format("File not found: %s", fn));
        }
        this.bgzf = Bgzf(bgzf_hopen(hf, this.mode.ptr));
    }

    ~this(){
        if(this.bgzf.getRef != null && this.bgzf.is_write)
            bgzf_flush(this.bgzf);
    }

    bool isEof(){
        return cast(bool)this.bgzf.last_block_eof;
    }

    ulong tell() {
        return bgzf_tell(this.bgzf);
    }

    void seek(ulong pos) {
        bgzf_seek(this.bgzf, pos, SEEK_SET);
    }

    void seekFromCur(ulong pos) {
        bgzf_seek(this.bgzf, pos, SEEK_CUR);
    }

    void readRaw(ubyte[] buf) {
        long bytes = bgzf_read(this.bgzf, buf.ptr, buf.length);
        if(bytes < 0) hts_log_error(__FUNCTION__, "Error reading data");
    }
    

    void writeRaw(ubyte[] buf) {
        long bytes = bgzf_write(this.bgzf, buf.ptr, buf.length);
        if(bytes < 0) hts_log_error(__FUNCTION__, "Error writing data");
    }
    
}

