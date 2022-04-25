module libmucor;

public import libmucor.vcfops;
public import libmucor.jsonlops;
public import libmucor.khashl;
public import libmucor.wideint;

import htslib.hts: htsThreadPool;
import htslib.thread_pool;
import std.parallelism: totalCPUs;

__gshared htsThreadPool * global_pool;

void setup_global_pool(int threads = -1) {
    if(threads == -1){
        threads = cast(int)totalCPUs;
    }
    global_pool = new htsThreadPool(null, 0);
    global_pool.pool = hts_tpool_init(threads);
}