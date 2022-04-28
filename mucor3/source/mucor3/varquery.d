module mucor3.varquery;
import std.stdio;
import std.datetime.stopwatch : StopWatch;
import std.exception : enforce;
import std.algorithm: map, joiner;
import std.range;
import std.conv:to;
import libmucor.error;

import asdf : deserializeAsdf = deserialize, parseJsonByLine, Asdf;
import libmucor.varquery;

void query_main(string[] args){
    StopWatch sw;
    sw.start;
 
    InvertedIndex * idx = new InvertedIndex(args[$-2], false);
    // auto idxs = idx.fields[args[1]].filter(args[2..$]);
    // float[] range = [args[2].to!float,args[3].to!float];
    log_info(__FUNCTION__, "Time to load index: %s",sw.peek.total!"seconds"," seconds");
    log_info(__FUNCTION__, "%d records in index",idx.recordMd5s.length);
    sw.stop;
    sw.reset;
    sw.start;
    foreach(obj;args[0..$-1].map!(x => File(x).byChunk(4096).parseJsonByLine).joiner.query(idx, args[$-1])){
        writeln(obj);
    }
    log_info(__FUNCTION__,"Time to query/filter records: ",sw.peek.total!"seconds"," seconds");
    // parseQuery("(key1:val1 AND key2:(val2 OR val3 OR val4) AND key3:1-2) OR key4:val4 OR key5:(val5 AND val6)",idx);
    // parseQuery("key1:val1 AND key2:(val2 OR val3) AND key4:val4 AND key5:(val5 OR val6)",idx);
}

void index_main(string[] args){

    StopWatch sw;
    
    args[0..$-1].map!(x => File(x).byChunk(4096).parseJsonByLine).joiner.index(args[$-1]);

}

