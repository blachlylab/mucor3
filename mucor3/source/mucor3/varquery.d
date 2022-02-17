module mucor3.varquery;
import std.stdio;
import std.datetime.stopwatch : StopWatch;
import std.exception : enforce;
import std.algorithm: map;
import std.range;
import std.conv:to;

import asdf : deserializeAsdf = deserialize, parseJsonByLine, Asdf;
import libmucor.varquery;

void query_main(string[] args){
    StopWatch sw;
    sw.start;
 
    JSONInvertedIndex idx = JSONInvertedIndex(args[1]);
    // auto idxs = idx.fields[args[1]].filter(args[2..$]);
    // float[] range = [args[2].to!float,args[3].to!float];
    stderr.writeln("Time to load index: ",sw.peek.total!"seconds"," seconds");
    stderr.writefln("%d records in index",idx.recordMd5s.length);
    sw.stop;
    sw.reset;
    sw.start;
    foreach(obj;File(args[0]).byChunk(4096).parseJsonByLine.query(idx, args[2])){
        writeln(obj);
    }
    stderr.writeln("Time to query/filter records: ",sw.peek.total!"seconds"," seconds");
    // parseQuery("(key1:val1 AND key2:(val2 OR val3 OR val4) AND key3:1-2) OR key4:val4 OR key5:(val5 AND val6)",idx);
    // parseQuery("key1:val1 AND key2:(val2 OR val3) AND key4:val4 AND key5:(val5 OR val6)",idx);
}

void index_main(string[] args){

    StopWatch sw;
    
    JSONInvertedIndex idx = File(args[0]).byChunk(4096).parseJsonByLine.index;

    sw.start;
    idx.writeToFile(File(args[1], "wb"));
    sw.stop;
    stderr.writefln("Wrote index in %d secs",sw.peek.total!"seconds");
}

