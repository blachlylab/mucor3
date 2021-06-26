module wrangler.merge;
import std.stdio;

import asdf;
import jsonlops.range: groupby, aggregate;
import jsonlops.basic: merge;
import std.algorithm : each;

void run(string[] args){
    if(args.length==0){
        writeln(
            "Merge rows together based on given index. Overlapping"~
            " fields in rows are combined into arrays."
        );
    }else{
        foreach(obj;stdin.byChunk(4096).parseJsonByLine.groupby(args).aggregate!merge)
            obj.writeln;
    }
}

