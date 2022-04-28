module mucor3.wrangler.normalize;

import std.algorithm: map, each;
import std.stdio;
import std.getopt;

import asdf;
import libmucor.jsonlops.basic;
import libmucor.error;

void norm_main(string[] args)
{
    auto res = getopt(args);
    if(res.helpWanted){
        log_info(__FUNCTION__,"Flattens JSON input via stdin, overlapping nested values are collected in arrays");
        return;
    }
    foreach(obj;stdin.byChunk(4096).parseJsonByLine.map!normalize)
        obj.writeln;
}
