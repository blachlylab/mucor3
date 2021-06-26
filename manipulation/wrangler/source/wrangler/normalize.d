module wrangler.normalize;

import std.algorithm: map, each;
import std.stdio;
import std.getopt;

import asdf;
import jsonlops.basic;

void run(string[] args)
{
    auto res = getopt(args);
    if(res.helpWanted){
        stderr.writeln("Flattens JSON input via stdin, overlapping nested values are collected in arrays");
        return;
    }
    foreach(obj;stdin.byChunk(4096).parseJsonByLine.map!normalize)
        obj.writeln;
}
