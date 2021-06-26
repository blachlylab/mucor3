module wrangler.unique;

import std.stdio;
import std.getopt;
import std.algorithm : sort, uniq, sum, map, joiner;
import std.array : array;

import jsonlops.basic;
import asdf;

void run(string[] args)
{
    auto res = getopt(args);
    if(res.helpWanted){
        stderr.writeln("Transforms JSON input via stdin so that arrays contain only unique values");
        return;
    }
    foreach(obj;stdin.byChunk(4096).parseJsonByLine.map!unique)
        obj.writeln;
}
