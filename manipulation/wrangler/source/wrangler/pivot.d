module wrangler.pivot;

import std.stdio;
import core.stdc.stdlib:exit;
import asdf;

import jsonlops.range;
import std.getopt;
import std.array: split;
import std.algorithm: map, each;
import std.array:array;


void run(string[] args){
    string index;
    string on;
    string value;
    auto res = getopt(args,std.getopt.config.required,"i|index",&index,
        std.getopt.config.required,"o|on",&on,
        std.getopt.config.required,"v|value",&value);
    if(res.helpWanted){
        stderr.writeln("Flattens JSON input via stdin, overlapping nested values are collected in arrays");
        return;
    }
    foreach(obj;stdin.byChunk(4096).parseJsonByLine.groupby(index.split(",")~[on]).pivot!"self"(on, value))
        obj.writeln;
}