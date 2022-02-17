module mucor3.wrangler.pivot;

import std.stdio;
import core.stdc.stdlib:exit;
import asdf;

import libmucor.jsonlops.range;
import std.getopt;
import std.array: split;
import std.algorithm: map, each;
import std.array:array;


void piv_main(string[] args){
    string[] indexes;
    string[] extras;
    string on;
    string value;
    arraySep = ",";
    auto res = getopt(args,std.getopt.config.required,"i|index",&indexes,
        std.getopt.config.required,"o|on",&on,
        std.getopt.config.required,"v|value",&value,
        "e|extra",&extras);
    if(res.helpWanted){
        stderr.writeln("Flattens JSON input via stdin, overlapping nested values are collected in arrays");
        return;
    }
    foreach(obj;stdin.byChunk(4096).parseJsonByLine.groupby(indexes).pivot!"self"(on, value, extras))
        obj.writeln;
}
