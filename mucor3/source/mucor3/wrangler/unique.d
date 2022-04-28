module mucor3.wrangler.unique;

import std.stdio;
import std.getopt;
import std.algorithm : sort, uniq, sum, map, joiner;
import std.array : array;

import libmucor.jsonlops.basic;
import asdf;
import libmucor.error;

void unique_main(string[] args)
{
    auto res = getopt(args);
    if (res.helpWanted)
    {
        log_info(__FUNCTION__,
                "Transforms JSON input via stdin so that arrays contain only unique values");
        return;
    }
    foreach (obj; stdin.byChunk(4096).parseJsonByLine.map!unique)
        obj.writeln;
}
