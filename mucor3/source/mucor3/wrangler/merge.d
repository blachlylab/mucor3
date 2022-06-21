module mucor3.wrangler.merge;
import std.stdio;

import asdf;
import libmucor.jsonlops.range : groupby, aggregate;
import libmucor.jsonlops.basic : merge;
import libmucor.error;
import std.algorithm : each;

void merge_main(string[] args)
{
    if (args.length == 0)
    {
        log_info(__FUNCTION__,
                "Merge rows together based on given index. Overlapping"
                ~ " fields in rows are combined into arrays.");
    }
    else
    {
        foreach (obj; stdin.byChunk(4096).parseJsonByLine.groupby(args).aggregate!merge)
            obj.writeln;
    }
}
