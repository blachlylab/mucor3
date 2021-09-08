import std.stdio;
static import wrangler.merge;
static import wrangler.table;
static import wrangler.pivot;
static import wrangler.normalize;
static import wrangler.unique;
import std.datetime.stopwatch:StopWatch;
import asdf : deserializeAsdf = deserialize;

void main(string[] args)
{
    if(args.length == 1){
        stderr.writeln("subcommands are : merge, join, table, pivot, norm, uniq");
        return;
    }
    switch(args[1]){
        case "merge":
            wrangler.merge.run(args[2..$]);
            return;
        case "table":
            wrangler.table.run(args[1..$]);
            return;
        case "pivot":
            wrangler.pivot.run(args[1..$]);
            return;
        case "norm":
            wrangler.normalize.run(args[1..$]);
            return;
        case "uniq":
            wrangler.unique.run(args[1..$]);
            return;
        default:
            break;
    }
}
