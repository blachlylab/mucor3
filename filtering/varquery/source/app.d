import std.stdio;
import std.datetime.stopwatch : StopWatch;

import asdf : deserializeAsdf = deserialize;
import varquery.invertedindex;
import varquery.query;
import varquery.wideint : uint128;

void main(string[] args)
{
    switch(args[1]){
        case "index":
            index(args[2..$]);
            return;
        case "query":
            filter(args[2..$]);
            return;
        default:
            break;
    }
}

void filter(string[] args){
    import asdf:parseJsonByLine;
    import std.range:enumerate;
    import std.conv:to;
    StopWatch sw;
    sw.start;
 
    JSONInvertedIndex idx = JSONInvertedIndex(File(args[0]));
    // auto idxs = idx.fields[args[1]].filter(args[2..$]);
    // float[] range = [args[2].to!float,args[3].to!float];
    stderr.writeln("Time to load index: ",sw.peek.total!"seconds"," seconds");
    sw.stop;
    sw.reset;
    sw.start;
    auto idxs = evalQuery(args[2],&idx);
    stderr.writeln("Time to parse query: ",sw.peek.total!"seconds"," seconds");
    stderr.writeln(idxs.length," records matched your query");
    sw.stop;
    sw.reset;
    bool[uint128] hashmap;
    foreach (key; idxs)
    {
        hashmap[key] = true;
    }
    sw.start;
    foreach(line;File(args[1]).byChunk(4096).parseJsonByLine){
        if(idxs.length==0) break;
        uint128 a;
        a.fromHexString(deserializeAsdf!string(line["md5"]));
        auto val = hashmap.get(a, false);
        if(val){
            writeln(line);
        }
    }
    stderr.writeln("Time to query/filter records: ",sw.peek.total!"seconds"," seconds");
    // parseQuery("(key1:val1 AND key2:(val2 OR val3 OR val4) AND key3:1-2) OR key4:val4 OR key5:(val5 AND val6)",idx);
    // parseQuery("key1:val1 AND key2:(val2 OR val3) AND key4:val4 AND key5:(val5 OR val6)",idx);
}

void index(string[] args){
    import asdf:parseJsonByLine;
    import std.range:enumerate;
    import std.conv:to;
    JSONInvertedIndex idx;

    StopWatch sw;
    sw.start;
    auto count = 0;
    foreach (line; File(args[0]).byChunk(4096).parseJsonByLine)
    {
        idx.addJsonObject(line);
        count++;
    }
    stderr.writeln("Avg time to index record: ",sw.peek.total!"usecs"/count," usecs");
    idx.writeToFile(File(args[1], "wb"));
}
