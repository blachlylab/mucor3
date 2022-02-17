module mucor3.varquery;
import std.stdio;
import std.datetime.stopwatch : StopWatch;
import std.exception : enforce;
import std.algorithm: map;
import std.range;
import std.conv:to;

import asdf : deserializeAsdf = deserialize, parseJsonByLine;
import libmucor.varquery.invertedindex;
import libmucor.varquery.query;
import libmucor.wideint : uint128;

void query(string[] args){
    StopWatch sw;
    sw.start;
 
    JSONInvertedIndex idx = JSONInvertedIndex(args[1]);
    // auto idxs = idx.fields[args[1]].filter(args[2..$]);
    // float[] range = [args[2].to!float,args[3].to!float];
    stderr.writeln("Time to load index: ",sw.peek.total!"seconds"," seconds");
    stderr.writefln("%d records in index",idx.recordMd5s.length);
    sw.stop;
    sw.reset;
    sw.start;
    foreach(obj;File(args[0]).byChunk(4096).parseJsonByLine.query(idx, args[2])){
        writeln(obj);
    }
    stderr.writeln("Time to query/filter records: ",sw.peek.total!"seconds"," seconds");
    // parseQuery("(key1:val1 AND key2:(val2 OR val3 OR val4) AND key3:1-2) OR key4:val4 OR key5:(val5 AND val6)",idx);
    // parseQuery("key1:val1 AND key2:(val2 OR val3) AND key4:val4 AND key5:(val5 OR val6)",idx);
}

auto query(Range)(Range range, JSONInvertedIndex idx, string queryStr){
    StopWatch sw;
    sw.start;

    auto idxs = evalQuery(queryStr, &idx);
    stderr.writeln("Time to parse query: ",sw.peek.total!"seconds"," seconds");
    stderr.writeln(idxs.length," records matched your query");
    sw.stop;

    bool[uint128] hashmap;
    foreach(key;idx.convertIds(idx.allIds)){
        hashmap[key] = false;
    }

    foreach (key; idxs)
    {
        hashmap[key] = true;
    }

    return range.enumerate.map!((x) {
        auto i = x.index;
        auto line = x.value;
        uint128 a;
        a.fromHexString(deserializeAsdf!string(line["md5"]));
        assert(idx.recordMd5s[i] ==  a);
        auto val = hashmap.get(a, false);
        if(a in hashmap){
            if(hashmap[a])
                return line;
            throw new Exception("Something odd happened");
        }else{
            throw new Exception("record not present in index");
        }
    });
}

void index(string[] args){

    StopWatch sw;
    
    JSONInvertedIndex idx = File(args[0]).byChunk(4096).parseJsonByLine.index;

    sw.start;
    idx.writeToFile(File(args[1], "wb"));
    sw.stop;
    stderr.writefln("Wrote index in %d secs",sw.peek.total!"seconds");
}

JSONInvertedIndex index(Range)(Range range){
    JSONInvertedIndex idx;

    StopWatch sw;
    sw.start;
    auto count = 0;
    foreach (line; range)
    {
        idx.addJsonObject(line);
        count++;
    }
    assert(count == idx.recordMd5s.length,"number of md5s doesn't match number of records");
    sw.stop;
    stderr.writefln("Indexed %d records in %d secs",count,sw.peek.total!"seconds");
    stderr.writefln("Avg time to index record: %f usecs",float(sw.peek.total!"usecs") / float(count));
    return idx;
}
