module libmucor.varquery;

import std.stdio;
import std.datetime.stopwatch : StopWatch;
import std.algorithm: map;
import std.range;

public import libmucor.varquery.invertedindex;
public import libmucor.varquery.query;

import asdf : deserializeAsdf = deserialize, parseJsonByLine, Asdf;
import libmucor.wideint : uint128;

auto queryRange(R)(R range, JSONInvertedIndex idx, string queryStr)
if (is(ElementType!R == Asdf))
{
    auto idxs = evalQuery(queryStr, &idx);

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

auto query(R)(R range, JSONInvertedIndex idx, string queryStr)
if (is(ElementType!R == Asdf))
{
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

JSONInvertedIndex index(R)(R range)
if (is(ElementType!R == Asdf))
{
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
