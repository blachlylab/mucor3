module mucor3.varquery;
import std.stdio;
import std.exception : enforce;
import std.algorithm: map, joiner, filter;
import std.range;
import std.conv:to;
import libmucor.error;

import std.datetime.stopwatch : StopWatch;

public import libmucor.invertedindex;
public import libmucor.query;

import asdf : deserializeAsdf = deserialize, parseJsonByLine, Asdf;
import libmucor.wideint : uint128;
import libmucor.khashl;
import libmucor.error;
import std.algorithm.searching: balancedParens;

auto query(R)(R range, InvertedIndex * idx, string queryStr)
if (is(ElementType!R == Asdf))
{
    StopWatch sw;
    sw.start;
    if(!queryStr.balancedParens('(',')')) {
        log_err(__FUNCTION__, "Parentheses aren't matched in query: %s", queryStr);
    }
    auto q = parseQuery(queryStr);
    log_info(__FUNCTION__,"Time to parse query: %s usecs",sw.peek.total!"usecs");
    sw.reset;
    auto idxs = evaluateQuery(q, idx);
    log_info(__FUNCTION__, "Time to evaluate query: %s seconds", sw.peek.total!"seconds");
    sw.stop;
    log_info(__FUNCTION__, "%d records matched your query",idxs.count);
    

    khashlSet!(uint128) selectedSums;
    foreach(key; idx.convertIds(idxs)){
        selectedSums.insert(key);
    }

    return range.filter!((line) {
        // auto i = x.index;
        // auto line = x.value;
        uint128 a;
        a.fromHexString(deserializeAsdf!string(line["md5"]));
        // assert(idx.recordMd5s[i] ==  a);
        return a in selectedSums ? true : false;
    });
}

void index(R)(R range, string prefix)
if (is(ElementType!R == Asdf))
{
    InvertedIndex * idx = new InvertedIndex(prefix, true);

    StopWatch sw;
    sw.start;
    auto count = 0;
    foreach (line; range)
    {
        idx.addJsonObject(line);
        count++;
    }
    // assert(count == idx.recordMd5s.length,"number of md5s doesn't match number of records");
    idx.close;

    sw.stop;
    log_info(__FUNCTION__, "Indexed %d records in %d secs",count,sw.peek.total!"seconds");
    log_info(__FUNCTION__,"Avg time to index record: %f usecs",float(sw.peek.total!"usecs") / float(count));
}


void query_main(string[] args){
    StopWatch sw;
    sw.start;
 
    InvertedIndex * idx = new InvertedIndex(args[$-2], false);
    // auto idxs = idx.fields[args[1]].filter(args[2..$]);
    // float[] range = [args[2].to!float,args[3].to!float];
    log_info(__FUNCTION__, "Time to load index: %s",sw.peek.total!"seconds"," seconds");
    log_info(__FUNCTION__, "%d records in index",idx.recordMd5s.length);
    sw.stop;
    sw.reset;
    sw.start;
    foreach(obj;args[0..$-1].map!(x => File(x).byChunk(4096).parseJsonByLine).joiner.query(idx, args[$-1])){
        writeln(obj);
    }
    log_info(__FUNCTION__,"Time to query/filter records: ",sw.peek.total!"seconds"," seconds");
    // parseQuery("(key1:val1 AND key2:(val2 OR val3 OR val4) AND key3:1-2) OR key4:val4 OR key5:(val5 AND val6)",idx);
    // parseQuery("key1:val1 AND key2:(val2 OR val3) AND key4:val4 AND key5:(val5 OR val6)",idx);
}

void index_main(string[] args){

    StopWatch sw;
    
    args[0..$-1].map!(x => File(x).byChunk(4096).parseJsonByLine).joiner.index(args[$-1]);

}

