module libmucor.varquery;

import std.stdio;
import std.datetime.stopwatch : StopWatch;
import std.algorithm: filter;
import std.range;

public import libmucor.varquery.invertedindex;
public import libmucor.varquery.query;

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
