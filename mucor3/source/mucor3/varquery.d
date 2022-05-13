module mucor3.varquery;
import std.stdio;
import std.exception : enforce;
import std.algorithm : map, joiner, filter;
import std.range;
import std.conv : to;
import libmucor.error;

import std.datetime.stopwatch : StopWatch;

public import libmucor.invertedindex;
public import libmucor.query;

import asdf : deserializeAsdf = deserialize, parseJsonByLine, Asdf;
import libmucor.wideint : uint128;
import libmucor.khashl;
import libmucor.error;
import libmucor: setup_global_pool;
import std.algorithm.searching : balancedParens;
import std.getopt;
import core.stdc.stdlib: exit;
import libmucor.query;

auto query(R)(R range, InvertedIndex* idx, string queryStr)
        if (is(ElementType!R == Asdf))
{
    StopWatch sw;
    sw.start;
    if (!queryStr.balancedParens('(', ')'))
    {
        log_err(__FUNCTION__, "Parentheses aren't matched in query: %s", queryStr);
    }
    auto q = Query(queryStr);
    log_info(__FUNCTION__, "Time to parse query: %s usecs", sw.peek.total!"usecs");
    sw.reset;
    auto idxs = q.evaluate(idx);
    log_info(__FUNCTION__, "Time to evaluate query: %s seconds", sw.peek.total!"seconds");
    sw.stop;
    log_info(__FUNCTION__, "%d records matched your query", idxs.count);

    khashlSet!(uint128) selectedSums;
    foreach (key; idx.convertIds(idxs))
    {
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

void index(R)(R range, string prefix, ulong fsize, ulong ssize, string query_str) if (is(ElementType!R == Asdf))
{
    InvertedIndex* idx = new InvertedIndex(prefix, true, fsize, ssize);
    if(query_str != "")
        idx.addQueryFilter(query_str);
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
    log_info(__FUNCTION__, "Indexed %d records in %d secs", count, sw.peek.total!"seconds");
    log_info(__FUNCTION__, "Avg time to index record: %f usecs",
            float(sw.peek.total!"usecs") / float(count));
}


int threads = -1;
ulong fileCacheSize = 8192;
ulong smallsSize = 128;
string prefix;
string query_str;

void query_main(string[] args)
{

    auto res = getopt(args, config.bundling,
            "threads|t", "threads for running mucor", &threads,
            config.required,
            "prefix|p", "index output prefix", &prefix,
            config.required,
            "query|q", "filter vcf data using varquery syntax", &query_str);

    setup_global_pool(threads);
    // set_log_level(LogLevel.Trace);

    if (res.helpWanted)
    {
        defaultGetoptPrinter("",res.options);
        exit(0);
    }
    if (args.length == 1)
    {
        defaultGetoptPrinter("",res.options);
        log_err(__FUNCTION__, "Please specify input json files");
        exit(1);
    }
    // set_log_level(LogLevel.Debug);
    StopWatch sw;
    sw.start;

    InvertedIndex* idx = new InvertedIndex(prefix, false);
    // auto idxs = idx.fields[args[1]].filter(args[2..$]);
    // float[] range = [args[2].to!float,args[3].to!float];
    log_info(__FUNCTION__, "Time to load index: %s seconds", sw.peek.total!"seconds");
    log_info(__FUNCTION__, "%d records in index", idx.recordMd5s.length);
    sw.stop;
    sw.reset;
    sw.start;
    foreach (obj; args[1 .. $].map!(x => File(x).byChunk(4096)
            .parseJsonByLine).joiner.query(idx, query_str))
    {
        writeln(obj);
    }
    log_info(__FUNCTION__, "Time to query/filter records: %s seconds", sw.peek.total!"seconds");
    // parseQuery("(key1:val1 AND key2:(val2 OR val3 OR val4) AND key3:1-2) OR key4:val4 OR key5:(val5 AND val6)",idx);
    // parseQuery("key1:val1 AND key2:(val2 OR val3) AND key4:val4 AND key5:(val5 OR val6)",idx);
}

void index_main(string[] args)
{

    auto res = getopt(args, config.bundling,
            "threads|t", "threads for running mucor", &threads,
            config.required,
            "prefix|p", "index output prefix", &prefix, 
            "file-cache-size|f", "number of highly used files kept open", &fileCacheSize,
            "ids-cache-size|i", "number of ids that can be stored per key before a file is opened", &smallsSize,
            "query|q", "only index data that pertains to a specific query", &query_str);
    setup_global_pool(threads);

    if (res.helpWanted)
    {
        defaultGetoptPrinter("",res.options);
        exit(0);
    }
    if (args.length == 1)
    {
        defaultGetoptPrinter("",res.options);
        log_err(__FUNCTION__, "Please specify json files");
        exit(1);
    }
    StopWatch sw;

    // set_log_level(LogLevel.Trace);

    args[1 .. $]
        .map!(x => File(x).byChunk(4096).parseJsonByLine.map!(x => Asdf(x.data.dup)))
        .joiner
        .index(prefix, fileCacheSize, smallsSize, query_str);

}
