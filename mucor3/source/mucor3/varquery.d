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
import libmucor.serde;
import libmucor : setup_global_pool;
import std.algorithm.searching : balancedParens;
import std.parallelism;
import std.getopt;
import core.stdc.stdlib : exit;
import libmucor.query;

int threads = -1;
ulong fileCacheSize = 8192;
ulong smallsSize = 128;
string prefix;
string query_str;

void query_main(string[] args)
{

    auto res = getopt(args, config.bundling, "threads|t",
            "threads for running mucor", &threads, config.required, "prefix|p",
            "index output prefix", &prefix, config.required, "query|q",
            "filter vcf data using varquery syntax", &query_str);

    setup_global_pool(threads);
    // set_log_level(LogLevel.Trace);

    if (res.helpWanted)
    {
        defaultGetoptPrinter("", res.options);
        exit(0);
    }
    // set_log_level(LogLevel.Debug);
    StopWatch sw;
    sw.start;

    InvertedIndex idx = InvertedIndex(prefix);
    // auto idxs = idx.fields[args[1]].filter(args[2..$]);
    // float[] range = [args[2].to!float,args[3].to!float];
    log_info(__FUNCTION__, "Time to load index: %s seconds", sw.peek.total!"seconds");
    // log_info(__FUNCTION__, "%d records in index", idx.recordMd5s.length);
    sw.stop;
    sw.reset;
    sw.start;
    query(stdout, idx, query_str);
    log_info(__FUNCTION__, "Time to query/filter records: %s seconds", sw.peek.total!"seconds");
    // parseQuery("(key1:val1 AND key2:(val2 OR val3 OR val4) AND key3:1-2) OR key4:val4 OR key5:(val5 AND val6)",idx);
    // parseQuery("key1:val1 AND key2:(val2 OR val3) AND key4:val4 AND key5:(val5 OR val6)",idx);
}

void index_main(string[] args)
{

    auto res = getopt(args, config.bundling, "threads|t",
            "threads for running mucor", &threads, config.required, "prefix|p",
            "index output prefix", &prefix, "query|q",
            "only index data that pertains to a specific query", &query_str);
    setup_global_pool(threads);

    if (res.helpWanted)
    {
        defaultGetoptPrinter("", res.options);
        exit(0);
    }
    if (args.length == 1)
    {
        defaultGetoptPrinter("", res.options);
        log_err(__FUNCTION__, "Please specify json files");
        exit(1);
    }
    StopWatch sw;

    // set_log_level(LogLevel.Trace);
    auto data = VcfIonDeserializer(File(args[1]));
    index(data, prefix);

}
