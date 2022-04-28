module mucor3.mucor.query;
import std.stdio;
import asdf;
import mucor3.varquery;
import std.algorithm : map;
import std.parallelism : taskPool, parallel;
import core.sync.mutex : Mutex;
import std.process;
import std.path : buildPath, baseName;
import progress;
import htslib.hts_log;
import std.array : array;
import std.format : format;
import libmucor.wideint;
import libmucor.khashl;
import libmucor.error;

void indexJsonFiles(string binary, string[] files, string indexFolder)
{
    auto pid = spawnProcess([binary, "index"] ~ files ~ [indexFolder], stdin, stdout, stderr);
    if (wait(pid) != 0)
    {
        log_err(__FUNCTION__, "mucor index failed");
    }
}

void queryJsonFiles(string[] files, string indexFolder, string queryStr, string outfile)
{
    import std.datetime.stopwatch : StopWatch;

    auto output = File(outfile, "w");

    StopWatch sw;
    sw.start;

    InvertedIndex idx = InvertedIndex(indexFolder, false);
    log_info(__FUNCTION__, "Time to load index: ", sw.peek.total!"seconds", " seconds");
    log_info(__FUNCTION__, "%d records in index", idx.recordMd5s.length);

    sw.reset;
    auto q = parseQuery(queryStr);
    log_info(__FUNCTION__, "Time to parse query: %d usecs", sw.peek.total!"usecs");

    sw.reset;
    auto idxs = evaluateQuery(q, &idx);
    log_info(__FUNCTION__, "Time to evaluate query: %d seconds", sw.peek.total!"seconds");

    sw.reset;

    khashlSet!uint128 hashmap;
    foreach (key; idx.convertIds(idx.allIds))
    {
        hashmap.insert(key);
    }

    auto recordCount = 0;
    auto matching = 0;

    Bar b = new Bar();
    b.message = { return "Filtering vcf data"; };
    b.max = files.length;
    b.fill = "#";
    auto m = new Mutex();
    foreach (f; parallel(files))
    {
        auto rc = 0;
        auto mc = 0;
        auto range = File(f).byChunk(4096).parseJsonByLine;
        foreach (obj; range)
        {
            rc++;
            uint128 a;
            a.fromHexString(deserialize!string(obj["md5"]));
            if (a in hashmap)
            {
                mc++;
                m.lock;
                output.writeln(obj);
                m.unlock;
            }
        }
        m.lock;
        b.next;
        recordCount += rc;
        matching += mc;
        m.unlock;
    }
    b.finish;
    log_info(__FUNCTION__, "Time to query/filter records: %d seconds", sw.peek.total!"seconds");
    log_info(__FUNCTION__, "%d / %d records matched your query", recordCount, matching);
}
