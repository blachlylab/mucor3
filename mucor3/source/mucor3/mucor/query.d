module mucor3.mucor.query;
import std.stdio;
import asdf;
import libmucor.varquery;
import std.algorithm: map;
import std.parallelism: taskPool, parallel;
import core.sync.mutex: Mutex;
import std.process;
import std.path: buildPath, baseName;
import progress;
import htslib.hts_log;
import std.array: array;
import std.format: format;
import libmucor.wideint;
import libmucor.khashl;

void indexJsonFiles(string binary, string[] files, string indexFolder, string outfile)
{
    auto pid = spawnProcess([binary, "index"] ~ files ~ [outfile], stdin, stdout, stderr);
    if(wait(pid) != 0) {
        throw new Exception("mucor index failed");
    }
}

void queryJsonFiles(string[] files, string idxFile, string queryStr, string outfile)
{
    import std.datetime.stopwatch: StopWatch;

    auto output = File(outfile, "w");

    StopWatch sw;
    sw.start;

    InvertedIndex idx = InvertedIndex(idxFile, false);
    stderr.writeln("Time to load index: ",sw.peek.total!"seconds"," seconds");
    stderr.writefln("%d records in index",idx.recordMd5s.length);

    sw.stop;
    sw.reset;
    sw.start;

    auto idxs = evalQuery(queryStr, &idx);
    hts_log_info(__FUNCTION__, format("Time to parse query: %d seconds", sw.peek.total!"seconds"));

    sw.stop;
    sw.reset;
    sw.start;
    
    khashlSet!uint128 hashmap;
    foreach(key;idx.convertIds(idx.allIds)){
        hashmap.insert(key);
    }

    auto recordCount = 0;
    auto matching = 0;

    Bar b = new Bar();
    b.message = {return "Filtering vcf data";};
    b.max = files.length;
    b.fill = "#";
    auto m = new Mutex();
    foreach(f; parallel(files)){
        auto rc = 0;
        auto mc = 0;
        auto range = File(f).byChunk(4096).parseJsonByLine;
        foreach(obj; range){
            rc++;
            uint128 a;
            a.fromHexString(deserialize!string(obj["md5"]));
            if(a in hashmap){
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
    hts_log_info(__FUNCTION__, format("Time to query/filter records: %d seconds",sw.peek.total!"seconds"));
    hts_log_info(__FUNCTION__, format("%d / %d records matched your query",recordCount, matching));
}
