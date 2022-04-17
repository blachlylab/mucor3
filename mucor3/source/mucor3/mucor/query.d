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

void indexJsonFiles(string binary, string[] files, string indexFolder, string outfile)
{
    auto pid = spawnProcess([binary, "index"] ~ files ~ [outfile], stdin, stdout, stderr);
    if(wait(pid) != 0) {
        throw new Exception("mucor index failed");
    }
}

void queryJsonFiles(string binary, string[] files, string idxFile, string query_str, string outfile)
{
    auto output = File(outfile, "w");
    auto pid = spawnProcess([binary, "query"] ~ files ~ [idxFile, query_str], stdin, output, stderr);
    if(wait(pid) != 0) {
        throw new Exception("mucor query failed");
    }
}
