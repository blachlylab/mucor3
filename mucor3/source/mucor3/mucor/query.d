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
import std.conv : to;

void indexJsonFiles(string binary, string query_str, string[] files,
        string indexFolder, int threads, ulong fileCacheSize, ulong smallsSize)
{
    auto cmdline = [
        binary, "index", "-p", indexFolder, "-t", threads.to!string, "-f",
        fileCacheSize.to!string, "-i", smallsSize.to!string, "-q", query_str,
    ] ~ files;
    auto pid = spawnProcess(cmdline);
    if (wait(pid) != 0)
    {
        log_err(__FUNCTION__, "mucor index failed");
    }

}

void queryJsonFiles(string binary, string query_str, string[] files,
        string indexFolder, int threads, string outfile)
{
    auto ofile = File(outfile, "w");
    auto cmdline = [
        binary, "query", "-p", indexFolder, "-t", threads.to!string, "-q",
        query_str,
    ] ~ files;
    auto pid = spawnProcess(cmdline, std.stdio.stdin, ofile);
    if (wait(pid) != 0)
    {
        log_err(__FUNCTION__, "mucor query failed");
    }
}

void combineJsonFiles(string[] files, string outfile)
{
    auto ofile = File(outfile, "w");
    auto cmdline = ["cat"] ~ files;
    auto pid = spawnProcess(cmdline, std.stdio.stdin, ofile);
    if (wait(pid) != 0)
    {
        log_err(__FUNCTION__, "combine failed");
    }
}
