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

JSONInvertedIndex indexFile(string fn) {
    return File(fn).byChunk(4096).parseJsonByLine.index;

}
void indexFile2(string binary, string fn, string outfile) {
    auto nf = File("/dev/null", "w");
    auto pid = spawnProcess([binary, "index", fn, outfile], std.stdio.stdin, nf, nf);
    wait(pid);
}

JSONInvertedIndex loadIndexFile(string fn) {
    return JSONInvertedIndex(fn);
}

JSONInvertedIndex indexJsonFilesAndMerge(string binary, string[] files, string indexFolder, string outfile)
{
    Bar b = new Bar();
    b.message = {return "Indexing vcf data";};
    b.max = files.length;
    b.fill = "#";
    auto m = new Mutex();
    foreach(f; parallel(files, 1)) {
        indexFile2(binary, f, buildPath(indexFolder, f));
        m.lock;
        b.next;
        m.unlock;
    }
    b.finish;
    auto tp = taskPool();

    hts_log_info(__FUNCTION__, "Merging indexes");

    auto indexFiles = files.map!(x => buildPath(indexFolder, x)).array;


    b = new Bar();
    b.message = {return "Indexing vcf data";};
    b.max = files.length;
    b.fill = "#";

    JSONInvertedIndex idx = JSONInvertedIndex(indexFiles[0]);

    b.next;
    foreach(f; parallel(indexFiles[1..$], 1)) {
        auto other = JSONInvertedIndex(f);
        m.lock;
        idx = idx + other;
        b.next;
        m.unlock;
    }

    auto output = File(outfile, "w");

    idx.writeToFile(output);

    return idx;
}

void queryJsonFiles(string[] files, JSONInvertedIndex idx, string query_str, string outfile)
{
    alias queryFile = (string x) {
        return File(x).byChunk(4096).parseJsonByLine.queryRange(idx, query_str);
    };

    auto output = File(outfile, "w");

    auto m = new Mutex();
    foreach(x;parallel(files)) {
        foreach(rec;queryFile(x)){
            m.lock;
            output.writeln(rec);
            m.unlock;
        }
    }
}
