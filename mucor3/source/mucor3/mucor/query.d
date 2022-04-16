module mucor3.mucor.query;
import std.stdio;
import asdf;
import libmucor.varquery;
import std.algorithm: map;
import std.parallelism: taskPool, parallel;
import core.sync.mutex: Mutex;

JSONInvertedIndex indexFile(string fn) {

    return File(fn).byChunk(4096).parseJsonByLine.index;

}

JSONInvertedIndex indexJsonFilesAndMerge(string[] files, string outfile)
{
    auto tp = taskPool();

    JSONInvertedIndex idx = tp.reduce!"a + b"(files.map!(indexFile));

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