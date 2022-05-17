module mucor3.mucor.vcf;

import core.stdc.stdlib : exit;
import std.parallelism : parallel;
import core.sync.mutex : Mutex;
import progress;
import std.stdio;
import dhtslib.vcf;
import libmucor.vcfops;
import libmucor.jsonlops.basic : getAllKeys;
import libmucor.error;
import libmucor.khashl;
import libmucor.invertedindex : sep;
import std.format : format;
import std.path : buildPath, baseName;
import std.process;
import std.typecons : Tuple;
import asdf;
import std.array : split;
import std.algorithm : reduce;

static void defaultAtomize(string binary, string fn, string outfile)
{
    auto output = File(outfile, "w");
    auto nf = File("/dev/null", "w");
    auto pid = spawnProcess([binary, "atomize", fn], std.stdio.stdin, output, nf);
    if (wait(pid) != 0)
    {
        log_err(__FUNCTION__, "mucor atomize failed");
    }
}

void atomizeVcfs(string bin, string[] vcf_files, string vcf_json_dir)
{
    Bar b = new Bar();
    b.message = { return "Atomizing vcfs"; };
    b.max = vcf_files.length;
    b.fill = "#";
    auto m = new Mutex();
    foreach (f; parallel(vcf_files, 1))
    {
        defaultAtomize(bin, f, buildPath(vcf_json_dir, baseName(f)));
        m.lock;
        b.next;
        m.unlock;
    }
    b.finish();
}

alias ColData = Tuple!(StringSet, "cols", StringSet, "samples");

auto collectColumns(string fn, string[] extra)
{
    StringSet set;
    StringSet sampleSet;
    foreach (obj; File(fn).byChunk(4096).parseJsonByLine)
    {
        foreach (k; obj.getAllKeys.byKey)
        {
            set.insert(k);
        }
        sampleSet.insert(obj["sample"].deserialize!string);
    }
    foreach (e; extra)
    {
        if (!(e in set))
        {
            log_warn(__FUNCTION__, "Extra column %s not present in json data!", e);
        }
    }
    return ColData(set, sampleSet);
}

auto validateData(string fn, string[] required)
{
    StringSet sampleSet;
    foreach (obj; File(fn).byChunk(4096).parseJsonByLine)
    {
        foreach (r; required)
        {
            auto v = r.split(sep);
            if (obj[v] == Asdf.init)
            {
                log_err(__FUNCTION__, "%s column not found in some rows!", r);
            }
        }
        sampleSet.insert(obj["sample"].deserialize!string);
    }
    return sampleSet;
}


auto validateVcfData(string[] json_files, string[] required)
{
    Bar b = new Bar();
    b.message = { return "Validating vcf data"; };
    b.max = json_files.length;
    b.fill = "#";
    auto m = new Mutex();
    auto coldatas = new StringSet[json_files.length];
    foreach (i, f; parallel(json_files, 1))
    {
        coldatas[i] = validateData(f, required);
        m.lock;
        b.next;
        m.unlock;
    }
    b.finish();

    auto combined = coldatas.reduce!"a | b";
    return combined;
}

ColData combineColData(ColData a, ColData b) {
    ColData ret;
    ret.cols = a.cols | b.cols;
    ret.samples = a.samples | b.samples;
    return ret;
}