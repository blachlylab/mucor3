module mucor3.mucor.vcf;

import htslib.hts_log;
import core.stdc.stdlib : exit;
import std.parallelism : parallel;
import core.sync.mutex : Mutex;
import progress;
import std.stdio;
import dhtslib.vcf;
import libmucor.vcfops;
import libmucor.error;
import std.format : format;
import std.path : buildPath, baseName;
import std.process;

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
