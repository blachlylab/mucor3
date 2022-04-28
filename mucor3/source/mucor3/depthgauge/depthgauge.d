module mucor3.depthgauge.depthgauge;
import std.stdio;
import std.algorithm.searching : until;
import std.algorithm.sorting : sort;
import std.algorithm.iteration : uniq, filter;
import std.algorithm : map, count;
import std.range : array;
import std.path : buildPath;
import std.conv : to;
import std.parallelism : parallel, defaultPoolThreads;
import std.getopt;
import dhtslib.sam : SAMFile;
import dhtslib.coordinates;

int threads = 0;

import mucor3.depthgauge.csv;

void depthgauge(string[] args)
{
    auto res = getopt(args, config.bundling, "threads|t",
            "threads for running depth gauge", &threads);
    if (res.helpWanted)
    {
        defaultGetoptPrinter(
                "usage: ./depthgauge [input tsv] [number of first sample column] [bam folder] [output tsv]",
                res.options);
        stderr.writeln();
        return;
    }
    if (args.length != 5)
    {
        writeln(
                "usage: ./depthgauge [input tsv] [number of first sample column] [bam folder] [output tsv]");
        return;
    }
    else
    {
        if (threads != 0)
        {
            defaultPoolThreads(threads);
        }
        // startsamples is OB then converted to ZB
        OB sampleStart = OB(args[2].to!long);

        // initialize AF table
        auto t = Table(args[1], sampleStart);
        // open first sam file to get header
        SAMFile s = SAMFile(buildPath(args[3], t.samples[0] ~ ".bam"), 0);

        // parse table records
        t.parseRecords(&s, sampleStart);

        // get depths from sam/bam files in parallel
        getDepths(t, args[3]);
        File f = File(args[4], "w");
        t.write(f);
        f.close;
    }

}

void getDepths(ref Table t, string prefix)
{

    foreach (j, sample; parallel(t.samples))
    {
        auto bam = SAMFile(buildPath(prefix, sample ~ ".bam"), 0);
        foreach (i, tableRec; t.records)
        {
            t.matrix[i][j] = bam[tableRec.chr, tableRec.pos].count;
        }
        stderr.writeln(sample);
    }
}
