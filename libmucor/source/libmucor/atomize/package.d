module libmucor.atomize;

import std.stdio;
import std.algorithm : map, each;
import std.array : array, split;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.math : isNaN;
import std.parallelism;
import core.sync.mutex : Mutex;

import dhtslib.vcf;
import dhtslib.coordinates;
import htslib;
import libmucor.atomize.record;
import libmucor.atomize.header;


import libmucor.khashl : khashl;
import libmucor.jsonlops;
import libmucor.error;
import libmucor: setup_global_pool;
import std.parallelism : parallel;

import mir.ser.interfaces;
import mir.ser;

struct VcfIR {
    string fn;
    int threads;
    bool multiSample;
    bool multiAllele;

    void serialize(ISerializer serializer) {
        setup_global_pool(threads);
        //open vcf
        auto vcf = VCFReader(fn, threads, UnpackLevel.All);

        StopWatch sw;
        sw.start;
        int vcf_row_count = 0;
        int output_count = 0;
        auto recIR = VcfRec(vcf.vcfhdr);
        // loop over records and parse
        auto s = serializer.listBegin;
        foreach (x; vcf)
        {
            recIR.parse(x);
            vcf_row_count++;
            if(!multiSample) {
                for(auto i = 0; i < recIR.hdrInfo.samples.length;i++) {
                    auto samIR = VcfRecSingleSample(recIR, i);
                    if(!multiAllele){
                        for(auto j = 0; j < samIR.alt.length; j++) {
                            auto aIR = VcfRecSingleAlt(samIR, j);
                            serializeValue(serializer, aIR);
                            output_count++;
                        }
                    } else {
                        serializeValue(serializer, samIR);
                        output_count++;
                    }
                }
            } else {
                serializeValue(serializer, recIR);
                output_count++;
            }
        }
        serializer.listEnd(s);
        if (vcf_row_count > 0)
        {
            log_info(__FUNCTION__, "Parsed %,3d records in %d seconds",
                    vcf_row_count, sw.peek.total!"seconds");
            log_info(__FUNCTION__, "Output %,3d json objects", output_count);
            log_info(__FUNCTION__, "Avg. time per VCF record: %d usecs",
                    sw.peek.total!"usecs" / vcf_row_count);
        }
        else
        log_info(__FUNCTION__, "No records in this file!");
    }
}

/// Parse VCF to JSONL
void parseVCF(string fn, int threads, bool multiSample, bool multiAllele, ref File output)
{
    auto x = VcfIR(fn, threads, multiSample, multiAllele);
    import mir.ser.ion;
    output.rawWrite(serializeIon(x));
}

unittest {
    {
        auto f = File("/tmp/test.ion", "wb");
        parseVCF("../test/data/vcf_file.vcf", -1, false, false, f);
    }
    import std.file : read;
    import mir.ion.conv;
    writeln(ion2text((cast(ubyte[])read("/tmp/test.ion"))));
}