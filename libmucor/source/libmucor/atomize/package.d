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
import libmucor.atomize.serde.ser;

import libmucor.khashl : khashl;
import libmucor.jsonlops;
import libmucor.error;
import libmucor: setup_global_pool;
import std.parallelism : parallel;

import mir.ser.interfaces;
import mir.ser;
import mir.serde : SerdeTarget;

/// Parse VCF to JSONL
void parseVCF(string fn, int threads, bool multiSample, bool multiAllele, ref File output)
{
    setup_global_pool(threads);
    //open vcf
    auto vcf = VCFReader(fn, threads, UnpackLevel.All);

    StopWatch sw;
    sw.start;
    int vcf_row_count = 0;
    int output_count = 0;
    auto recIR = VcfRec(vcf.vcfhdr);
    // loop over records and parse
    auto ser = VcfSerializer(output, recIR.hdrInfo, SerdeTarget.ion);
    if(multiSample && multiAllele) {
        foreach (x; vcf)
        {
            recIR.parse(x);
            vcf_row_count++;
            ser.putRecord(recIR);
            output_count++;
        }
    } else if(!multiSample && multiAllele) {
        foreach (x; vcf)
        {
            recIR.parse(x);
            vcf_row_count++;
            for(auto i = 0; i < recIR.hdrInfo.samples.length;i++) {
                auto samIR = VcfRecSingleSample(recIR, i);
                ser.putRecord(samIR);
                output_count++;
            }
            
        }
    } else {
        foreach (x; vcf)
        {
            recIR.parse(x);
            vcf_row_count++;
            for(auto i = 0; i < recIR.hdrInfo.samples.length;i++) {
                auto samIR = VcfRecSingleSample(recIR, i);
                for(auto j = 0; j < samIR.alt.length; j++) {
                    auto aIR = VcfRecSingleAlt(samIR, j);
                    ser.putRecord(aIR);
                    output_count++;
                }
            }
            
        }
    }
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

unittest {
    {
        auto f = File("/tmp/test.ion", "wb");
        parseVCF("../test/data/vcf_file.vcf", -1, false, false, f);
    }
    import std.file : read;
    import mir.ion.conv;
    import mir.ser.text;
    import libmucor.atomize.serde.deser;
    
    auto f = File("/tmp/test.ion");
    auto rdr = VcfIonDeserializer(f);
    
    foreach (rec; rdr)
    {
        auto r = rec.unwrap;
        writeln(vcfIonToText(r));    
    }
}