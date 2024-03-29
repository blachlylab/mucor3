module mucor3.atomize;
import std.stdio;
import std.getopt;
import std.parallelism;

import libmucor.vcfops.vcf : parseVCF;
import libmucor.error;

bool multiSample;
bool multiAllelic;
bool splitAnnotations;
bool flatten;

int threads = 0;

string help = "
atomize_vcf: converts VCF format to JSONL
usage: ./atomize_vcf [flags] [VCF/BCF input]

Input can be VCF/BCF and can be compressed with bgzf or gzip. 
By default rows are split by sample and ALT allele. To get a 
one-to-one VCF record representation, use the -s and -m flags.

";

void atomize(string[] args)
{
    auto res = getopt(args, config.bundling, "threads|t",
            "extra threads for parsing the VCF file", &threads, "multi-sample|s",
            "don't split (and duplicate) records by sample", &multiSample,
            "multi-allelic|m",
            "don't split (and duplicate) records by sample",
            &multiAllelic, "annotation|a", "split (and duplicate) records by annotation (also sets -m flag)",
            &splitAnnotations, "flatten|f", "flatten sub-objects", &flatten);

    if (res.helpWanted | (args.length < 2))
    {
        defaultGetoptPrinter(help, res.options);
        stderr.writeln();
        return;
    }
    if (splitAnnotations)
    {
        log_warn(__FUNCTION__, "using -a also splits by allele");
    }

    ubyte con = (cast(ubyte)(flatten) << 3) | (cast(ubyte)(!multiSample) << 2) | (
            cast(ubyte)(!multiAllelic) << 1) | cast(ubyte)(splitAnnotations);
    parseVCF(args[1], threads, con, stdout);
}
