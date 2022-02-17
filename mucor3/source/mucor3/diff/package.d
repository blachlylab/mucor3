module mucor3.diff;

import std.path: buildPath;
import std.algorithm : map, joiner;

import mucor3.diff.process; 

import dhtslib.vcf;
import dhtslib.coordinates;
import libmucor.vcfops;
import libmucor.jsonlops;
import libmucor.khashl;
import asdf;

string[] varKeys = ["CHROM", "POS", "REF", "ALT"];

string[] samVarKeys = ["CHROM", "POS", "REF", "ALT", "sample"];
alias Set = khashlSet!(Asdf, true);

void diff_main(string[] args)
{
    auto vcfa = VCFReader(args[1], 4, UnpackLevel.All);
    auto vcfb = VCFReader(args[2], 4, UnpackLevel.All);

    auto aRange = parseVCF(vcfa, 4);
    auto bRange = parseVCF(vcfb, 4);
    auto prefix = "";
    
    auto idxs = processVcfRaw(aRange, bRange, buildPath(prefix,"raw"));
    processVcfFiltered(aRange, bRange, args[3], idxs, buildPath(prefix, "filtered"));

}

/// Parse VCF to JSONL
auto parseVCF(VCFReaderImpl!(CoordSystem.zbc, false) vcf, int threads){

    //get info needed from header 
    auto cfg = getHeaderConfig(vcf.vcfhdr);

    return vcf.map!(x => parseRecord(x, cfg))
        .map!((x) {
            dropNullGenotypes(x);
            return expandBySample(x);
        }).joiner
        .map!((obj) {
            auto numAlts = getNumAlts(obj);
            return expandMultiAllelicSites!true(obj, numAlts);
        }).joiner.map!(x => x.serializeToAsdf);
}