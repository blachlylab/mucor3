module mucor3.diff;

import std.algorithm : map, joiner;
import std.sumtype;

import dhtslib.vcf;
import libmucor.vcfops;
import libmucor.jsonlops.jsonvalue;

void diff_main(string[] args)
{
    auto varRangePrev = parseVCF(args[1], 4);
    auto varRangeCurr = parseVCF(args[2], 4);

    
}

/// Parse VCF to JSONL
auto parseVCF(string fn, int threads){
    //open vcf
    auto vcf = VCFReader(fn,threads, UnpackLevel.All);

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
        }).joiner;
}