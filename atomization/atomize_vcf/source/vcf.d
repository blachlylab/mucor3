module vcf;

import std.stdio;
import std.string : toStringz, fromStringz;
import std.algorithm : map, each;
import std.array : array, split;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.digest.md : MD5Digest, toHexString;
import std.math : isNaN;

import dhtslib.vcf;
import dhtslib.coordinates;
import htslib.vcf;
import htslib.hts;
import htslib.hts_log;
import asdf;
import fields;
import jsonlops.range;
import jsonlops.basic;

auto norm(R)(R range, bool active)
{
    return range.map!((x) {
        if(!active)
            return x;
        else
            return normalize(x);
    });
}

/// Parse VCF to JSONL
void parseVCF(string fn, int threads, ubyte con){
    //open vcf
    auto vcf = VCFReader(fn);

    //set extra threads
    hts_set_threads(vcf.fp, threads);
    //get header
    StopWatch sw;
    sw.start;
    auto vcf_row_count = 0;
    auto output_count = 0;
    // loop over records and parse
    auto range = vcf.map!((x) {
            auto obj = parseRecord(x);
            vcf_row_count++;
            return obj;
        }).dropNullGenotypes(cast(bool)(con & 8))
        .expandBySample(cast(bool)(con & 4))
        .expandMultiAllelicSites(cast(bool)(con & 2))
        .norm(cast(bool)(con & 16));
    foreach(x; range){
        writeln(x);
        output_count++;
    }
    if(vcf_row_count > 0) {
        stderr.writefln("Parsed %,3d records in %d seconds",vcf_row_count, sw.peek.total!"seconds");
        stderr.writefln("Output %,3d json objects",output_count);
        stderr.writeln("Avg. time per VCF record: ",sw.peek.total!"usecs"/vcf_row_count," usecs");
    }else
        stderr.writeln("No records in this file!");
}

/// Parse individual records to JSON
Asdf parseRecord(VCFRecord record){
    record.unpack(UnpackLevel.All);
    

    // create root json object
    auto root = AsdfNode("{}".parseJson);

    // parse standard fields
    root["CHROM"] = AsdfNode(record.chrom.serializeToAsdf);
    root["POS"] = AsdfNode(record.pos.to!OB.pos.serializeToAsdf);
    root["ID"] = AsdfNode(record.id.serializeToAsdf);
    if(!isNaN(record.qual)) // ignore if nan
        root["QUAL"] = AsdfNode(record.qual.serializeToAsdf);
    
    // parse ref and alt alleles
    auto alleles = record.allelesAsArray;

    root["REF"] = AsdfNode(alleles[0].serializeToAsdf);
    if (alleles.length > 2) {
        root["ALT"] = AsdfNode(alleles[1..$].serializeToAsdf);
    }else if (alleles.length > 1) {
        root["ALT"] = AsdfNode(alleles[1].serializeToAsdf);
    }

    // parse filters if any
    const(char)[][] filters;
    for(int i = 0;i < record.line.d.n_flt;i++){
        filters~=fromStringz(record.vcfheader.hdr.id[BCF_DT_ID][ record.line.d.flt[i]].key);
    }
    if(filters != [])
        root["FILTER"] = AsdfNode(filters.serializeToAsdf);

    // prepare info root object
    auto info_root = AsdfNode(parseInfoFields(record));

    // Go by each vcf info field and by type
    // convert to native type then to asdf
    //  
    

    // add info fields to root and 
    // parse any annotation fields
    root["INFO"] = info_root;
    root =parseAnnotationField(root,"ANN",ANN_FIELDS[]);
    root =parseAnnotationField(root,"NMD",LOF_FIELDS[],LOF_TYPES[]);
    root =parseAnnotationField(root,"LOF",LOF_FIELDS[],LOF_TYPES[]);

    // if no samples/format info, write
    if(record.vcfheader.nsamples==0){
        writeln(cast(Asdf)root);
    }

    // if there are samples
    // go by sample and for each
    // fromat field and convert to native type
    // and then convert to asdf
    // and write one record per sample
    auto fmt_root = AsdfNode(parseFormatFields(record));
    // add root to format and write
    root["FORMAT"] = fmt_root;
   return cast(Asdf) root;
}

Asdf md5sumObject(Asdf obj) {
    // create md5 object
    auto md5 = new MD5Digest();
    auto root = AsdfNode(obj);
    root["md5"] = AsdfNode(serializeToAsdf(md5.digest(obj.data).toHexString));
    return cast(Asdf)root;
}


/// basic helper for range based 
/// access to vcf file
struct VCFRange{
    htsFile * fp;
    bcf_hdr_t * header;
    bcf1_t * b;
    bool EOF = false;
    this(htsFile * fp,bcf_hdr_t * header){
        this.fp=fp;
        this.header=header;
        this.b = bcf_init1();
    }
    bcf1_t * front(){
        return this.b;
    }
    void popFront(){
        
    }
    bool empty(){
        auto success = bcf_read(fp,header,b);
        if(success==-1) EOF=true;
        return EOF;
    }
}

