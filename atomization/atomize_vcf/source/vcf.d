module vcf;

import std.stdio;
import std.string : toStringz, fromStringz;
import std.algorithm : map;
import std.array : array, split;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.digest.md : MD5Digest, toHexString;
import std.math : isNaN;
import std.traits : EnumMembers;
import std.range : enumerate;

import dhtslib.vcf;
import dhtslib.coordinates;
import htslib.vcf;
import htslib.hts;
import htslib.hts_log;
import asdf;
import fields;

/// Parse VCF to JSONL
void parseVCF(string fn, int threads = 4){
    //open vcf
    auto vcf = VCFReader(fn);

    //set extra threads
    hts_set_threads(vcf.fp, threads);
    //get header
    StopWatch sw;
    sw.start;
    auto count =0;
    // loop over records and parse
    foreach (b; vcf)
    {
        parseRecord(b);
        count++;
    }
    stderr.writeln("Avg. time per record: ",sw.peek.total!"usecs"/count," usecs");

}

/// Parse individual records to JSON
void parseRecord(VCFRecord record){
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
    writeln(cast(Asdf) root);
}

Asdf parseInfoFields(VCFRecord record) {
    // prepare info root object
    auto info_root=AsdfNode("{}".parseJson);
    info_root["by_allele"] = AsdfNode("{}".parseJson);
    auto alleles = record.allelesAsArray();
    foreach (allele; alleles[1..$])
    {
        info_root["by_allele"][allele] = AsdfNode("{}".parseJson);
    }
    auto infos = record.getInfos;
    // Go by each vcf info field and by type
    // convert to native type then to asdf
    //  
    foreach (key, info; infos)
    {
        auto hRec = record.vcfheader.getHeaderRecord(HeaderRecordType.Info, key);
        
        final switch(info.type){
            // char/string
            case BcfRecordType.Char:
                info_root[key]=AsdfNode(info.to!string.serializeToAsdf);
                break;
            // float or float array
            case BcfRecordType.Float:
                parseFieldsMixin!(InfoField, float)(info_root, info, key, alleles, [], hRec);
                break;
            // int type or array
            case BcfRecordType.Int8:
            case BcfRecordType.Int16:
            case BcfRecordType.Int32:
            case BcfRecordType.Int64:
                parseFieldsMixin!(InfoField, long)(info_root, info, key, alleles, [], hRec);
                break;
            case BcfRecordType.Null:
                info_root[key]=AsdfNode("null".parseJson);
                break;
        }
    }
    return cast(Asdf) info_root;
}

Asdf parseFormatFields(VCFRecord record) {
    // prepare info root object
    auto format_root=AsdfNode("{}".parseJson);
    auto alleles = record.allelesAsArray();
    auto samples = record.vcfheader.getSamples;
    auto genotypes = record.getGenotypes;
    foreach (sample; samples)
    {
        format_root[sample] = AsdfNode("{}".parseJson);
        format_root[sample]["by_allele"] = AsdfNode("{}".parseJson);
        foreach (allele; alleles[1..$])
        {
            format_root[sample]["by_allele"][allele] = AsdfNode("{}".parseJson);
        }
    }
    auto fmts = record.getFormats;
    fmts.remove("GT");
    // Go by each vcf info field and by type
    // convert to native type then to asdf
    //  
    foreach (i,sample; samples) {
        format_root[sample]["GT"]= AsdfNode(genotypes[i].toString.serializeToAsdf);
    }
    foreach (key, fmt; fmts)
    {
        auto hRec = record.vcfheader.getHeaderRecord(HeaderRecordType.Format, key);
        
        final switch(fmt.type){
            // char/string
            case BcfRecordType.Char:
                auto vals = fmt.to!string;
                foreach (i,sample; samples) {
                    format_root[sample][key]= AsdfNode(vals[i][0].serializeToAsdf);
                }
                break;
            // float or float array
            case BcfRecordType.Float:
                parseFieldsMixin!(FormatField, float)(format_root, fmt, key, alleles, samples, hRec);
                break;
            // int type or array
            case BcfRecordType.Int8:
            case BcfRecordType.Int16:
            case BcfRecordType.Int32:
            case BcfRecordType.Int64:
                parseFieldsMixin!(FormatField, long)(format_root, fmt, key, alleles, samples, hRec);
                break;
            case BcfRecordType.Null:
                format_root[key]=AsdfNode("null".parseJson);
                break;
        }
    }
    return cast(Asdf) format_root;
}

void parseFieldsMixin(T, V)(ref AsdfNode root, ref T item, string key, string[] alleles, string[] samples, HeaderRecord hRec) {
    static if(is(T == FormatField)) {
        auto itemLen = item.n;
        alias vtype = V;
    } else {
        auto itemLen = item.len;
        alias vtype = V[];
    }
    switch(hRec.lenthType){
        case HeaderLengths.OnePerAllele:
            if(alleles.length != itemLen) {
                hts_log_warning(__FUNCTION__,T.stringof ~" "~key~" doesn't have same number of values as header indicates! Skipping...");
                break;
            }
            auto vals = item.to!vtype;
            static if(is(T == FormatField)) {
                foreach (i,val; vals.enumerate)
                {
                    auto sam = samples[i];
                    auto parent = root[sam]["by_allele"];
                    for(auto j=0; j< itemLen; j++){
                        if (j==0) continue;
                        parent[alleles[j]][key] = AsdfNode([val[0], val[j]].serializeToAsdf);
                    }
                }
            } else {
                foreach (i,val; vals)
                {
                    if (i==0) continue;
                    root["by_allele"][alleles[i]][key] = AsdfNode([vals[0], val].serializeToAsdf);
                }
            }     
            break;
        case HeaderLengths.OnePerAltAllele:
            if((alleles.length - 1) != itemLen) {
                hts_log_warning(__FUNCTION__,"Format field "~key~" doesn't have same number of values as header indicates! Skipping...");
                break;
            }
            auto vals = item.to!vtype;
            static if(is(T == FormatField)) {
                foreach (i,val; vals.enumerate)
                {
                    auto sam = samples[i];
                    auto parent = root[sam]["by_allele"];
                    for(auto j=0; j< itemLen; j++){
                        parent[alleles[j+1]][key] = AsdfNode(val[j].serializeToAsdf);
                    }
                }
            } else {
                foreach (i,val; vals)
                {
                    root["by_allele"][alleles[i+1]][key] = AsdfNode(val.serializeToAsdf);
                }
            }
            
            break;
        default:
            auto vals = item.to!vtype;
            static if(is(T == FormatField)) {
                foreach (i,val; vals.enumerate)
                {
                    auto sam = samples[i];
                    for(auto j=0; j< itemLen; j++){
                        if(val.length == 1)
                            root[sam][key] = AsdfNode(val[0].serializeToAsdf);
                        else
                            root[sam][key] = AsdfNode(val.serializeToAsdf);
                    }
                }
            } else {
                if(itemLen > 1)
                    root[key]=AsdfNode(vals.serializeToAsdf);
                else
                    root[key]=AsdfNode(vals[0].serializeToAsdf);
            }
            break;
    }
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

