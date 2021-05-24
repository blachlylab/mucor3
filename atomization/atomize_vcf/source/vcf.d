module vcf;

import std.stdio;
import std.string : toStringz, fromStringz;
import std.algorithm : map;
import std.array : array;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.digest.md : MD5Digest, toHexString;
import std.math : isNaN;

import dhtslib.vcf;
import htslib.vcf;
import htslib.hts;
import asdf;
import fields;


/// Parse VCF to JSONL
void parseVCF(string fn, int threads=4){
    //open vcf
    auto fp = vcf_open(toStringz(fn),"r"c.ptr);
    if(!fp) throw new Exception("Could not open vcf");

    //set extra threads
    hts_set_threads(fp,threads);
    //get header
    auto header = bcf_hdr_read(fp);
    
    //get seqs and number of samples
    auto nsamples = bcf_hdr_nsamples(header);
    int nseqs;
    auto seqs_ptr = bcf_hdr_seqnames(header,&nseqs); 
    auto seqs = seqs_ptr[0..nseqs].map!(x=>fromStringz(x)).array;
    auto samples = header.samples[0..nsamples].map!(x=>fromStringz(x)).array;
    int success = 0;
    //initialize bcf1_t
    // auto b = bcf_init1();
    // b.max_unpack = BCF_UN_ALL;
    StopWatch sw;
    sw.start;
    auto count =0;
    // loop over records and parse
    foreach (b; VCFRange(fp,header))
    {
        // 0 = good, -1 = EOF, -2 = error
        bcf_unpack(b,BCF_UN_ALL);
        parseRecord(b,header,seqs,samples);
        count++;
    }
    stderr.writeln("Avg. time per record: ",sw.peek.total!"usecs"/count," usecs");

}

/// Parse individual records to JSON
void parseRecord(bcf1_t * record,bcf_hdr_t * header,const(char)[][] seqs,char[][] samples){
    // create md5 object
    auto md5 = new MD5Digest();

    // create root json object
    auto root = AsdfNode("{}".parseJson);

    // parse standard fields
    root["CHROM"] = AsdfNode(serializeToAsdf(seqs[record.rid]));
    root["POS"] = AsdfNode(serializeToAsdf(record.pos));
    root["ID"] = AsdfNode(serializeToAsdf(fromStringz(record.d.id)));
    if(!isNaN(record.qual)) // ignore if nan
        root["QUAL"] = AsdfNode(serializeToAsdf(record.qual));
    
    // parse ref and alt alleles
    char[][] vars;
    int n_alleles = record.n_allele;
    bcf_info_t[] infos = record.d.info[0..record.n_info];
    bcf_fmt_t[] fmts = record.d.fmt[0..record.n_fmt];
    for(int i =0;i < n_alleles;i++){
        vars~=fromStringz(record.d.allele[i]);
    }
    root["REF"] = AsdfNode(serializeToAsdf(vars[0]));
    if(vars.length>2){
        root["ALT"] = AsdfNode(serializeToAsdf(vars[1..$]));
    }else if(vars.length>1){
        root["ALT"] = AsdfNode(serializeToAsdf(vars[1]));
    }

    // parse filters if any
    const(char)[][] filters;
    for(int i = 0;i < record.d.n_flt;i++){
        filters~=fromStringz(header.id[BCF_DT_ID][ record.d.flt[i]].key);
    }
    if(filters != [])
        root["FILTER"] = AsdfNode(serializeToAsdf(filters));

    // prepare info root object
    auto info_root=AsdfNode("{}".parseJson);

    // Go by each vcf info field and by type
    // convert to native type then to asdf
    //  
    enum LOWER_TYPE_MASK =0b1111;
    foreach (info; infos)
    {
        switch(info.type&LOWER_TYPE_MASK){
            // char/string
            case BCF_BT_CHAR:
                info_root[
                    fromStringz(header.id[BCF_DT_ID][info.key].key)
                ]=AsdfNode(serializeToAsdf((cast(char *)info.vptr)[0..info.vptr_len]));
                break;
            // float or float array
            case BCF_BT_FLOAT:
                if(info.len>1)
                    info_root[
                        fromStringz(header.id[BCF_DT_ID][info.key].key)
                    ]=AsdfNode(serializeToAsdf((cast(float *)info.vptr)[0..info.len]));
                else
                    info_root[
                        fromStringz(header.id[BCF_DT_ID][info.key].key)
                    ]=AsdfNode(serializeToAsdf(info.v1.f));
                break;
            // byte or byte array
            case BCF_BT_INT8:
                if(info.len>1)
                    info_root[
                        fromStringz(header.id[BCF_DT_ID][info.key].key)
                    ]=AsdfNode(serializeToAsdf(info.vptr[0..info.len]));
                else
                    info_root[
                        fromStringz(header.id[BCF_DT_ID][info.key].key)
                    ]=AsdfNode(serializeToAsdf(info.v1.i));
                break;
            // short or short array
            case BCF_BT_INT16:
                if(info.len>1)
                    info_root[
                        fromStringz(header.id[BCF_DT_ID][info.key].key)
                    ]=AsdfNode(serializeToAsdf((cast(short *)info.vptr)[0..info.len]));
                else
                    info_root[
                        fromStringz(header.id[BCF_DT_ID][info.key].key)
                    ]=AsdfNode(serializeToAsdf(info.v1.i));
                break;
            // int or int array
            case BCF_BT_INT32:
                if(info.len>1)
                    info_root[
                        fromStringz(header.id[BCF_DT_ID][info.key].key)
                    ]=AsdfNode(serializeToAsdf((cast(int *)info.vptr)[0..info.len]));
                else
                    info_root[
                        fromStringz(header.id[BCF_DT_ID][info.key].key)
                    ]=AsdfNode(serializeToAsdf(info.v1.i));
                break;
            default:
                break;
        }
        //root[header.id[BCF_DT_ID][info.id]]=
    }

    // add info fields to root and 
    // parse any annotation fields
    root["INFO"] = info_root;
    root =parseAnnotationField(root,"ANN",ANN_FIELDS[]);
    root =parseAnnotationField(root,"NMD",LOF_FIELDS[],LOF_TYPES[]);
    root =parseAnnotationField(root,"LOF",LOF_FIELDS[],LOF_TYPES[]);

    // if no samples/format info, write
    if(samples.length==0){
        root["md5"] = AsdfNode(serializeToAsdf(md5.digest((cast(Asdf) root).data).toHexString));
        writeln(cast(Asdf)root);
    }

    // if there are samples
    // go by sample and for each
    // fromat field and convert to native type
    // and then convert to asdf
    // and write one record per sample
    foreach(i,sample;samples){
        auto fmt_root=AsdfNode("{}".parseJson);
        auto has_allele =false;
        for(int j=0;j<fmts[0].n;j++){
            if(fmts[0].p[i*fmts[0].n+j]!=0){
                has_allele=true;
                break;
            }
        }
        if(!has_allele) continue;
        foreach (fmt; fmts){
            switch(fmt.type&LOWER_TYPE_MASK){
                case BCF_BT_CHAR:
                    fmt_root[
                        fromStringz(header.id[BCF_DT_ID][fmt.id].key)
                    ]=AsdfNode(serializeToAsdf((cast(char *)fmt.p)[i*fmt.n..(i+1)*fmt.n]));
                    break;
                case BCF_BT_FLOAT:
                    if(fmt.n>1)
                        fmt_root[
                            fromStringz(header.id[BCF_DT_ID][fmt.id].key)
                        ]=AsdfNode(serializeToAsdf((cast(float *)fmt.p)[i*fmt.n..(i+1)*fmt.n]));
                    else
                        fmt_root[
                            fromStringz(header.id[BCF_DT_ID][fmt.id].key)
                        ]=AsdfNode(serializeToAsdf((cast(float *)fmt.p)[i*fmt.n]));
                    break;
                case BCF_BT_INT8:
                    if(fmt.n>1)
                        fmt_root[
                            fromStringz(header.id[BCF_DT_ID][fmt.id].key)
                        ]=AsdfNode(serializeToAsdf(fmt.p[i*fmt.n..(i+1)*fmt.n]));
                    else
                        fmt_root[
                            fromStringz(header.id[BCF_DT_ID][fmt.id].key)
                        ]=AsdfNode(serializeToAsdf(fmt.p[i*fmt.n]));
                    break;
                case BCF_BT_INT16:
                    if(fmt.n>1)
                        fmt_root[
                            fromStringz(header.id[BCF_DT_ID][fmt.id].key)
                        ]=AsdfNode(serializeToAsdf((cast(short *)fmt.p)[i*fmt.n..(i+1)*fmt.n]));
                    else
                        fmt_root[
                            fromStringz(header.id[BCF_DT_ID][fmt.id].key)
                        ]=AsdfNode(serializeToAsdf((cast(short *)fmt.p)[i*fmt.n]));
                    break;
                case BCF_BT_INT32:
                    if(fmt.n>1)
                        fmt_root[
                            fromStringz(header.id[BCF_DT_ID][fmt.id].key)
                        ]=AsdfNode(serializeToAsdf((cast(int *)fmt.p)[i*fmt.n..(i+1)*fmt.n]));
                    else
                        fmt_root[
                            fromStringz(header.id[BCF_DT_ID][fmt.id].key)
                        ]=AsdfNode(serializeToAsdf((cast(int *)fmt.p)[i*fmt.n]));
                    break;
                default:
                    break;
            }
        }

        // add root to format and write
        fmt_root["sample"]=AsdfNode(serializeToAsdf(sample));
        fmt_root.add(cast(Asdf)root);
        fmt_root["md5"] = AsdfNode(serializeToAsdf(md5.digest((cast(Asdf) fmt_root).data).toHexString));
        writeln(cast(Asdf)fmt_root);
    }
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

