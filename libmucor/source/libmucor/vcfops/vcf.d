module libmucor.vcfops.vcf;

import std.stdio;
import std.string : toStringz, fromStringz;
import std.algorithm : map, each;
import std.array : array, split;
import std.datetime.stopwatch : StopWatch, AutoStart;
import std.math : isNaN;
import std.parallelism;
import core.sync.mutex : Mutex;

import dhtslib.vcf;
import dhtslib.coordinates;
import htslib;
import asdf;
import libmucor.vcfops;
import libmucor.khashl : khashl;
import libmucor.jsonlops;
import libmucor.error;
import libmucor: setup_global_pool;
import std.parallelism : parallel;

auto norm(R)(R range, bool active)
{
    return range.map!((x) {
        if (!active)
            return x;
        else
            return normalize(x);
    });
}

/// Parse VCF to JSONL
void parseVCF(string fn, int threads, ubyte con, ref File output)
{
    setup_global_pool(threads);
    //open vcf
    auto vcf = VCFReader(fn, UnpackLevel.All);

    //get info needed from header 
    auto cfg = getHeaderConfig(vcf.vcfhdr);

    StopWatch sw;
    sw.start;
    int vcf_row_count = 0;
    int output_count = 0;
    // loop over records and parse
    foreach (x; vcf)
    {

        auto obj = parseRecord(x, cfg);
        applyOperations(obj, cast(bool)(con & 1), cast(bool)(con & 2),
                cast(bool)(con & 4), cast(bool)(con & 8), &vcf_row_count, &output_count, output);
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

struct FieldInfo
{
    HeaderTypes t;
    HeaderLengths n;
}

struct HeaderConfig
{
    khashl!(string, FieldInfo) fmts;
    khashl!(string, FieldInfo) infos;
    string[] samples;
}

HeaderConfig getHeaderConfig(VCFHeader header)
{
    import std.stdio;

    HeaderConfig cfg;
    for (auto i = 0; i < header.hdr.nhrec; i++)
    {
        auto hrec = HeaderRecord(header.hdr.hrec[i]);
        if (hrec.recType == HeaderRecordType.Format)
            cfg.fmts[hrec.getID().idup] = FieldInfo(hrec.valueType, hrec.lenthType);
        else if (hrec.recType == HeaderRecordType.Info)
            cfg.infos[hrec.getID().idup] = FieldInfo(hrec.valueType, hrec.lenthType);
    }
    cfg.samples = header.getSamples();
    return cfg;
}

/// Parse individual records to JSON
Json parseRecord(VCFRecord record, HeaderConfig cfg)
{
    record.unpack(UnpackLevel.All);

    // create root json object
    auto root = makeJsonObject;

    // parse standard fields
    root["CHROM"] = record.chrom;
    root["POS"] = record.pos.to!OB.pos;
    root["ID"] = record.id;
    if (!isNaN(record.line.qual)) // ignore if nan
        root["QUAL"] = record.qual;

    // parse ref and alt alleles
    auto alleles = record.allelesAsArray;

    root["REF"] = alleles[0];
    if (alleles.length > 2)
    {
        root["ALT"] = alleles[1 .. $];
    }
    else if (alleles.length > 1)
    {
        root["ALT"] = alleles[1];
    }

    // parse filters if any
    string[] filters;
    for (int i = 0; i < record.line.d.n_flt; i++)
    {
        filters ~= cast(string) fromStringz(
                record.vcfheader.hdr.id[BCF_DT_ID][record.line.d.flt[i]].key);
    }
    if (filters != [])
        root["FILTER"] = filters;

    // prepare info root object
    auto info_root = parseInfos(&record, cfg, alleles.length - 1);

    // Go by each vcf info field and by type
    // convert to native type then to asdf
    //  

    // add info fields to root and 
    // parse any annotation fields

    parseAnnotationField(&info_root, "ANN", ANN_FIELDS[]);
    parseAnnotationField(&info_root, "NMD", LOF_FIELDS[], LOF_TYPES[]);
    parseAnnotationField(&info_root, "LOF", LOF_FIELDS[], LOF_TYPES[]);
    root["INFO"] = info_root;

    // if no samples/format info, write
    if (cfg.samples.length == 0)
    {
        return root;
    }

    // if there are samples
    // go by sample and for each
    // fromat field and convert to native type
    // and then convert to asdf
    // and write one record per sample
    auto fmt_root = parseFormats(&record, cfg, alleles.length - 1, cfg.samples);
    // add root to format and write
    root["FORMAT"] = fmt_root;
    root["type"] = "vcf_record";
    return root;
}
