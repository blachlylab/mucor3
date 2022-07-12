module libmucor.atomize.record;
import mir.ser;

import dhtslib.vcf;
import libmucor.atomize.fmt;
import libmucor.atomize.info;
import libmucor.atomize.ann;
import libmucor.atomize.header;

struct VcfRec {
    @serdeKeys("CHROM", "chrom")
    string chrom;

    @serdeKeys("POS", "pos")
    long pos;

    @serdeKeys("ID", "id")
    string id;

    @serdeKeys("REF", "ref")
    string ref_;

    @serdeKeys("ALT", "alt")
    string[] alt;

    @serdeKeys("QUAL", "qual")
    float qual;

    @serdeKeys("FILTER", "filter")
    string[] filter;

    @serdeKeys("INFO", "info")
    Info info;

    @serdeKeys("FORMAT", "fmt")
    Fmt fmt;
    // Annotations[] anns;
    @serdeIgnoreOut
    HeaderConfig hdrInfo;

    this(VCFHeader hdr) {
        this.hdrInfo = HeaderConfig(hdr);
        this.info = Info(this.hdrInfo);
        this.fmt = Fmt(this.hdrInfo);
    }

    void parse(VCFRecord rec) {
        import std.array : split;
        this.chrom = rec.chrom;
        this.pos = rec.pos.pos;
        this.id = rec.id;
        this.ref_ = rec.refAllele;
        this.alt = rec.altAllelesAsArray;
        this.qual = rec.line.qual;
        this.filter = rec.filter.split(";");
        this.info.parse(rec);
        this.fmt.parse(rec);
    }
}

unittest {
    import mir.ser.ion;
    import mir.ion.conv;
    import std.stdio;

    VcfRec rec;
    rec.chrom = "chr1";
    rec.pos = 1;
    rec.id = ".";
    rec.qual = 1.0;
    rec.filter = ["Test", "notpass"];

    writeln(serializeIon(rec).ion2text);
}

struct VcfRecSingleSample {
    @serdeKeys("CHROM", "chrom")
    string chrom;

    @serdeKeys("POS", "pos")
    long pos;

    @serdeKeys("ID", "id")
    string id;

    @serdeKeys("REF", "ref")
    string ref_;

    @serdeKeys("ALT", "alt")
    string[] alt;

    @serdeKeys("QUAL", "qual")
    float qual;

    @serdeKeys("FILTER", "filter")
    string[] filter;

    string sample;

    @serdeKeys("INFO", "info")
    Info info;

    @serdeKeys("FORMAT", "fmt")
    FmtSingleSample fmt;
    // Annotations[] anns;
    @serdeIgnoreOut
    HeaderConfig hdrInfo;

    this(VcfRec rec, size_t samIdx) {
        this.chrom = rec.chrom;
        this.pos = rec.pos;
        this.id = rec.id;
        this.ref_ = rec.ref_;
        this.alt = rec.alt;
        this.qual = rec.qual;
        this.filter = rec.filter;
        this.sample = rec.hdrInfo.samples[samIdx];
        this.info = rec.info;
        this.fmt = FmtSingleSample(rec.fmt, samIdx);
        this.hdrInfo = rec.hdrInfo;
    }
}

struct VcfRecSingleAlt {
    @serdeKeys("CHROM", "chrom")
    string chrom;

    @serdeKeys("POS", "pos")
    long pos;

    @serdeKeys("ID", "id")
    string id;

    @serdeKeys("REF", "ref")
    string ref_;

    @serdeKeys("ALT", "alt")
    string alt;

    @serdeKeys("QUAL", "qual")
    float qual;

    @serdeKeys("FILTER", "filter")
    string[] filter;
    
    string sample;

    @serdeKeys("INFO", "info")
    InfoSingleAlt info;

    @serdeKeys("FORMAT", "fmt")
    FmtSingleAlt fmt;
    @serdeIgnoreOut
    HeaderConfig hdrInfo;

    this(VcfRecSingleSample rec, size_t altIdx) {
        this.chrom = rec.chrom;
        this.pos = rec.pos;
        this.id = rec.id;
        this.ref_ = rec.ref_;
        this.alt = rec.alt[altIdx];
        this.qual = rec.qual;
        this.filter = rec.filter;
        this.sample = rec.sample;
        this.info = InfoSingleAlt(rec.info, altIdx, this.alt);
        this.fmt = FmtSingleAlt(rec.fmt, altIdx);
        this.hdrInfo = rec.hdrInfo;
    }
}

unittest {
    import mir.ser.ion;
    import mir.ion.conv;
    import std.stdio;

    auto vcf = VCFReader("../test/data/vcf_file.vcf",-1, UnpackLevel.All);

    auto rec = vcf.front;

    auto res1 = `{CHROM:"1",POS:3000149,ID:".",REF:"C",ALT:["T"],QUAL:59.2,FILTER:["PASS"],INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{A:{GT:"0/1",GQ:245},B:{GT:"0/1",GQ:245}}}`;
    auto res2 = `{CHROM:"1",POS:3000149,ID:".",REF:"C",ALT:["T"],QUAL:59.2,FILTER:["PASS"],sample:"A",INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{GT:"0/1",GQ:245}}`;
    auto res3 = `{CHROM:"1",POS:3000149,ID:".",REF:"C",ALT:["T"],QUAL:59.2,FILTER:["PASS"],sample:"B",INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{GT:"0/1",GQ:245}}`;
    auto res4 = `{CHROM:"1",POS:3000149,ID:".",REF:"C",ALT:"T",QUAL:59.2,FILTER:["PASS"],sample:"A",INFO:{AC:2,AN:4},FORMAT:{GT:"0/1",GQ:245}}`;
    auto res5 = `{CHROM:"1",POS:3000149,ID:".",REF:"C",ALT:"T",QUAL:59.2,FILTER:["PASS"],sample:"B",INFO:{AC:2,AN:4},FORMAT:{GT:"0/1",GQ:245}}`;
    auto ionRec = VcfRec(vcf.vcfhdr);

    ionRec.parse(rec);
    
    assert(serializeIon(ionRec).ion2text == res1);

    {
        auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
        assert(serializeIon(ionRecSS1).ion2text == res2);
        auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
        assert(serializeIon(ionRecSS2).ion2text == res3);

        auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
        assert(serializeIon(ionRecSA1).ion2text == res4);
        auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
        assert(serializeIon(ionRecSA2).ion2text == res5);
    }

    vcf.popFront;
    vcf.popFront;
    vcf.popFront;

    res1 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:"G",ALT:["T","C"],QUAL:12.6,FILTER:["test"],INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{A:{byAllele:[{TT:0},{TT:1}],GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},B:{byAllele:[{TT:0},{TT:1}],GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}}`;
    res2 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:"G",ALT:["T","C"],QUAL:12.6,FILTER:["test"],sample:"A",INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{byAllele:[{TT:0},{TT:1}],GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}}`;
    res3 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:"G",ALT:["T","C"],QUAL:12.6,FILTER:["test"],sample:"B",INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{byAllele:[{TT:0},{TT:1}],GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;
    res4 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:"G",ALT:"T",QUAL:12.6,FILTER:["test"],sample:"A",INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:0,GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}}`;
    res5 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:"G",ALT:"T",QUAL:12.6,FILTER:["test"],sample:"B",INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:0,GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;
    auto res6 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:"G",ALT:"C",QUAL:12.6,FILTER:["test"],sample:"A",INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:1,GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}}`;
    auto res7 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:"G",ALT:"C",QUAL:12.6,FILTER:["test"],sample:"B",INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:1,GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;
    
    rec = vcf.front;
    ionRec.parse(rec);
    assert(serializeIon(ionRec).ion2text == res1);

    auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
    assert(serializeIon(ionRecSS1).ion2text == res2);

    auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
    assert(serializeIon(ionRecSS2).ion2text == res3);

    auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
    assert(serializeIon(ionRecSA1).ion2text == res4);

    auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
    assert(serializeIon(ionRecSA2).ion2text == res5);

    auto ionRecSA3 = VcfRecSingleAlt(ionRecSS1, 1);
    assert(serializeIon(ionRecSA3).ion2text == res6);

    auto ionRecSA4 = VcfRecSingleAlt(ionRecSS2, 1);
    assert(serializeIon(ionRecSA4).ion2text == res7);
}