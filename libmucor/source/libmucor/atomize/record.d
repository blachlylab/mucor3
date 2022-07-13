module libmucor.atomize.record;
import mir.ser;
import mir.ser.interfaces;

import std.math : isNaN;

import dhtslib.vcf;
import libmucor.atomize.fmt;
import libmucor.atomize.info;
import libmucor.atomize.ann;
import libmucor.atomize.header;
import libmucor.atomize.serializer;

struct VcfRec {
    @serdeAnnotation
    @serdeKeys("CHROM")
    string chrom;

    @serdeKeys("POS")
    long pos;

    @serdeKeys("ID")
    @serdeIgnoreOutIf!`a == "."`
    string id;

    @serdeAnnotation
    @serdeKeys("REF")
    string ref_;

    @serdeAnnotation
    @serdeKeys("ALT")
    string[] alt;

    @serdeIgnoreOutIf!isNaN
    @serdeKeys("QUAL")
    float qual;

    @serdeAnnotation
    @serdeKeys("FILTER")
    string[] filter;

    @serdeKeys("INFO")
    Info info;

    @serdeKeys("FORMAT")
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

    size_t[3] partialSerialize(S)(ref S serializer) {
        auto w = serializer.annotationWrapperBegin;
        serializer.putAnnotation(this.chrom);
        serializer.putAnnotation(this.ref_);
        foreach (string key; this.alt)
        {
            serializer.putAnnotation(key);
        }

        foreach (string key; filter)
        {
            serializer.putAnnotation(key);    
        }
        auto a = serializer.annotationsEnd(w);
        auto s = serializer.structBegin;
        serializer.putKey("POS");
        serializer.putValue(pos);

        if(this.id != ".") {
            serializer.putKey("ID");
            serializer.putValue(id);
        }
        if(!isNaN(this.qual)) {
            serializer.putKey("QUAL");
            serializer.putValue(qual);
        }
        serializer.putKey("INFO");
        info.serialize(serializer);

        serializer.putKey("FORMAT");
        fmt.serialize(serializer);

        return [s, a, w];
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
    @serdeAnnotation
    @serdeKeys("CHROM")
    string chrom;
    
    @serdeKeys("POS")
    long pos;

    @serdeIgnoreOutIf!`a == "."`
    @serdeKeys("ID")
    string id;

    @serdeAnnotation
    @serdeKeys("REF")
    string ref_;

    @serdeAnnotation
    @serdeKeys("ALT")
    string[] alt;

    @serdeIgnoreOutIf!isNaN
    @serdeKeys("QUAL")
    float qual;

    @serdeAnnotation
    @serdeKeys("FILTER")
    string[] filter;

    @serdeAnnotation
    string sample;

    @serdeKeys("INFO")
    Info info;

    @serdeKeys("FORMAT")
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

    size_t[3] partialSerialize(S)(ref S serializer) {
        auto s = serializer.structBegin;
        auto w = serializer.annotationWrapperBegin;
        serializer.putAnnotation(this.chrom);
        serializer.putAnnotation(this.ref_);
        foreach (string key; this.alt)
        {
            serializer.putAnnotation(key);
        }

        foreach (string key; filter)
        {
            serializer.putAnnotation(key);    
        }

        serializer.putAnnotation(sample);

        auto a = serializer.annotationsEnd(w);

        serializer.putKey("POS");
        serializer.putValue(pos);

        if(this.id != ".") {
            serializer.putKey("ID");
            serializer.putValue(id);
        }
        if(!isNaN(this.qual)) {
            serializer.putKey("QUAL");
            serializer.putValue(qual);
        }
        serializer.putKey("INFO");
        serializeValue(serializer, info);

        serializer.putKey("FORMAT");
        serializeValue(serializer, fmt);

        return [s, a, w];
    }
}

struct VcfRecSingleAlt {
    @serdeAnnotation
    @serdeKeys("CHROM")
    string chrom;

    @serdeKeys("POS")
    long pos;

    @serdeKeys("ID")
    @serdeIgnoreOutIf!`a == "."`
    string id;

    @serdeAnnotation
    @serdeKeys("REF")
    string ref_;

    @serdeAnnotation
    @serdeKeys("ALT")
    string alt;

    @serdeIgnoreOutIf!isNaN
    @serdeKeys("QUAL")
    float qual;

    @serdeAnnotation
    @serdeKeys("FILTER")
    string[] filter;
    
    @serdeAnnotation
    string sample;

    @serdeKeys("INFO")
    InfoSingleAlt info;

    @serdeKeys("FORMAT")
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

    size_t[3] partialSerialize(S)(ref S serializer) {
        auto s = serializer.structBegin;
        auto w = serializer.annotationWrapperBegin;
        serializer.putAnnotation(this.chrom);
        serializer.putAnnotation(this.ref_);
        serializer.putAnnotation(this.alt);

        foreach (string key; filter)
        {
            serializer.putAnnotation(key);    
        }

        serializer.putAnnotation(sample);
        auto a = serializer.annotationsEnd(w);
        serializer.putKey("POS");
        serializer.putValue(pos);

        if(this.id != ".") {
            serializer.putKey("ID");
            serializer.putValue(id);
        }
        if(!isNaN(this.qual)) {
            serializer.putKey("QUAL");
            serializer.putValue(qual);
        }
        serializer.putKey("INFO");
        serializeValue(serializer, info);

        serializer.putKey("FORMAT");
        serializeValue(serializer, fmt);

        return [s, a, w];
    }
}

unittest {
    import mir.ser.ion;
    import mir.ion.conv;
    import std.stdio;

    auto vcf = VCFReader("../test/data/vcf_file.vcf",-1, UnpackLevel.All);

    auto rec = vcf.front;

    auto res1 = `'1'::C::T::PASS::{POS:3000149,QUAL:59.2,INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{A:{GT:"0/1",GQ:245},B:{GT:"0/1",GQ:245}}}`;
    auto res2 = `'1'::C::T::PASS::A::{POS:3000149,QUAL:59.2,INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{GT:"0/1",GQ:245}}`;
    auto res3 = `'1'::C::T::PASS::B::{POS:3000149,QUAL:59.2,INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{GT:"0/1",GQ:245}}`;
    auto res4 = `'1'::C::T::PASS::A::{POS:3000149,QUAL:59.2,INFO:{AC:2,AN:4},FORMAT:{GT:"0/1",GQ:245}}`;
    auto res5 = `'1'::C::T::PASS::B::{POS:3000149,QUAL:59.2,INFO:{AC:2,AN:4},FORMAT:{GT:"0/1",GQ:245}}`;
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

    res1 = `'1'::G::T::C::test::{POS:3062914,ID:"idSNP",QUAL:12.6,INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{A:{byAllele:[{TT:0},{TT:1}],GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},B:{byAllele:[{TT:0},{TT:1}],GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}}`;
    res2 = `'1'::G::T::C::test::A::{POS:3062914,ID:"idSNP",QUAL:12.6,INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{byAllele:[{TT:0},{TT:1}],GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}}`;
    res3 = `'1'::G::T::C::test::B::{POS:3062914,ID:"idSNP",QUAL:12.6,INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{byAllele:[{TT:0},{TT:1}],GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;
    res4 = `'1'::G::T::test::A::{POS:3062914,ID:"idSNP",QUAL:12.6,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:0,GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}}`;
    res5 = `'1'::G::T::test::B::{POS:3062914,ID:"idSNP",QUAL:12.6,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:0,GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;
    auto res6 = `'1'::G::C::test::A::{POS:3062914,ID:"idSNP",QUAL:12.6,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:1,GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}}`;
    auto res7 = `'1'::G::C::test::B::{POS:3062914,ID:"idSNP",QUAL:12.6,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:1,GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;
    
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