module libmucor.atomize.record;
import mir.ser;
import mir.ser.interfaces;
import mir.bignum.integer;

import std.math : isNaN;

import dhtslib.vcf;
import dhtslib.coordinates;
import libmucor.atomize.fmt;
import libmucor.atomize.info;
import libmucor.atomize.ann;
import libmucor.atomize.header;
import libmucor.serde.ser;
import libmucor.serde;

struct VcfRequiredFields(bool singleSample, bool singleAlt)
{

    @serdeKeys("CHROM")
    string chrom;

    @serdeKeys("POS")
    long pos;

    @serdeKeys("ID")
    @serdeIgnoreOutIf!`a == "."` string id;

    @serdeKeys("REF")
    string ref_;

    @serdeKeys("ALT")
    static if (singleAlt)
        string alt;
    else
        string[] alt;

    @serdeIgnoreOutIf!isNaN @serdeKeys("QUAL")
    float qual;

    @serdeKeys("FILTER")
    string[] filter;

    static if (singleSample)
    {
        string sample;
    }

    BigInt!2 checksum;

    void serialize(ref VcfRecordSerializer serializer)
    {
        serializer.putKey("CHROM");
        serializer.putSymbol(this.chrom);

        serializer.putKey("POS");
        serializer.putValue(pos);

        if (this.id != ".")
        {
            serializer.putKey("ID");
            serializer.putSymbol(id);
        }

        serializer.putKey("REF");
        serializer.putSymbol(this.ref_);

        serializer.putKey("ALT");
        static if (singleAlt)
        {
            serializer.putSymbol(this.alt);
        }
        else
        {
            auto l = serializer.listBegin;
            foreach (ref string key; this.alt)
            {
                serializer.putSymbol(key);
            }
            serializer.listEnd(l);
        }

        if (!isNaN(this.qual))
        {
            serializer.putKey("QUAL");
            serializer.putValue(qual);
        }

        serializer.putKey("FILTER");
        auto l2 = serializer.listBegin;
        foreach (ref string key; filter)
        {
            serializer.putSymbol(key);
        }
        serializer.listEnd(l2);

        static if (singleSample)
        {
            serializer.putKey("sample");
            serializer.putSymbol(this.sample);
        }
    }
}

struct VcfRec
{
    alias ReqFields = VcfRequiredFields!(false, false);
    ReqFields required;
    alias required this;

    @serdeKeys("INFO")
    Info info;

    @serdeKeys("FORMAT")
    Fmt fmt;
    // Annotations[] anns;
    @serdeIgnoreOut HeaderConfig hdrInfo;

    this(VCFHeader hdr)
    {
        this.hdrInfo = HeaderConfig(hdr);
        this.info = Info(this.hdrInfo);
        this.fmt = Fmt(this.hdrInfo);
    }

    void parse(VCFRecord rec)
    {
        import std.array : split;

        this.chrom = rec.chrom;
        this.pos = rec.pos.to!OB.pos;
        this.id = rec.id;
        this.ref_ = rec.refAllele;
        this.alt = rec.altAllelesAsArray;
        this.qual = rec.line.qual;
        this.filter = rec.filter.split(";");
        this.info.parse(rec);
        this.fmt.parse(rec);
    }

    void serialize(ref VcfRecordSerializer serializer)
    {
        auto s = serializer.structBegin;
        this.required.serialize(serializer);

        serializer.putKey("INFO");
        info.serialize(serializer);

        serializer.putKey("FORMAT");
        fmt.serialize(serializer);

        auto last = serializer.serializer.data[s .. $];
        serializer.putKey("checksum");

        serializeValue(serializer.serializer, hashIon(serializer.symbols.getRawSymbols ~ last));
        serializer.structEnd(s);
    }
}

struct VcfRecSingleSample
{
    alias ReqFields = VcfRequiredFields!(true, false);
    @serdeIgnoreOut ReqFields required;
    alias required this;

    @serdeKeys("INFO")
    Info info;

    @serdeKeys("FORMAT")
    FmtSingleSample fmt;
    // Annotations[] anns;
    @serdeIgnoreOut HeaderConfig hdrInfo;

    this(VcfRec rec, size_t samIdx)
    {
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

    void serialize(ref VcfRecordSerializer serializer)
    {
        auto s = serializer.structBegin;
        this.required.serialize(serializer);

        serializer.putKey("INFO");
        info.serialize(serializer);

        serializer.putKey("FORMAT");
        fmt.serialize(serializer);

        auto last = serializer.serializer.data[s .. $];
        serializer.putKey("checksum");

        serializeValue(serializer.serializer, hashIon(serializer.symbols.getRawSymbols ~ last));
        serializer.structEnd(s);
    }
}

struct VcfRecSingleAlt
{
    alias ReqFields = VcfRequiredFields!(true, true);
    @serdeIgnoreOut ReqFields required;
    alias required this;

    @serdeKeys("INFO")
    InfoSingleAlt info;

    @serdeKeys("FORMAT")
    FmtSingleAlt fmt;
    @serdeIgnoreOut HeaderConfig hdrInfo;

    this(VcfRecSingleSample rec, size_t altIdx)
    {
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

    void serialize(ref VcfRecordSerializer serializer)
    {
        auto s = serializer.structBegin;
        this.required.serialize(serializer);

        serializer.putKey("INFO");
        info.serialize(serializer);

        serializer.putKey("FORMAT");
        fmt.serialize(serializer);

        auto last = serializer.serializer.data[s .. $];
        serializer.putKey("checksum");

        serializeValue(serializer.serializer, hashIon(serializer.symbols.getRawSymbols ~ last));
        serializer.structEnd(s);
    }
}

unittest
{
    import std.stdio;
    import libmucor.serde;
    import mir.ion.conv;

    auto vcf = VCFReader("../test/data/vcf_file.vcf", -1, UnpackLevel.All);

    auto hdrInfo = HeaderConfig(vcf.vcfhdr);
    auto rec = vcf.front;

    auto res1 = `{CHROM:'1',POS:3000150,REF:C,ALT:[T],QUAL:59.2,FILTER:[PASS],INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{A:{GT:'0/1',GQ:245},B:{GT:'0/1',GQ:245}},checksum:167696256410866322737776890645209371115}`;
    auto res2 = `{CHROM:'1',POS:3000150,REF:C,ALT:[T],QUAL:59.2,FILTER:[PASS],sample:A,INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{GT:'0/1',GQ:245},checksum:121802908002629222478147552112985116920}`;
    auto res3 = `{CHROM:'1',POS:3000150,REF:C,ALT:[T],QUAL:59.2,FILTER:[PASS],sample:B,INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{GT:'0/1',GQ:245},checksum:205904315182720303206170334056174769153}`;
    auto res4 = `{CHROM:'1',POS:3000150,REF:C,ALT:T,QUAL:59.2,FILTER:[PASS],sample:A,INFO:{AC:2,AN:4},FORMAT:{GT:'0/1',GQ:245},checksum:128754861391790484436424915143308824327}`;
    auto res5 = `{CHROM:'1',POS:3000150,REF:C,ALT:T,QUAL:59.2,FILTER:[PASS],sample:B,INFO:{AC:2,AN:4},FORMAT:{GT:'0/1',GQ:245},checksum:150905171508320205568740822437435841824}`;
    auto ionRec = VcfRec(vcf.vcfhdr);

    ionRec.parse(rec);

    assert(serializeVcfToIon(ionRec, hdrInfo).ion2text == res1);

    {
        auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
        // assert(serializeVcfToIon(ionRecSS1, hdrInfo).ion2text == res2);
        // 
        auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
        // assert(serializeVcfToIon(ionRecSS2, hdrInfo).ion2text == res3);
        // 

        auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
        // assert(serializeVcfToIon(ionRecSA1, hdrInfo).ion2text == res4);
        // 
        auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
        // assert(serializeVcfToIon(ionRecSA2, hdrInfo).ion2text == res5);
        // 
    }

    vcf.popFront;
    vcf.popFront;
    vcf.popFront;

    res1 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:G,ALT:[T,C],QUAL:12.6,FILTER:[test],INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{A:{byAllele:[{TT:0},{TT:1}],GT:'0/1',GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},B:{byAllele:[{TT:0},{TT:1}],GT:'2',GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}},checksum:207971251731807253524814117744582773582}`;
    res2 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:G,ALT:[T,C],QUAL:12.6,FILTER:[test],sample:A,INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{byAllele:[{TT:0},{TT:1}],GT:'0/1',GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},checksum:205101975252432127554312214322397311520}`;
    res3 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:G,ALT:[T,C],QUAL:12.6,FILTER:[test],sample:B,INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{byAllele:[{TT:0},{TT:1}],GT:'2',GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]},checksum:94280738342039847251228534150331534876}`;
    res4 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:G,ALT:T,QUAL:12.6,FILTER:[test],sample:A,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:0,GT:'0/1',GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},checksum:127877221559661713373435823224823837691}`;
    res5 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:G,ALT:T,QUAL:12.6,FILTER:[test],sample:B,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:0,GT:'2',GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]},checksum:175127199109185310019294968658578824505}`;
    auto res6 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:G,ALT:C,QUAL:12.6,FILTER:[test],sample:A,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:1,GT:'0/1',GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},checksum:219863281376588456487662513914302963957}`;
    auto res7 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:G,ALT:C,QUAL:12.6,FILTER:[test],sample:B,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:1,GT:'2',GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]},checksum:89391436317617167887546552862988127933}`;

    rec = vcf.front;
    ionRec.parse(rec);
    assert(serializeVcfToIon(ionRec, hdrInfo).ion2text == res1);

    auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
    assert(serializeVcfToIon(ionRecSS1, hdrInfo).ion2text == res2);

    auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
    assert(serializeVcfToIon(ionRecSS2, hdrInfo).ion2text == res3);

    auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
    assert(serializeVcfToIon(ionRecSA1, hdrInfo).ion2text == res4);

    auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
    assert(serializeVcfToIon(ionRecSA2, hdrInfo).ion2text == res5);

    auto ionRecSA3 = VcfRecSingleAlt(ionRecSS1, 1);
    assert(serializeVcfToIon(ionRecSA3, hdrInfo).ion2text == res6);

    auto ionRecSA4 = VcfRecSingleAlt(ionRecSS2, 1);
    assert(serializeVcfToIon(ionRecSA4, hdrInfo).ion2text == res7);
}
