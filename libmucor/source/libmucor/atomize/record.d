module libmucor.atomize.record;
import libmucor.atomize.fmt;
import libmucor.atomize.info;
import libmucor.atomize.header;
import libmucor.serde;

import std.math : isNaN;
import std.string : fromStringz;

import dhtslib.vcf;
import htslib.vcf;
import dhtslib.coordinates;

import mir.ser;
import mir.bignum.integer;
import mir.utility : _expect;

auto hashAndFinalize(ref VcfRecordSerializer serializer, size_t s) @nogc nothrow @safe {
    import core.stdc.stdlib : malloc, free;

    if(_expect(serializer.calculateHash, true)) {
        Buffer!ubyte d;

        d ~= cast(ubyte[])serializer.symbols.getRawSymbols[];
        d ~= serializer.serializer.data[s .. $];
        
        serializer.putKey("checksum");
        serializeValue(serializer.serializer, hashIon(d[]));
        d.deallocate;
    }
    serializer.structEnd(s);
}

struct VcfRequiredFields(bool singleSample, bool singleAlt)
{

    const(char)[] chrom;

    long pos;

    const(char)[] id;

    const(char)[] ref_;

    static if (singleAlt)
        const(char)[] alt;
    else
        Buffer!(const(char)[]) alt;

    float qual;

    Buffer!(const(char)[]) filter;

    static if (singleSample)
    {
        const(char)[] sample;
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
        serializer.putValue(this.ref_);

        serializer.putKey("ALT");
        static if (singleAlt)
        {
            serializer.putValue(this.alt);
        }
        else
        {
            auto l = serializer.listBegin;
            foreach (ref const(char)[] key; this.alt)
            {
                serializer.putValue(key);
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
        foreach (ref const(char)[] key; filter)
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

struct VcfRec(bool singleSample, bool singleAlt, bool noSample)
{
    alias ReqFields = VcfRequiredFields!(singleSample, singleAlt);
    ReqFields required;
    alias required this;

    static if(singleAlt)
        InfoSingleAlt info;
    else
        Info info;

    static if(singleSample && singleAlt)
        FmtSingleAlt fmt;
    else static if(singleSample) 
        FmtSingleSample fmt;
    else static if(!noSample)
        Fmt fmt;
    
    HeaderConfig hdrInfo;
    

    static if(!singleAlt && !singleSample) {
        this(VCFHeader hdr)
        {
            this.hdrInfo = HeaderConfig(hdr);
            this.info = Info(this.hdrInfo);
            static if(!noSample) this.fmt = Fmt(this.hdrInfo);
        }

        void parse(VCFRecord rec)
        {

            this.chrom = fromStringz(bcf_hdr_id2name(rec.vcfheader.hdr, rec.line.rid));
            this.pos = rec.pos.to!OB.pos;
            this.id = fromStringz(rec.line.d.id);
            this.ref_ = fromStringz(rec.line.d.als);
            this.alt.length = rec.line.n_allele - 1;        // n=0, no reference; n=1, ref but no alt
            foreach(int i; 0 .. rec.line.n_allele - 1) // ref allele is index 0
            {
                this.alt[i] = fromStringz(rec.line.d.allele[i + 1]);
            }
            this.qual = rec.line.qual;
            this.filter.length = rec.line.d.n_flt;
            for(int i; i< rec.line.d.n_flt; i++) {
                this.filter[i] = fromStringz(rec.vcfheader.hdr.id[BCF_DT_ID][ rec.line.d.flt[i] ].key);
            }
            this.info.parse(rec);
            static if(!noSample) this.fmt.parse(rec);
        }
    }

    static if(singleSample && !singleAlt) {
        this(VcfRec!(false, false, noSample) rec, size_t samIdx)
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
            static if(!noSample) this.fmt = FmtSingleSample(rec.fmt, samIdx);
            this.hdrInfo = rec.hdrInfo;
        }
    }

    static if(singleSample && singleAlt) {
        this(VcfRec!(true, false, noSample) rec, size_t altIdx)
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
            static if(!noSample) this.fmt = FmtSingleAlt(rec.fmt, altIdx);
            this.hdrInfo = rec.hdrInfo;
        }
    }

    static if(singleAlt && noSample) {
        this(VcfRec!(false, false, true) rec, size_t altIdx)
        {
            this.chrom = rec.chrom;
            this.pos = rec.pos;
            this.id = rec.id;
            this.ref_ = rec.ref_;
            this.alt = rec.alt[altIdx];
            this.qual = rec.qual;
            this.filter = rec.filter;
            this.info = InfoSingleAlt(rec.info, altIdx, this.alt);
            
            this.hdrInfo = rec.hdrInfo;
        }
    }

    void serialize(ref VcfRecordSerializer serializer)
    {
        auto s = serializer.structBegin;
        this.required.serialize(serializer);

        serializer.putKey("INFO");
        info.serialize(serializer);

        static if(!noSample) {
            serializer.putKey("FORMAT");
            fmt.serialize(serializer);
        }

        hashAndFinalize(serializer, s);
    }
}

alias FullVcfRec = VcfRec!(false, false, false);
alias VcfRecNoSample = VcfRec!(false, false, true);

alias VcfRecSingleAltNoSample = VcfRec!(false, true, true);

alias VcfRecSingleSample = VcfRec!(true, false, false);
alias VcfRecSingleAlt = VcfRec!(true, true, false);

unittest
{
    import std.stdio;
    import libmucor.serde;
    import mir.ion.conv;
    

    auto vcf = VCFReader("test/data/vcf_file.vcf", -1, UnpackLevel.All);

    auto hdrInfo = HeaderConfig(vcf.vcfhdr);
    auto rec = vcf.front;

    auto res1 = `{CHROM:'1',POS:3000150,REF:"C",ALT:["T"],QUAL:59.2,FILTER:[PASS],INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{A:{GT:"0/1",GQ:245},B:{GT:"0/1",GQ:245}}}`;
    auto res2 = `{CHROM:'1',POS:3000150,REF:"C",ALT:["T"],QUAL:59.2,FILTER:[PASS],sample:A,INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{GT:"0/1",GQ:245}}`;
    auto res3 = `{CHROM:'1',POS:3000150,REF:"C",ALT:["T"],QUAL:59.2,FILTER:[PASS],sample:B,INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{GT:"0/1",GQ:245}}`;
    auto res4 = `{CHROM:'1',POS:3000150,REF:"C",ALT:"T",QUAL:59.2,FILTER:[PASS],sample:A,INFO:{AC:2,AN:4},FORMAT:{GT:"0/1",GQ:245}}`;
    auto res5 = `{CHROM:'1',POS:3000150,REF:"C",ALT:"T",QUAL:59.2,FILTER:[PASS],sample:B,INFO:{AC:2,AN:4},FORMAT:{GT:"0/1",GQ:245}}`;
    auto ionRec = FullVcfRec(vcf.vcfhdr);

    ionRec.parse(rec);
    writeln(serializeVcfToIon(ionRec, hdrInfo, false).ion2text);
    assert(serializeVcfToIon(ionRec, hdrInfo, false).ion2text == res1);
    {
        auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
        assert(serializeVcfToIon(ionRecSS1, hdrInfo, false).ion2text == res2);

        
        auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
        assert(serializeVcfToIon(ionRecSS2, hdrInfo, false).ion2text == res3);

        

        auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
        assert(serializeVcfToIon(ionRecSA1, hdrInfo, false).ion2text == res4);

        
        auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
        assert(serializeVcfToIon(ionRecSA2, hdrInfo, false).ion2text == res5);

        
    }

    vcf.popFront;
    vcf.popFront;
    vcf.popFront;

    res1 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:["T","C"],QUAL:12.6,FILTER:[test],INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{A:{byAllele:[{TT:0},{TT:1}],GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},B:{byAllele:[{TT:0},{TT:1}],GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}}`;
    res2 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:["T","C"],QUAL:12.6,FILTER:[test],sample:A,INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{byAllele:[{TT:0},{TT:1}],GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}}`;
    res3 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:["T","C"],QUAL:12.6,FILTER:[test],sample:B,INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{byAllele:[{TT:0},{TT:1}],GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;
    res4 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:"T",QUAL:12.6,FILTER:[test],sample:A,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:0,GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}}`;
    res5 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:"T",QUAL:12.6,FILTER:[test],sample:B,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:0,GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;
    auto res6 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:"C",QUAL:12.6,FILTER:[test],sample:A,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:1,GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}}`;
    auto res7 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:"C",QUAL:12.6,FILTER:[test],sample:B,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:1,GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;

    rec = vcf.front;
    ionRec.parse(rec);
    assert(serializeVcfToIon(ionRec, hdrInfo, false).ion2text == res1);

    auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
    assert(serializeVcfToIon(ionRecSS1, hdrInfo, false).ion2text == res2);

    auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
    assert(serializeVcfToIon(ionRecSS2, hdrInfo, false).ion2text == res3);

    auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
    assert(serializeVcfToIon(ionRecSA1, hdrInfo, false).ion2text == res4);

    auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
    assert(serializeVcfToIon(ionRecSA2, hdrInfo, false).ion2text == res5);

    auto ionRecSA3 = VcfRecSingleAlt(ionRecSS1, 1);
    assert(serializeVcfToIon(ionRecSA3, hdrInfo, false).ion2text == res6);

    auto ionRecSA4 = VcfRecSingleAlt(ionRecSS2, 1);
    assert(serializeVcfToIon(ionRecSA4, hdrInfo, false).ion2text == res7);
}
