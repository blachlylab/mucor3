module libmucor.atomize.fmt;

import libmucor.atomize.field;
import libmucor.atomize.header;
import libmucor.atomize.genotype;
import libmucor.atomize.util;
import libmucor.serde.ser;

import std.algorithm : map, any;

import dhtslib.vcf: VCFRecord, HeaderLengths, BcfRecordType, UnpackLevel;
import htslib: bcf_fmt_t;
import memory;

/** 
 * sam1: {
 *      byAllele: {...}
 *      DP: ...,
 * }
 */
struct FmtSampleValues
{
    Buffer!(Buffer!FmtField) byAllele;
    Genotype gt;
    Buffer!FmtField other;

    void reset() @nogc nothrow @safe
    {
        this.gt.reset();
        byAllele[] = Buffer!FmtField.init;
        // foreach (ref v; byAllele)
        // {
        //     v.deallocate();
        // }
        this.byAllele.length = 0;
        this.other.length = 0;
    }

    void serialize(ref VcfRecordSerializer serializer) @nogc nothrow @safe
    {
        auto state = serializer.structBegin;
        if(this.byAllele.length) {
            serializer.putKey("byAllele");
            auto state2 = serializer.listBegin;
            foreach (ref ba; this.byAllele)
            {
                auto state3 = serializer.structBegin;
                foreach (ref v; ba)
                    v.serialize(serializer);
                serializer.structEnd(state3);
            }
            serializer.listEnd(state2);
        }

        serializer.putKey("GT");
        gt.serialize(serializer);

        foreach (ref val; this.other)
        {
            val.serialize(serializer);
        }
        serializer.structEnd(state);
    }
}
/** 
 * FORMAT: {
 *      sam1: {...}
 *      sam2: {...}
 * }
 */
struct Fmt
{
    Buffer!FmtSampleValues bySample;
    const(HeaderConfig) cfg;

    this(HeaderConfig cfg) @nogc nothrow @safe
    {
        this.bySample.length = cfg.samples.length;
        this.cfg = cfg;
    }

    void reset() @nogc nothrow @safe
    {
        foreach (ref sam; bySample)
        {
            sam.reset;
        }
    }

    void parse(ref VCFRecord rec) @nogc nothrow @trusted
    {
        // unpack(rec, UnpackLevel.All);
        
        this.reset;
        
        assert(this.bySample.length == rec.line.n_sample);
        if (rec.line.n_fmt == 0)
            return;
        /// get genotype first
        /// loop over samples and get genotypes
        for (auto i = 0; i < rec.line.n_sample; i++)
        {
            this.bySample[i].gt = Genotype(&rec.line.d.fmt[0], i);
        }

        /// loop over fmts
        foreach (t, bcf_fmt_t v; rec.line.d.fmt[1 .. rec.line.n_fmt])
        {
            auto idx = cfg.getIdx(v.id);
            auto hdrInfo = cfg.getFmt(v.id);

            for (auto i = 0; i < rec.line.n_sample; i++)
            {
                if(this.bySample[i].gt.isNullOrRef) continue;
                final switch(hdrInfo.n) {
                    case HeaderLengths.OnePerAllele:
                        if(!this.bySample[i].byAllele.length)
                            this.bySample[i].byAllele.length = rec.line.n_allele - 1;

                        for(auto j=0; j < this.bySample[i].byAllele.length; j++) {
                            this.bySample[i].byAllele[j] ~= FmtField(&v, cfg.fmts.byAllele.names[idx], i, j, true);
                        }
                        break;
                    case HeaderLengths.OnePerAltAllele:
                        if(!this.bySample[i].byAllele.length)
                            this.bySample[i].byAllele.length = rec.line.n_allele - 1;

                        for(auto j=0; j < this.bySample[i].byAllele.length; j++) {
                            this.bySample[i].byAllele[j] ~= FmtField(&v, cfg.fmts.byAllele.names[idx], i, j);
                        }
                        break;
                    case HeaderLengths.OnePerGenotype:
                    case HeaderLengths.Fixed:
                    case HeaderLengths.None:
                    case HeaderLengths.Variable:
                        this.bySample[i].other ~= FmtField(&v, cfg.fmts.other.names[idx], i);
                        break;
                }
            }
        }
    }

    void serialize(ref VcfRecordSerializer serializer) @nogc nothrow @safe
    {
        auto state = serializer.structBegin;

        foreach (i, string key; cfg.samples)
        {
            if (this.bySample[i].gt.isNullOrRef)
                continue;
            serializer.putKey(key);
            this.bySample[i].serialize(serializer);
        }
        serializer.structEnd(state);
    }
}

struct FmtSingleSample
{
    FmtSampleValues sampleValues;
    const(HeaderConfig) cfg;

    this(Fmt fmt, size_t samIdx) @nogc nothrow @safe
    {
        this.sampleValues = fmt.bySample[samIdx];
        this.cfg = fmt.cfg;
    }

    void serialize(S)(ref S serializer) @nogc nothrow @safe
    {
        this.sampleValues.serialize(serializer);
    }
}

struct FmtSingleAlt
{
    Buffer!FmtField alleleValues;
    Genotype gt;
    Buffer!FmtField other;
    const(HeaderConfig) cfg;

    this(FmtSingleSample fmt, size_t altIdx) @nogc nothrow @safe
    {
        if(fmt.sampleValues.byAllele.length)
            this.alleleValues = fmt.sampleValues.byAllele[altIdx];
        this.gt = fmt.sampleValues.gt;
        this.other = fmt.sampleValues.other;
        this.cfg = fmt.cfg;
    }

    void serialize(ref VcfRecordSerializer serializer) @nogc nothrow @safe
    {
        auto state = serializer.structBegin;
        foreach (i, val; this.alleleValues)
        {
            val.serialize(serializer);
        }

        serializer.putKey("GT");
        gt.serialize(serializer);

        foreach (i, val; this.other)
        {
            val.serialize(serializer);
        }

        serializer.structEnd(state);
    }
}

unittest
{
    import std.stdio;
    import libmucor.serde;
    import libmucor.atomize.record;
    import mir.ion.conv;
    import dhtslib.vcf : VCFReader;
    

    auto vcf = VCFReader("test/data/vcf_file.vcf", -1, UnpackLevel.All);
    
    auto hdrInfo = HeaderConfig(vcf.vcfhdr);
    vcf.popFront;
    vcf.popFront;
    vcf.popFront;
    auto ionRec = FullVcfRec(vcf.vcfhdr);

    auto res1 = `{A:{byAllele:[{TT:0},{TT:1}],GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},B:{byAllele:[{TT:0},{TT:1}],GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;
    auto res2 = `{byAllele:[{TT:0},{TT:1}],GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}`;
    auto res3 = `{byAllele:[{TT:0},{TT:1}],GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}`;
    auto res4 = `{TT:0,GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}`;
    auto res5 = `{TT:0,GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}`;
    auto res6 = `{TT:1,GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}`;
    auto res7 = `{TT:1,GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}`;

    auto rec = vcf.front;
    ionRec.parse(rec);
    assert(serializeVcfToIon(ionRec.fmt, hdrInfo, false).ion2text == res1);
    {
        auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
        assert(serializeVcfToIon(ionRecSS1.fmt, hdrInfo, false).ion2text == res2);

        auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
        assert(serializeVcfToIon(ionRecSS2.fmt, hdrInfo, false).ion2text == res3);

        auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
        assert(serializeVcfToIon(ionRecSA1.fmt, hdrInfo, false).ion2text == res4);

        auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
        assert(serializeVcfToIon(ionRecSA2.fmt, hdrInfo, false).ion2text == res5);

        auto ionRecSA3 = VcfRecSingleAlt(ionRecSS1, 1);
        assert(serializeVcfToIon(ionRecSA3.fmt, hdrInfo, false).ion2text == res6);

        auto ionRecSA4 = VcfRecSingleAlt(ionRecSS2, 1);
        assert(serializeVcfToIon(ionRecSA4.fmt, hdrInfo, false).ion2text == res7);
    }
    vcf.popFront;

    rec = vcf.front;

    res1 = `{A:{GT:"0/1",GQ:245,DP:32},B:{GT:"0/1",GQ:245,DP:32}}`;
    res2 = `{GT:"0/1",GQ:245,DP:32}`;
    res3 = `{GT:"0/1",GQ:245,DP:32}`;
    res4 = `{GT:"0/1",GQ:245,DP:32}`;
    res5 = `{GT:"0/1",GQ:245,DP:32}`;

    ionRec.parse(rec);
    writeln(serializeVcfToIon(ionRec.fmt, hdrInfo, false).ion2text);
    assert(serializeVcfToIon(ionRec.fmt, hdrInfo, false).ion2text == res1);
    {
        auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
        assert(serializeVcfToIon(ionRecSS1.fmt, hdrInfo, false).ion2text == res2);

        
        auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
        assert(serializeVcfToIon(ionRecSS2.fmt, hdrInfo, false).ion2text == res3);

        

        auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
        assert(serializeVcfToIon(ionRecSA1.fmt, hdrInfo, false).ion2text == res4);

        
        auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
        assert(serializeVcfToIon(ionRecSA2.fmt, hdrInfo, false).ion2text == res5);

        
    }
}
