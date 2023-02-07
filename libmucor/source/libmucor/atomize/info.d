module libmucor.atomize.info;

import libmucor.atomize.header;
import libmucor.atomize.field;
import libmucor.atomize.ann;
import libmucor.serde.ser;

import std.algorithm : map, any;

import dhtslib.vcf: VCFRecord, HeaderLengths, BcfRecordType, UnpackLevel, HeaderTypes;
import memory;

struct Info
{
    Annotation _dummy;
    Buffer!(Buffer!InfoField) byAllele;
    Buffer!InfoField other;
    Buffer!Annotations annFields;
    const(HeaderConfig) cfg;
    size_t numByAlleleFields;

    this(HeaderConfig cfg) @nogc nothrow @safe
    {
        this.numByAlleleFields = cfg.infos.byAllele.names.length;
        this.other.length = cfg.infos.other.names.length;
        this.annFields.length = cfg.infos.annotations.names.length;
        this.cfg = cfg;
    }

    void reset() @nogc nothrow @safe
    {
        byAllele[] = Buffer!InfoField.init;
        // foreach (ref v; byAllele)
        // {
        //     v.deallocate();
        // }
        this.byAllele.length = 0;
        other.length = 0;
    }

    void parse(VCFRecord rec) @nogc nothrow @trusted
    {
        import htslib.vcf;

        this.reset;
        /// make sure all arrays are initialized
        this.byAllele.length = rec.line.n_allele - 1;
        
        if (rec.line.n_info == 0)
            return;
        /// loop over infos
        foreach (bcf_info_t v; rec.line.d.info[0 .. rec.line.n_info])
        {
            if (!v.vptr)
                continue;

            auto idx = cfg.getIdx(v.key);
            auto hdrInfo = cfg.getInfo(v.key);
            if (hdrInfo.t == HeaderTypes.Flag) {
                if(cfg.isByAllele[v.key])
                    other ~= InfoField(true, cfg.infos.byAllele.names[idx]);
                else
                    other ~= InfoField(true, cfg.infos.other.names[idx]);
                continue;
            }
            if (cfg.isAnn[v.key])
            {
                annFields[idx] = Annotations(cast(const(char)[])(v.vptr[0 .. v.vptr_len]));
                continue;
            }
            final switch(hdrInfo.n) {
                case HeaderLengths.OnePerAllele:
                    if(!this.byAllele.length)
                        this.byAllele.length = rec.line.n_allele - 1;

                    for(auto i=0; i < this.byAllele.length; i++) {
                        this.byAllele[i] ~= InfoField(&v, cfg.infos.byAllele.names[idx], i, true);
                    }
                    break;
                case HeaderLengths.OnePerAltAllele:
                    if(!this.byAllele.length)
                        this.byAllele.length = rec.line.n_allele - 1;

                    for(auto i=0; i < this.byAllele.length; i++) {
                        this.byAllele[i] ~= InfoField(&v, cfg.infos.byAllele.names[idx], i);
                    }
                    break;
                case HeaderLengths.OnePerGenotype:
                case HeaderLengths.Fixed:
                case HeaderLengths.None:
                case HeaderLengths.Variable:
                    other ~= InfoField(&v, cfg.infos.other.names[idx]);
                    break;
            }
        }
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

        foreach (ref val; this.other)
        {
            val.serialize(serializer);
        }

        foreach (i, anns; annFields)
        {
            serializer.putKey(cfg.infos.annotations.names[i]);
            auto l = serializer.listBegin;
            foreach (ann; anns)
            {
                ann.serialize(serializer);
            }
            serializer.listEnd(l);
        }
        serializer.structEnd(state);
    }
}

struct InfoSingleAlt
{
    Buffer!InfoField alleleValues;
    Buffer!InfoField other;
    Buffer!Annotations annFields;
    const(HeaderConfig) cfg;
    const(char)[] allele;

    this(Info info, size_t altIdx, const(char)[] allele) @nogc nothrow @safe
    {
        if(info.byAllele.length)
            this.alleleValues = info.byAllele[altIdx];
        this.other = info.other;
        this.annFields = info.annFields;
        this.allele = allele;
        this.cfg = info.cfg;
    }

    void serialize(ref VcfRecordSerializer serializer) @nogc nothrow @safe
    {
        auto state = serializer.structBegin;
        foreach (i, val; this.alleleValues)
        {
            val.serialize(serializer);
        }

        foreach (i, val; this.other)
        {
            val.serialize(serializer);
        }

        foreach (i, anns; annFields)
        {
            serializer.putKey(cfg.infos.annotations.names[i]);
            auto l = serializer.listBegin;
            foreach (ann; anns)
            {
                if (ann.allele != allele){
                    // ann.effect.deallocate;
                    continue;
                }
                ann.serialize(serializer);
            }
            serializer.listEnd(l);
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
    auto res1 = `{byAllele:[{AC:2}],DP4:[1,2,3,4],AN:4,INDEL:true,STR:"test"}`;
    auto res2 = `{byAllele:[{AC:2}],DP4:[1,2,3,4],AN:4,INDEL:true,STR:"test"}`;
    auto res3 = `{byAllele:[{AC:2}],DP4:[1,2,3,4],AN:4,INDEL:true,STR:"test"}`;
    auto res4 = `{AC:2,DP4:[1,2,3,4],AN:4,INDEL:true,STR:"test"}`;
    auto res5 = `{AC:2,DP4:[1,2,3,4],AN:4,INDEL:true,STR:"test"}`;

    auto ionRec = FullVcfRec(vcf.vcfhdr);

    auto rec = vcf.front;
    ionRec.parse(rec);
    assert(serializeVcfToIon(ionRec.info, hdrInfo, false).ion2text == res1);
    {
        auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
        assert(serializeVcfToIon(ionRecSS1.info, hdrInfo, false).ion2text == res2);

        
        auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
        assert(serializeVcfToIon(ionRecSS2.info, hdrInfo, false).ion2text == res3);

        

        auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
        assert(serializeVcfToIon(ionRecSA1.info, hdrInfo, false).ion2text == res4);

        
        auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
        assert(serializeVcfToIon(ionRecSA2.info, hdrInfo, false).ion2text == res5);
    }

    vcf.popFront;

    res1 = `{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3}`;
    res2 = `{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3}`;
    res3 = `{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3}`;
    res4 = `{AC:1,TEST:5,DP4:[1,2,3,4],AN:3}`;
    res5 = `{AC:1,TEST:5,DP4:[1,2,3,4],AN:3}`;
    auto res6 = `{AC:1,TEST:5,DP4:[1,2,3,4],AN:3}`;
    auto res7 = `{AC:1,TEST:5,DP4:[1,2,3,4],AN:3}`;

    rec = vcf.front;
    ionRec.parse(rec);
    assert(serializeVcfToIon(ionRec.info, hdrInfo, false).ion2text == res1);
    {
        auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
        assert(serializeVcfToIon(ionRecSS1.info, hdrInfo, false).ion2text == res2);

        auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
        assert(serializeVcfToIon(ionRecSS2.info, hdrInfo, false).ion2text == res3);

        auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
        assert(serializeVcfToIon(ionRecSA1.info, hdrInfo, false).ion2text == res4);

        auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
        assert(serializeVcfToIon(ionRecSA2.info, hdrInfo, false).ion2text == res5);

        auto ionRecSA3 = VcfRecSingleAlt(ionRecSS1, 1);
        assert(serializeVcfToIon(ionRecSA3.info, hdrInfo, false).ion2text == res6);

        auto ionRecSA4 = VcfRecSingleAlt(ionRecSS2, 1);
        assert(serializeVcfToIon(ionRecSA4.info, hdrInfo, false).ion2text == res7);
    }
    vcf.popFront;

    rec = vcf.front;

    res1 = `{byAllele:[{AC:2}],AN:4}`;
    res2 = `{byAllele:[{AC:2}],AN:4}`;
    res3 = `{byAllele:[{AC:2}],AN:4}`;
    res4 = `{AC:2,AN:4}`;
    res5 = `{AC:2,AN:4}`;

    ionRec.parse(rec);
    assert(serializeVcfToIon(ionRec.info, hdrInfo, false).ion2text);
    assert(serializeVcfToIon(ionRec.info, hdrInfo, false).ion2text == res1);
    {
        auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
        assert(serializeVcfToIon(ionRecSS1.info, hdrInfo, false).ion2text == res2);

        
        auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
        assert(serializeVcfToIon(ionRecSS2.info, hdrInfo, false).ion2text == res3);

        

        auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
        assert(serializeVcfToIon(ionRecSA1.info, hdrInfo, false).ion2text == res4);

        
        auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
        assert(serializeVcfToIon(ionRecSA2.info, hdrInfo, false).ion2text == res5);

        
    }

    // vcf.popFront;
    // vcf.popFront;
    // vcf.popFront;

    // res1 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:["T","C"],QUAL:12.6,FILTER:[test],INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{A:{byAllele:[{TT:0},{TT:1}],GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},B:{byAllele:[{TT:0},{TT:1}],GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}}`;
    // res2 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:["T","C"],QUAL:12.6,FILTER:[test],sample:A,INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{byAllele:[{TT:0},{TT:1}],GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}}`;
    // res3 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:["T","C"],QUAL:12.6,FILTER:[test],sample:B,INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{byAllele:[{TT:0},{TT:1}],GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;
    // res4 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:"T",QUAL:12.6,FILTER:[test],sample:A,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:0,GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}}`;
    // res5 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:"T",QUAL:12.6,FILTER:[test],sample:B,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:0,GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;
    // auto res6 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:"C",QUAL:12.6,FILTER:[test],sample:A,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:1,GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]}}`;
    // auto res7 = `{CHROM:'1',POS:3062915,ID:idSNP,REF:"G",ALT:"C",QUAL:12.6,FILTER:[test],sample:B,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:1,GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}}`;

    // rec = vcf.front;
    // ionRec.parse(rec);
    // assert(serializeVcfToIon(ionRec, hdrInfo, false).ion2text == res1);

    // auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
    // assert(serializeVcfToIon(ionRecSS1, hdrInfo, false).ion2text == res2);

    // auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
    // assert(serializeVcfToIon(ionRecSS2, hdrInfo, false).ion2text == res3);

    // auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
    // assert(serializeVcfToIon(ionRecSA1, hdrInfo, false).ion2text == res4);

    // auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
    // assert(serializeVcfToIon(ionRecSA2, hdrInfo, false).ion2text == res5);

    // auto ionRecSA3 = VcfRecSingleAlt(ionRecSS1, 1);
    // assert(serializeVcfToIon(ionRecSA3, hdrInfo, false).ion2text == res6);

    // auto ionRecSA4 = VcfRecSingleAlt(ionRecSS2, 1);
    // assert(serializeVcfToIon(ionRecSA4, hdrInfo, false).ion2text == res7);
}
unittest
{
    import std.stdio;
    import libmucor.serde;
    import libmucor.atomize.record;
    import mir.ion.conv;
    import dhtslib.vcf : VCFReader;
    

    auto vcf = VCFReader("test/data/vcf_file2.vcf", -1, UnpackLevel.All);
    
    auto hdrInfo = HeaderConfig(vcf.vcfhdr);
    auto res1 = `{byAllele:[{AC:2}],DP4:[1,2,3,4],AN:4,INDEL:true,STR:"test"}`;
    auto res2 = `{byAllele:[{AC:2}],DP4:[1,2,3,4],AN:4,INDEL:true,STR:"test"}`;
    auto res3 = `{byAllele:[{AC:2}],DP4:[1,2,3,4],AN:4,INDEL:true,STR:"test"}`;
    auto res4 = `{AC:2,DP4:[1,2,3,4],AN:4,INDEL:true,STR:"test"}`;
    auto res5 = `{AC:2,DP4:[1,2,3,4],AN:4,INDEL:true,STR:"test"}`;

    auto ionRec = FullVcfRec(vcf.vcfhdr);

    auto rec = vcf.front;
    ionRec.parse(rec);
    writeln(serializeVcfToIon(ionRec.info, hdrInfo, false).ion2text);
}
