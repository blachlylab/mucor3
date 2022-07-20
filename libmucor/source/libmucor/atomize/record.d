module libmucor.atomize.record;
import mir.ser;
import mir.ser.interfaces;
import mir.bignum.integer;

import std.math : isNaN;

import dhtslib.vcf;
import libmucor.atomize.fmt;
import libmucor.atomize.info;
import libmucor.atomize.ann;
import libmucor.atomize.header;
import libmucor.atomize.serde.ser;
import libmucor.atomize.serde;

struct VcfRequiredFields(bool singleSample, bool singleAlt) {

    @serdeKeys("CHROM")
    string chrom;

    @serdeKeys("POS")
    long pos;

    @serdeKeys("ID")
    @serdeIgnoreOutIf!`a == "."`
    string id;

    @serdeKeys("REF")
    string ref_;

    @serdeKeys("ALT")
    static if(singleAlt)
        string alt;
    else
        string[] alt;

    @serdeIgnoreOutIf!isNaN
    @serdeKeys("QUAL")
    float qual;

    @serdeKeys("FILTER")
    string[] filter;

    static if(singleSample) {
        string sample;
    }

    BigInt!2 checksum;
    
    void serialize(ref VcfRecordSerializer serializer) {
        serializer.putSharedKey("CHROM");
        serializer.putValue(this.chrom);

        serializer.putSharedKey("POS");
        serializer.putValue(pos);

        if(this.id != ".") {
            serializer.putSharedKey("ID");
            serializer.putValue(id);
        }

        serializer.putSharedKey("REF");
        serializer.putSymbol(this.ref_);

        serializer.putSharedKey("ALT");
        static if(singleAlt){
            serializer.putSymbol(this.alt);
        } else {
            auto l = serializer.listBegin;
            foreach (ref string key; this.alt)
            {
                serializer.putSymbol(key);
            }
            serializer.listEnd(l);
        }
        
        if(!isNaN(this.qual)) {
            serializer.putSharedKey("QUAL");
            serializer.putValue(qual);
        }

        serializer.putSharedKey("FILTER");
        auto l2 = serializer.listBegin;
        foreach (ref string key; filter)
        {
            serializer.putSharedSymbol(key);    
        }
        serializer.listEnd(l2);

        static if(singleSample){
            serializer.putSharedKey("sample");
            serializer.putSharedSymbol(this.sample);
        }
    }
}

struct VcfRec {
    alias ReqFields = VcfRequiredFields!(false, false);
    ReqFields required;
    alias required this;

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

    void serialize(ref VcfRecordSerializer serializer) {
        auto s = serializer.structBegin;
        this.required.serialize(serializer);
        
        serializer.putSharedKey("INFO");
        info.serialize(serializer);

        serializer.putSharedKey("FORMAT");
        fmt.serialize(serializer);
    
        auto last = serializer.serializer.data[s .. $];
        serializer.putSharedKey("checksum");

        serializeValue(serializer.serializer, hashIon(last));
        serializer.structEnd(s);
    }
}

struct VcfRecSingleSample {
    alias ReqFields = VcfRequiredFields!(true, false);
    @serdeIgnoreOut
    ReqFields required;
    alias required this;

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

    void serialize(ref VcfRecordSerializer serializer) {
        auto s = serializer.structBegin;
        this.required.serialize(serializer);
        
        serializer.putSharedKey("INFO");
        info.serialize(serializer);

        serializer.putSharedKey("FORMAT");
        fmt.serialize(serializer);
        
        auto last = serializer.serializer.data[s .. $];
        serializer.putSharedKey("checksum");

        serializeValue(serializer.serializer, hashIon(last));
        serializer.structEnd(s);
    }
}

struct VcfRecSingleAlt {
    alias ReqFields = VcfRequiredFields!(true, true);
    @serdeIgnoreOut
    ReqFields required;
    alias required this;

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

    void serialize(ref VcfRecordSerializer serializer) {
        auto s = serializer.structBegin;
        this.required.serialize(serializer);
        
        serializer.putSharedKey("INFO");
        info.serialize(serializer);

        serializer.putSharedKey("FORMAT");
        fmt.serialize(serializer);

        auto last = serializer.serializer.data[s .. $];
        serializer.putSharedKey("checksum");

        serializeValue(serializer.serializer, hashIon(last));
        serializer.structEnd(s);
    }
}

unittest {
    import std.stdio;
    import libmucor.atomize.serde;

    auto vcf = VCFReader("../test/data/vcf_file.vcf",-1, UnpackLevel.All);

    auto hdrInfo = HeaderConfig(vcf.vcfhdr);
    auto rec = vcf.front;

    auto res1 = `{CHROM:"1",POS:3000149,REF:C,ALT:[T],QUAL:59.2,FILTER:[PASS],INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{A:{GT:"0/1",GQ:245},B:{GT:"0/1",GQ:245}},checksum:117558158828293615626413830071040573524}`;
    auto res2 = `{CHROM:"1",POS:3000149,REF:C,ALT:[T],QUAL:59.2,FILTER:[PASS],sample:A,INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{GT:"0/1",GQ:245},checksum:312819208676450488940002048919564391637}`;
    auto res3 = `{CHROM:"1",POS:3000149,REF:C,ALT:[T],QUAL:59.2,FILTER:[PASS],sample:B,INFO:{byAllele:[{AC:2}],AN:4},FORMAT:{GT:"0/1",GQ:245},checksum:183101013209721148783211394903802951045}`;
    auto res4 = `{CHROM:"1",POS:3000149,REF:C,ALT:T,QUAL:59.2,FILTER:[PASS],sample:A,INFO:{AC:2,AN:4},FORMAT:{GT:"0/1",GQ:245},checksum:10062642182546142337762130151449115834}`;
    auto res5 = `{CHROM:"1",POS:3000149,REF:C,ALT:T,QUAL:59.2,FILTER:[PASS],sample:B,INFO:{AC:2,AN:4},FORMAT:{GT:"0/1",GQ:245},checksum:264289506251969372394017004258015941852}`;
    auto ionRec = VcfRec(vcf.vcfhdr);

    ionRec.parse(rec);

    assert(serializeVcfToIon(ionRec, hdrInfo).vcfIonToText == res1);

    {
        auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
        assert(serializeVcfToIon(ionRecSS1, hdrInfo).vcfIonToText == res2);
        auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
        assert(serializeVcfToIon(ionRecSS2, hdrInfo).vcfIonToText == res3);

        auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
        assert(serializeVcfToIon(ionRecSA1, hdrInfo).vcfIonToText == res4);
        auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
        assert(serializeVcfToIon(ionRecSA2, hdrInfo).vcfIonToText == res5);
    }

    vcf.popFront;
    vcf.popFront;
    vcf.popFront;

    res1 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:G,ALT:[T,C],QUAL:12.6,FILTER:[test],INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{A:{byAllele:[{TT:0},{TT:1}],GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},B:{byAllele:[{TT:0},{TT:1}],GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]}},checksum:219116576482101330421115625536470289143}`;
    res2 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:G,ALT:[T,C],QUAL:12.6,FILTER:[test],sample:A,INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{byAllele:[{TT:0},{TT:1}],GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},checksum:185658878538043403797160906909977743490}`;
    res3 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:G,ALT:[T,C],QUAL:12.6,FILTER:[test],sample:B,INFO:{byAllele:[{AC:1},{AC:1}],TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{byAllele:[{TT:0},{TT:1}],GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]},checksum:92660607317732590518334398831914442500}`;
    res4 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:G,ALT:T,QUAL:12.6,FILTER:[test],sample:A,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:0,GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},checksum:332798400777634619017407027204223542362}`;
    res5 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:G,ALT:T,QUAL:12.6,FILTER:[test],sample:B,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:0,GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]},checksum:242569322480355185711890828813215226132}`;
    auto res6 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:G,ALT:C,QUAL:12.6,FILTER:[test],sample:A,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:1,GT:"0/1",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,-20.0,-5.0,-20.0]},checksum:176520503121926150263921337119712364072}`;
    auto res7 = `{CHROM:"1",POS:3062914,ID:"idSNP",REF:G,ALT:C,QUAL:12.6,FILTER:[test],sample:B,INFO:{AC:1,TEST:5,DP4:[1,2,3,4],AN:3},FORMAT:{TT:1,GT:"2",GQ:409,DP:35,GL:[-20.0,-5.0,-20.0,nan,nan,nan]},checksum:154878335168869126708367871750094980419}`;
    
    rec = vcf.front;
    ionRec.parse(rec);
    assert(serializeVcfToIon(ionRec, hdrInfo).vcfIonToText == res1);

    auto ionRecSS1 = VcfRecSingleSample(ionRec, 0);
    assert(serializeVcfToIon(ionRecSS1, hdrInfo).vcfIonToText == res2);

    auto ionRecSS2 = VcfRecSingleSample(ionRec, 1);
    assert(serializeVcfToIon(ionRecSS2, hdrInfo).vcfIonToText == res3);

    auto ionRecSA1 = VcfRecSingleAlt(ionRecSS1, 0);
    assert(serializeVcfToIon(ionRecSA1, hdrInfo).vcfIonToText == res4);

    auto ionRecSA2 = VcfRecSingleAlt(ionRecSS2, 0);
    assert(serializeVcfToIon(ionRecSA2, hdrInfo).vcfIonToText == res5);

    auto ionRecSA3 = VcfRecSingleAlt(ionRecSS1, 1);
    assert(serializeVcfToIon(ionRecSA3, hdrInfo).vcfIonToText == res6);

    auto ionRecSA4 = VcfRecSingleAlt(ionRecSS2, 1);
    assert(serializeVcfToIon(ionRecSA4, hdrInfo).vcfIonToText == res7);
}