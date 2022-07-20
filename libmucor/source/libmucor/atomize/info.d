module libmucor.atomize.info;

import mir.ser.interfaces;
import mir.ser;
import mir.serde;

import dhtslib.vcf;
import std.string : fromStringz;
import std.algorithm : map, any;

import libmucor.khashl;
import libmucor.atomize.header;
import libmucor.atomize.field;
import libmucor.atomize.ann;
import libmucor.atomize.serde.ser;
import libmucor.error;

struct InfoAlleleValues {
    @serdeIgnore
    FieldValue[] fields;
    @serdeIgnore
    bool isNull = true;

    void reset() {
        this.isNull = true;
        foreach (ref f; fields)
        {
            f.reset;
        }
    }

    ref auto opIndex(size_t index)
    {
        this.isNull = false;
        return fields[index];
    }

    void serialize(ref VcfRecordSerializer serializer, const(HeaderConfig) * cfg, bool byAllele) {
        if(this.isNull) return;
        auto s = serializer.structBegin;
        foreach (i,val; this.fields)
        {
            if(val.isNull) continue;
            if(byAllele)
                serializer.putSharedKey(cfg.infos.byAllele.names[i]);
            else
                serializer.putSharedKey(cfg.infos.other.names[i]);
            val.serialize(serializer);
        }
        serializer.structEnd(s);
    }
}

struct Info {
    Annotation _dummy;
    @serdeIgnore
    InfoAlleleValues[] byAllele;
    @serdeIgnore
    FieldValue[] other;
    @serdeIgnore
    Annotations[] annFields;
    @serdeIgnore
    const(HeaderConfig) cfg;
    @serdeIgnore
    size_t numByAlleleFields;

    this(HeaderConfig cfg) {
        this.numByAlleleFields = cfg.infos.byAllele.names.length;
        this.other.length = cfg.infos.other.names.length;
        this.annFields.length = cfg.infos.annotations.names.length;
        this.cfg = cfg;
    }

    void reset() {
        foreach (ref key; byAllele){
            key.reset;
        }
        foreach (ref key; other)
        {
            key.reset;
        }
    }

    void parse(VCFRecord rec)
    {
        import htslib.vcf;
        this.reset;
        /// make sure all arrays are initialized
        if(this.byAllele.length < rec.line.n_allele - 1){
            this.byAllele.length = rec.line.n_allele - 1;
            foreach (ref v; this.byAllele)
            {
                v.fields.length = this.numByAlleleFields;
            }
        } else {
            this.byAllele.length = rec.line.n_allele - 1;
        }
        if(rec.line.n_info == 0) return;
        /// loop over infos
        foreach (bcf_info_t v; rec.line.d.info[0 .. rec.line.n_info])
        {
            if (!v.vptr)
                continue;

            auto info = InfoField("", &v, rec.line);
            auto idx = cfg.getIdx(v.key);
            auto hdrInfo = cfg.getInfo(v.key);
            if(cfg.isAnn[v.key]) {
                annFields[idx] = Annotations(info.to!string);
                continue;
            }
            final switch (info.type)
            {
            case BcfRecordType.Char:
                other[idx] = info.to!string;
                continue;
                // float or float array
            case BcfRecordType.Float:
                this.parseInfo!float(hdrInfo, info, idx);
                break;
                // int type or array
            case BcfRecordType.Int8:
            case BcfRecordType.Int16:
            case BcfRecordType.Int32:
            case BcfRecordType.Int64:
                this.parseInfo!long(hdrInfo, info, idx);
                break;
            case BcfRecordType.Null:
                continue;
            }
        }
    }

    void parseInfo(T)(HdrFieldInfo hdrInfo, InfoField info, size_t idx) {
        final switch (hdrInfo.n)
        {
        case HeaderLengths.OnePerAllele:
            if(this.byAllele.length == 0) return;
            auto vals = info.to!(T[]);

            foreach (i, val; vals)
            {
                if (i == 0)
                    continue;
                this.byAllele[i - 1][idx] = [vals[0], val];
            }
            return;
        case HeaderLengths.OnePerAltAllele:
            if(this.byAllele.length == 0) return;
            auto vals = info.to!(T[]);

            foreach (i, val; vals)
            {
                this.byAllele[i][idx] = val;
            }
            return;
        case HeaderLengths.OnePerGenotype:
        case HeaderLengths.Fixed:
        case HeaderLengths.None:
        case HeaderLengths.Variable:
            if (info.len == 1)
                this.other[idx] = info.to!(T[])[0];
            else
                this.other[idx] = info.to!(T[]);
            return;
        }
    }

    void serialize(ref VcfRecordSerializer serializer) {
        auto state = serializer.structBegin;
        if(this.byAllele.map!(x => !x.isNull).any){
            serializer.putSharedKey("byAllele");
            auto state2 = serializer.listBegin;
            foreach (i,v; this.byAllele)
            {
                v.serialize(serializer, &cfg, true);
            }
            serializer.listEnd(state2);
        }

        foreach (i,val; this.other)
        {
            if(val.isNull) continue;
            serializer.putSharedKey(cfg.infos.other.names[i]);
            val.serialize(serializer);
        }

        foreach (i, anns; annFields)
        {
            serializer.putSharedKey(cfg.infos.annotations.names[i]);
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

struct InfoSingleAlt {
    Annotation _dummy;
    @serdeIgnore
    InfoAlleleValues alleleValues;
    @serdeIgnore
    FieldValue[] other;
    @serdeIgnore
    Annotations[] annFields;
    @serdeIgnore
    const(HeaderConfig) cfg;
    @serdeIgnore
    string allele;

    this(Info info, size_t altIdx, string allele) {
        this.alleleValues = info.byAllele[altIdx];
        this.other = info.other;
        this.annFields = info.annFields;
        this.allele = allele;
        this.cfg = info.cfg;
    }

    void serialize(ref VcfRecordSerializer serializer) {
        auto state = serializer.structBegin;
        foreach (i,val; this.alleleValues.fields)
        {
            if(val.isNull) continue;
            serializer.putSharedKey(cfg.infos.byAllele.names[i]);
            val.serialize(serializer);
        }

        foreach (i,val; this.other)
        {
            if(val.isNull) continue;
            serializer.putSharedKey(cfg.infos.other.names[i]);
            val.serialize(serializer);
        }

        foreach (i, anns; annFields)
        {
            serializer.putSharedKey(cfg.infos.annotations.names[i]);
            auto l = serializer.listBegin;
            foreach (ann; anns)
            {
                if(ann.allele != allele) continue;
                ann.serialize(serializer);
            }
            serializer.listEnd(l);
        }
        serializer.structEnd(state);
    }

}