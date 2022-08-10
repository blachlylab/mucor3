module libmucor.atomize.fmt;

import std.algorithm : map, any;

import mir.ser.interfaces;
import mir.ser;
import mir.serde;

import dhtslib.vcf;
import libmucor.khashl;
import libmucor.atomize.field;
import libmucor.atomize.header;
import libmucor.serde.ser;

struct FmtValues
{
    @serdeIgnore FieldValue[] fields;
    @serdeIgnore bool isNull = true;

    void reset()
    {
        this.isNull = true;
        foreach (f; fields)
        {
            f.reset;
        }
    }

    ref auto opIndex(size_t index)
    {
        this.isNull = false;
        return fields[index];
    }

    void serialize(ref VcfRecordSerializer serializer, const(HeaderConfig)* cfg, bool byAllele)
    {
        if (this.isNull)
            return;
        auto s = serializer.structBegin;
        foreach (i, val; this.fields)
        {
            if (val.isNull)
                continue;
            if (byAllele)
                serializer.putKey(cfg.fmts.byAllele.names[i]);
            else
                serializer.putKey(cfg.fmts.other.names[i]);
            val.serialize(serializer);
        }
        serializer.structEnd(s);
    }
}

/** 
 * sam1: {
 *      byAllele: {...}
 *      DP: ...,
 * }
 */
struct FmtSampleValues
{
    @serdeIgnore FmtValues[] byAllele;
    @serdeIgnore FmtValues other;
    @serdeIgnore bool isNull = true;

    void reset()
    {
        this.isNull = true;
        foreach (ref v; byAllele)
        {
            v.reset;
        }
        this.other.reset;
    }

    void serialize(ref VcfRecordSerializer serializer, const(HeaderConfig)* cfg)
    {
        auto state = serializer.structBegin;
        if (this.byAllele.map!(x => !x.isNull).any)
        {
            serializer.putKey("byAllele");
            auto state2 = serializer.listBegin;
            foreach (i, v; this.byAllele)
            {
                v.serialize(serializer, cfg, true);
            }
            serializer.listEnd(state2);
        }

        foreach (i, val; this.other.fields)
        {
            if (val.isNull)
                continue;
            serializer.putKey(cfg.fmts.other.names[i]);
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
    @serdeIgnore FmtSampleValues[] bySample;
    @serdeIgnoreOut Genotype[] genotypes;
    @serdeIgnore const(HeaderConfig) cfg;
    @serdeIgnore size_t numByAlleleFields;

    this(HeaderConfig cfg)
    {
        this.numByAlleleFields = cfg.fmts.byAllele.names.length;
        this.bySample.length = cfg.samples.length;
        foreach (ref s; this.bySample)
        {
            s.other.fields.length = cfg.fmts.other.names.length;
        }
        this.genotypes.length = cfg.samples.length;
        this.cfg = cfg;
    }

    void reset()
    {
        foreach (ref sam; bySample)
        {
            sam.reset;
        }
    }

    void parse(VCFRecord rec)
    {
        import htslib.vcf;

        this.reset;
        /// make sure all arrays are initialized
        foreach (ref s; this.bySample)
        {
            if (s.byAllele.length < rec.line.n_allele - 1)
            {
                s.byAllele.length = rec.line.n_allele - 1;
                foreach (ref v; s.byAllele)
                {
                    v.fields.length = this.numByAlleleFields;
                }
            }
            else
            {
                s.byAllele.length = rec.line.n_allele - 1;
            }
        }

        assert(this.bySample.length == rec.line.n_sample);
        if (rec.line.n_fmt == 0)
            return;
        /// get genotype first
        auto gtFMT = FormatField("GT", &rec.line.d.fmt[0], rec.line);
        /// loop over samples and get genotypes
        for (auto i = 0; i < rec.line.n_sample; i++)
        {
            auto gt = Genotype(gtFMT, i);
            genotypes[i] = gt;
        }

        /// loop over fmts
        foreach (bcf_fmt_t v; rec.line.d.fmt[1 .. rec.line.n_fmt])
        {
            auto fmt = FormatField("", &v, rec.line);

            auto idx = cfg.getIdx(v.id);
            auto hdrInfo = cfg.getFmt(v.id);
            final switch (fmt.type)
            {
            case BcfRecordType.Char:
                auto vals = fmt.to!string;
                for (auto i = 0; i < genotypes.length; i++)
                {
                    if(genotypes[i].isNull) continue;
                    final switch(hdrInfo.n) {
                        case HeaderLengths.OnePerAllele:
                        case HeaderLengths.OnePerAltAllele:
                            this.bySample[i].byAllele[0][idx] = vals[i][0];
                            break;
                        case HeaderLengths.OnePerGenotype:
                        case HeaderLengths.Fixed:
                        case HeaderLengths.None:
                        case HeaderLengths.Variable:
                            this.bySample[i].other[idx] = vals[i][0];
                            break;
                    }
                }
                continue;
                // float or float array
            case BcfRecordType.Float:
                this.parseFmt!float(hdrInfo, fmt, idx);
                break;
                // int type or array
            case BcfRecordType.Int8:
            case BcfRecordType.Int16:
            case BcfRecordType.Int32:
            case BcfRecordType.Int64:
                this.parseFmt!long(hdrInfo, fmt, idx);
                break;
            case BcfRecordType.Null:
                continue;
            }
        }
        for (auto i = 0; i < genotypes.length; i++)
        {
            auto gt = genotypes[i];
            if (gt.isNull)
                continue;
            this.bySample[i].isNull = false;
            this.bySample[i].other[cfg.getIdx(rec.line.d.fmt[0].id)] = gt.toString;
        }
    }

    void parseFmt(T)(HdrFieldInfo hdrInfo, FormatField fmt, size_t idx)
    {
        assert(genotypes.length == this.bySample.length);
        final switch (hdrInfo.n)
        {
        case HeaderLengths.OnePerAllele:
            auto vals = fmt.to!T;
            for (auto i = 0; i < genotypes.length; i++)
            {
                auto gt = genotypes[i];

                if (gt.isNull)
                    continue;
                auto val = vals[i];
                auto byAllele = &this.bySample[i].byAllele;
                if (byAllele.length == 0)
                    return;
                assert(fmt.n == val.length);

                auto first = val[0];
                foreach (gi; gt.alleles)
                {
                    if (gi == 0)
                    {
                        first = val[gi];
                        break;
                    }
                }
                auto j = 0;
                foreach (gi; gt.alleles)
                {
                    if (gi != 0 && j < byAllele.length)
                    {
                        (*byAllele)[j][idx] = [first, val[gi]];
                        j++;
                    }
                }
            }
            return;
        case HeaderLengths.OnePerAltAllele:
            auto vals = fmt.to!T;
            for (auto i = 0; i < genotypes.length; i++)
            {
                auto gt = genotypes[i];
                if (gt.isNull)
                    continue;

                auto val = vals[i];
                if (this.bySample[i].byAllele.length == 0)
                    return;
                assert(this.bySample[i].byAllele.length == val.length);

                foreach (j, ref allele; this.bySample[i].byAllele)
                {
                    allele[idx] = val[j];
                }
            }
            return;
        case HeaderLengths.OnePerGenotype:
        case HeaderLengths.Fixed:
        case HeaderLengths.None:
        case HeaderLengths.Variable:
            auto vals = fmt.to!T;

            for (auto i = 0; i < genotypes.length; i++)
            {
                auto val = vals[i];

                auto gt = genotypes[i];
                if (gt.isNull)
                    continue;

                if (fmt.n == 1)
                    this.bySample[i].other[idx] = val[0];
                else
                    this.bySample[i].other[idx] = val;
            }
            return;
        }
    }

    void serialize(ref VcfRecordSerializer serializer)
    {
        auto state = serializer.structBegin;

        foreach (i, string key; cfg.samples)
        {
            if (this.bySample[i].isNull)
                continue;
            serializer.putKey(key);
            this.bySample[i].serialize(serializer, &cfg);
        }
        serializer.structEnd(state);
    }
}

struct FmtSingleSample
{
    @serdeIgnore FmtSampleValues sampleValues;
    @serdeIgnore const(HeaderConfig) cfg;

    this(Fmt fmt, size_t samIdx)
    {
        this.sampleValues = fmt.bySample[samIdx];
        this.cfg = fmt.cfg;
    }

    void serialize(S)(ref S serializer)
    {
        this.sampleValues.serialize(serializer, &cfg);
    }
}

struct FmtSingleAlt
{
    @serdeIgnore FmtValues alleleValues;
    @serdeIgnore FmtValues other;
    @serdeIgnore const(HeaderConfig) cfg;

    this(FmtSingleSample fmt, size_t altIdx)
    {
        this.alleleValues = fmt.sampleValues.byAllele[altIdx];
        this.other = fmt.sampleValues.other;
        this.cfg = fmt.cfg;
    }

    void serialize(ref VcfRecordSerializer serializer)
    {
        auto state = serializer.structBegin;
        foreach (i, val; this.alleleValues.fields)
        {
            if (val.isNull)
                continue;
            serializer.putKey(cfg.fmts.byAllele.names[i]);
            val.serialize(serializer);
        }
        foreach (i, val; this.other.fields)
        {
            if (val.isNull)
                continue;
            serializer.putKey(cfg.fmts.other.names[i]);
            val.serialize(serializer);
        }

        serializer.structEnd(state);
    }
}
