module libmucor.atomize.header;

import dhtslib.vcf;

struct HdrFieldInfo
{
    HeaderTypes t;
    HeaderLengths n;
}

struct HdrFields
{
    string[] names;
    HdrFieldInfo[] info;
}

struct FormatHdrFields
{
    HdrFields byAllele;
    HdrFields other;

    size_t add(HeaderRecord hrec)
    {
        this.other.names ~= hrec.getID().idup;
        this.other.info ~= HdrFieldInfo(hrec.valueType, hrec.lenthType);
        return this.other.names.length - 1;
    }

    size_t addByAllele(HeaderRecord hrec)
    {
        this.byAllele.names ~= hrec.getID().idup;
        this.byAllele.info ~= HdrFieldInfo(hrec.valueType, hrec.lenthType);
        return this.byAllele.names.length - 1;
    }

}

struct InfoHdrFields
{
    HdrFields byAllele;
    HdrFields other;
    HdrFields annotations;

    size_t add(HeaderRecord hrec)
    {
        this.other.names ~= hrec.getID().idup;
        this.other.info ~= HdrFieldInfo(hrec.valueType, hrec.lenthType);
        return this.other.names.length - 1;
    }

    size_t addByAllele(HeaderRecord hrec)
    {
        this.byAllele.names ~= hrec.getID().idup;
        this.byAllele.info ~= HdrFieldInfo(hrec.valueType, hrec.lenthType);
        return this.byAllele.names.length - 1;
    }

    size_t addAnnotation(HeaderRecord hrec)
    {
        this.annotations.names ~= hrec.getID().idup;
        this.annotations.info ~= HdrFieldInfo(hrec.valueType, hrec.lenthType);
        return this.annotations.names.length - 1;
    }

}

struct HeaderConfig
{
    FormatHdrFields fmts;
    InfoHdrFields infos;
    long[] remapIdxs;
    bool[] isByAllele;
    bool[] isAnn;
    bool[] isInfo;

    string[] samples;
    string[] filters;

    this(VCFHeader header)
    {
        this.isAnn.length = header.hdr.n[HeaderDictTypes.Id];
        this.isByAllele.length = header.hdr.n[HeaderDictTypes.Id];
        this.isInfo.length = header.hdr.n[HeaderDictTypes.Id];
        this.remapIdxs.length = header.hdr.n[HeaderDictTypes.Id];
        this.remapIdxs[] = -1;

        long[string] hm;
        for (auto i = 0; i < header.hdr.n[HeaderDictTypes.Id]; i++)
        {
            import std.string;

            hm[fromStringz(header.hdr.id[HeaderDictTypes.Id][i].key)] = i;
        }
        for (auto i = 0; i < header.hdr.nhrec; i++)
        {
            auto hrec = HeaderRecord(header.hdr.hrec[i]);
            if (hrec.recType == HeaderRecordType.Filter)
            {
                this.filters ~= hrec.getID().idup;
            }
            if (!(hrec.recType == HeaderRecordType.Info || hrec.recType == HeaderRecordType.Format))
                continue;
            auto idx = hm[hrec.getID];
            switch (hrec.lenthType)
            {
            case HeaderLengths.OnePerAllele:
            case HeaderLengths.OnePerAltAllele:
                this.isByAllele[idx] = true;
                break;
            default:
                break;
            }
            this.isAnn[idx] = false;
            switch (hrec.recType)
            {
            case HeaderRecordType.Info:
                this.isInfo[idx] = true;
                if (hrec.getID() == "ANN")
                    this.isAnn[idx] = true;

                if (this.isByAllele[idx])
                    this.remapIdxs[idx] = this.infos.addByAllele(hrec);

                else if (this.isAnn[idx])
                    this.remapIdxs[idx] = this.infos.addAnnotation(hrec);

                else
                    this.remapIdxs[idx] = this.infos.add(hrec);
                break;

            case HeaderRecordType.Format:
                if (this.isByAllele[idx])
                    this.remapIdxs[idx] = this.fmts.addByAllele(hrec);

                else
                    this.remapIdxs[idx] = this.fmts.add(hrec);
                break;
            default:
                break;
            }
        }
        this.samples = header.getSamples;
    }

    auto getInfo(size_t idx) const
    {
        auto byAllele = this.isByAllele[idx];
        auto isAnn = this.isAnn[idx];
        auto i = this.getIdx(idx);
        assert(i >= 0);
        if (byAllele)
            return this.infos.byAllele.info[i];
        else if (isAnn)
            return this.infos.annotations.info[i];
        else
            return this.infos.other.info[i];
    }

    auto getFmt(size_t idx) const
    {
        auto byAllele = this.isByAllele[idx];
        auto i = this.getIdx(idx);
        assert(i >= 0);
        if (byAllele)
            return this.fmts.byAllele.info[i];
        else
            return this.fmts.other.info[i];
    }

    auto getIdx(size_t idx) const
    {
        return this.remapIdxs[idx];
    }


    auto toString() const {

        import std.format;

        string ret = "[\n";
        for (auto i = 0; i < this.remapIdxs.length; i++)
        {
            if (this.remapIdxs[i] != -1)
            {
                auto newIdx = this.remapIdxs[i];
                if (this.isInfo[i])
                {
                    HdrFieldInfo finfo;
                    string name;
                    if (this.isByAllele[i])
                    {
                        finfo = this.infos.byAllele.info[newIdx];
                        name = this.infos.byAllele.names[newIdx];
                    }
                    else if (this.isAnn[i])
                    {
                        finfo = this.infos.annotations.info[newIdx];
                        name = this.infos.annotations.names[newIdx];
                    }
                    else
                    {
                        finfo = this.infos.other.info[newIdx];
                        name = this.infos.other.names[newIdx];
                    }

                    ret ~= "\tINFO/%s:(id: %d, newId: %d, type: %s, num: %s, byAllele: %s, isAnn: %s),\n".format(name,
                            i, newIdx, finfo.t, finfo.n, this.isByAllele[i], this.isAnn[i]);
                }
                else
                {
                    HdrFieldInfo finfo;
                    string name;
                    if (this.isByAllele[i])
                    {
                        finfo = this.fmts.byAllele.info[newIdx];
                        name = this.fmts.byAllele.names[newIdx];
                    }
                    else
                    {
                        finfo = this.fmts.other.info[newIdx];
                        name = this.fmts.other.names[newIdx];
                    }

                    ret ~= "\tFMT/%s:(id: %d, newId: %d, type: %s, num: %s, byAllele: %s),\n".format(name,
                            i, newIdx, finfo.t, finfo.n, this.isByAllele[i]);
                }

            }
        }
        return ret ~ "]";
    }
}
