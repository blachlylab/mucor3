module vcfvalue;
import std.algorithm: map, each, joiner, filter;
import std.sumtype: SumType, This, match, tryMatch;
import std.range: enumerate, iota, inputRangeObject;
import std.array: array;
import asdf;
import asdf.serialization: serializeToAsdf;
import jsonlops.basic;
import std.traits: isIntegral, isSomeString, isArray;
import std.digest.md : MD5Digest, toHexString;
import std.string: fromStringz;

import dhtslib.vcf;
import htslib.hts_log;
import vcf;
import jsonvalue;

JsonValue * parseInfos(VCFRecord * rec, HeaderConfig cfg, ulong numAlts)
{
    auto infoObj = makeJsonObject;
    auto byAllele = makeJsonArray(numAlts);
    for(auto i = 0; i < numAlts; i++){
        (*byAllele)[i] = *makeJsonObject;
    }

    foreach(v; rec.line.d.info[0..rec.line.n_info]){
        if(!v.vptr) continue;
        auto key = cast(string)fromStringz(rec.vcfheader.hdr.id[HeaderDictTypes.Id][v.key].key);
        auto info = InfoField(key, &v, rec.line);
        if(!(key in cfg.infos)){
            hts_log_warning(__FUNCTION__, "Info Key "~key~" not found in header! Ignoring...");
            continue;
        }
        auto hdrInfo = cfg.infos[key];
        JsonValue * data;
        final switch(info.type){
            // char/string
            case BcfRecordType.Char:
                (*infoObj)[key] = JsonValue(info.to!string);
                continue;
            // float or float array
            case BcfRecordType.Float:
                data = parseInfo!float(info, hdrInfo);
                break;
            // int type or array
            case BcfRecordType.Int8:
            case BcfRecordType.Int16:
            case BcfRecordType.Int32:
            case BcfRecordType.Int64:
                data = parseInfo!long(info, hdrInfo);
                break;
            case BcfRecordType.Null:
                continue;
        }

        assert(data);
        assert(data.value);
        if(hdrInfo.n == HeaderLengths.OnePerAllele){
            for(auto i=0; i < data.length; i++)
            {
                (*byAllele)[i][key] = (*data)[i];
            }
        } else {
            if(data.length == 1)
                (*infoObj)[key] = (*data)[0];
            else 
                (*infoObj)[key] = *data;
        }
    }
    (*infoObj)["byAllele"] = *byAllele;
    return infoObj;
}

JsonValue * parseInfo(T)(ref InfoField item, FieldInfo hdrInfo) {
    switch(hdrInfo.n){
        case HeaderLengths.OnePerAllele:
            return parseOnePerAllele!T(item);
        case HeaderLengths.OnePerAltAllele:
            return parseOnePerAltAllele!T(item);
        default:
            return parseOnePerAltAllele!T(item);
    }
}

JsonValue * parseOnePerAllele(T, V)(ref V item) {
    
    static if (is(V == InfoField))
    {
        auto byAllele = makeJsonArray(item.len - 1);
        static if (is(T == string))
            auto vals = item.to!(string);
        else
            auto vals = item.to!(T[]);

        foreach (i,val; vals)
        {
            if (i==0) continue;
            (*byAllele)[i-1] = [vals[0], val];
        }
        assert(byAllele[0].value);
        return byAllele;
    }else{
        auto bySample = makeJsonArray(item.nSamples);
        auto vals = item.to!T;

        foreach (i,val; vals.enumerate)
        {
            auto byAllele = makeJsonArray(item.n - 1);
            assert(item.n == val.length);

            for(auto j=0; j < item.n - 1; j++){
                (*byAllele)[j] =  [val[0], val[j + 1]];
            }
            (*bySample)[i] = *byAllele;
        }

        return bySample;
    }
    
}

JsonValue * parseOnePerAltAllele(T, V)(ref V item) {
    static if (is(V == InfoField))
    {
        auto byAllele = makeJsonArray(item.len);
        static if (is(T == string))
            auto vals = item.to!(string);
        else
            auto vals = item.to!(T[]);

        foreach (i,val; vals)
        {
            (*byAllele)[i] = val;
        }
        assert(byAllele[0].value);
        return byAllele;
    } else {
        auto bySample = makeJsonArray(item.nSamples);
        auto vals = item.to!T;

        foreach (i,val; vals.enumerate)
        {
            auto byAllele = makeJsonArray(item.n);
            
            for(auto j=0; j < item.n; j++){
                (*byAllele)[j] =  val[j];
            }
            (*bySample)[i] = *byAllele;
        }
        return bySample;
    }
}

JsonValue * parseFormats(VCFRecord * rec, HeaderConfig cfg, ulong numAlts, string[] samples)
{
    auto bySample = makeJsonObject;
    for(auto i = 0; i < samples.length; i++){
        auto byAllele = makeJsonArray(numAlts);

        (*bySample)[samples[i]] = *makeJsonObject;

        for(auto j = 0; j < numAlts; j++){
            (*byAllele)[j] = *makeJsonObject;
        }
        
        (*bySample)[samples[i]]["byAllele"] = *byAllele;
        (*bySample)[samples[i]]["GT"] = rec.getGenotype(i).toString;
    }

    foreach(v; rec.line.d.fmt[0..rec.line.n_fmt]){
        if(!v.p) continue;
        auto key = cast(string)fromStringz(rec.vcfheader.hdr.id[HeaderDictTypes.Id][v.id].key);
        auto fmt = FormatField(key, &v, rec.line);
        if(!(key in cfg.fmts)){
            hts_log_warning(__FUNCTION__, "Format Key "~key~" not found in header! Ignoring...");
            continue;
        }
        auto hdrInfo = cfg.fmts[key];
        
        JsonValue * data;
        final switch(fmt.type){
            // char/string
            case BcfRecordType.Char:
                auto vals = fmt.to!string;
                foreach (i,sample; cfg.samples) {
                    (*bySample)[sample][key]= vals[i][0].idup;
                }
                continue;
            // float or float array
            case BcfRecordType.Float:
                data = parseFormat!float(fmt, hdrInfo);
                break;
            // int type or array
            case BcfRecordType.Int8:
            case BcfRecordType.Int16:
            case BcfRecordType.Int32:
            case BcfRecordType.Int64:
                data = parseFormat!long(fmt, hdrInfo);
                break;
            case BcfRecordType.Null:
                break;
        }

        if(hdrInfo.n == HeaderLengths.OnePerAllele){
            for(auto i=0; i < data.length; i++)
            {
                for(auto j=0; j < numAlts; j++)
                {   
                    (*bySample)[samples[i]]["byAllele"][j][key] = (*data)[i][j];
                }
            }
        } else {
            if(fmt.n == 1){
                for(auto i=0; i < data.length; i++)
                {
                    (*bySample)[samples[i]][key] = (*data)[i][0];
                }
            } else {
                for(auto i=0; i < data.length; i++)
                {
                    (*bySample)[samples[i]][key] = (*data)[i];
                }
            }
            
        }
    }
    return bySample;
}


JsonValue * parseFormat(T)(ref FormatField item, FieldInfo hdrInfo) {

    switch(hdrInfo.n){
        case HeaderLengths.OnePerAllele:
            return parseOnePerAllele!T(item);
        case HeaderLengths.OnePerAltAllele:
            return parseOnePerAltAllele!T(item);
        default:
            return parseOnePerAltAllele!T(item);
    }
}


unittest
{
    import htslib.vcf;
    import dhtslib.coordinates;

    auto hdr = VCFHeader(bcf_hdr_init("w\0"c.ptr));

    hdr.addHeaderLineKV("contig", "<ID=20,length=62435964,assembly=B36,md5=f126cdf8a6e0c7f379d618ff66beb2da,species=\"Homo sapiens\",taxonomy=x>"); // @suppress(dscanner.style.long_line)
    hdr.addHeaderLineKV("INFO", "<ID=DP,Number=1,Type=Integer,Description=\"Total Depth\">");
    hdr.addHeaderLineKV("INFO", "<ID=DP2,Number=2,Type=Integer,Description=\"Total Depth\">");
    hdr.addHeaderLineKV("INFO", "<ID=AD,Number=R,Type=Integer,Description=\"Total Depth\">");
    hdr.addHeaderLineKV("INFO", "<ID=AF,Number=A,Type=Float,Description=\"Total Depth\">");
    hdr.addHeaderLineKV("INFO", "<ID=ST,Number=1,Type=String,Description=\"Total Depth\">");

    hdr.addHeaderLineKV("FORMAT", "<ID=DP,Number=1,Type=Integer,Description=\"Total Depth\">");
    hdr.addHeaderLineKV("FORMAT", "<ID=DP2,Number=2,Type=Integer,Description=\"Total Depth\">");
    hdr.addHeaderLineKV("FORMAT", "<ID=AD,Number=R,Type=Integer,Description=\"Total Depth\">");
    hdr.addHeaderLineKV("FORMAT", "<ID=AF,Number=A,Type=Float,Description=\"Total Depth\">");
    hdr.addSample("sam1");
    hdr.addSample("sam2");

    auto rec = VCFRecord(hdr, bcf_init1());
    rec.chrom = "20";
    rec.pos = ZB(1);
    rec.alleles = ["G","A","C"];
    rec.addInfo("DP", 2);
    rec.addInfo("DP2", [2,3]);
    rec.addInfo("AD", [2,3,4]);
    rec.addInfo("AF", [1.0, 0.5]);
    rec.addInfo("ST", "test");

    rec.addFormat("DP", [2,0]);
    rec.addFormat("DP2", [2,3,0,0]);
    rec.addFormat("AD", [2,3,4,0,0,0]);
    // rec.addFormat("AF", [1.0, 0.5, 0.0, 0.1]);

    auto cfg = getHeaderConfig(hdr);

    import std.stdio;
    // writeln(rec.getInfos);
    // writeln(rec.getFormats);
    writeln(parseInfos(rec.getInfos,cfg, rec.altAllelesAsArray.length).serializeToAsdf);
    writeln(parseFormats(rec.getFormats,cfg, rec.altAllelesAsArray.length, hdr.getSamples).serializeToAsdf);
    
}

auto expandBySample(JsonValue * obj)
{
    auto samples = (*obj)["FORMAT"].asObject;
    return samples.byKey.map!((y) {
        auto root = new JsonValue(obj.dup.value);
        (*root)["sample"] = y;
        (*root)["FORMAT"] = JsonValue((*samples)[y]);
        return root;
    });
}

auto expandByAnno(JsonValue * obj, string key)
{
    auto annos = (*(*obj)["INFO"][key].asArray);
    auto len = (*obj)["INFO"][key].length;
    return iota(len)
        .filter!(y => JsonValue(annos[y])["allele"].asValue!string == (*obj)["ALT"].asValue!string)
        .map!((y) {
            auto root = new JsonValue(obj.dup.value);
            (*root)["INFO"][key] = JsonValue(annos[y]);
            return root;
        });
}

auto expandMultiAllelicSites(bool sampleExpanded)(JsonValue * obj, ulong numAlts)
{
    static if(sampleExpanded){
        auto i_exists = ("INFO" in (*(*obj).asObject)) != null;
        auto iba_exists = i_exists ? (("by_allele" in (*(*obj)["INFO"].asObject)) != null) : false;

        auto f_exists = ("FORMAT" in (*(*obj).asObject)) != null;
        auto fba_exists = f_exists ? (("by_allele" in (*(*obj)["FORMAT"].asObject)) != null) : false;

        auto info_vals = iba_exists ? (*(*obj)["INFO"]["by_allele"].asArray) : [];
        auto fmt_vals = fba_exists ? (*(*obj)["FORMAT"]["by_allele"].asArray) : [];
        return iota(numAlts).map!((i) {

            auto root = new JsonValue(obj.dup.value);

            if(fba_exists)
                (*root)["FORMAT"].asObject.remove("by_allele");

            if(iba_exists)
                (*root)["INFO"].asObject.remove("by_allele");

            if(info_vals.length > 0){
                assert(info_vals.length == numAlts);
                auto vals = (*JsonValue(info_vals[i]).asObject);
                foreach (k; vals.byKey)
                {
                    (*root)["INFO"][k] = JsonValue(vals[k]);
                }
            }

            if(fmt_vals.length > 0){
                assert(fmt_vals.length == numAlts);
                auto vals = (*JsonValue(fmt_vals[i]).asObject);
                foreach (k; vals.byKey)
                {
                    (*root)["FORMAT"][k] =JsonValue(vals[k]);
                }
            }

            if (numAlts > 1)
                (*root)["ALT"] = (*obj)["ALT"][i].asValue!string;
            return root;
        });
    }else{
        auto i_exists = ("INFO" in (*(*obj).asObject)) != null;
        auto iba_exists = i_exists ? (("by_allele" in (*(*obj)["INFO"].asObject)) != null) : false;

        auto info_vals = (*(*obj)["INFO"]["by_allele"].asArray);
        auto fmt = (*(*obj)["FORMAT"].asObject);
        return iota(numAlts).map!((i) {

            auto root = new JsonValue(obj.dup.value);

            if(iba_exists)
                (*root)["INFO"].asObject.remove("by_allele");

            if(info_vals.length > 0){
                assert(info_vals.length == numAlts);
                auto vals = (*JsonValue(info_vals[i]).asObject);
                foreach (k; vals.byKey)
                {
                    (*root)["INFO"][k] = JsonValue(vals[k]);
                }
            }

            foreach (key; fmt.byKey)
            {
                auto f_exists = ("FORMAT" in (*(*obj).asObject)) != null;
                auto fba_exists = f_exists ? (("by_allele" in (*(*obj)["FORMAT"][key].asObject)) != null) : false;
                if(fba_exists)
                    (*root)["FORMAT"][key].asObject.remove("by_allele");
                auto fmt_vals = fba_exists ? (*JsonValue(fmt[key])["byAllele"].asArray) : [];
                if(fmt_vals.length > 0){
                    auto vals = (*JsonValue(fmt_vals[i]).asObject);
                    foreach (k; vals.byKey)
                    {
                        (*root)["FORMAT"][key][k] = JsonValue(vals[k]);
                    }
                }    
            }
            

            if (numAlts > 1)
                (*root)["ALT"] = JsonValue((*(*obj)["ALT"].asArray)[i]);
            return root;
        });
    }
}

void dropNullGenotypes(JsonValue * obj)
{   
    auto fmt = (*(*obj)["FORMAT"].asObject);
    foreach(sample;fmt.byKey) {
        if (JsonValue(fmt[sample])["GT"].asValue!string[0] =='.')
            fmt.remove(sample);
    } 
}


Asdf md5sumObject(Asdf obj) {
    // create md5 object
    auto md5 = new MD5Digest();
    auto root = AsdfNode(obj);
    root["md5"] = AsdfNode(serializeToAsdf(md5.digest(obj.data).toHexString));
    return cast(Asdf)root;
}

void applyOperations(JsonValue * obj, bool anno, bool allele, bool sam, bool gen, bool norm, shared int * input_count, shared int * output_count)
{
    import std.stdio;
    import core.atomic: atomicOp;
    atomicOp!"+="(*input_count, 1);
    if(gen){
        dropNullGenotypes(obj);
    }
    if(sam && allele && anno){
        auto numAlts = (*(*obj)["ALT"].value).tryMatch!(
            (string x) => 1,
            (JsonArray x) => x.length
        );
        auto range = expandBySample(obj)
            .map!(x => expandMultiAllelicSites!true(x, numAlts)).joiner
            .map!(x => expandByAnno(x, "ANN")).joiner;
        if(norm){
            range.map!(x => normalize(x))
                .each!((x) {
                    writeln(x.serializeToAsdf.md5sumObject);
                    atomicOp!"+="(*output_count, 1);
                });
        }else{
            range.each!((x) {
                    writeln(x.serializeToAsdf.md5sumObject);
                    atomicOp!"+="(*output_count, 1);
                });
        }
    } else if(sam) {
        auto range = expandBySample(obj);
        if(norm){
            range.map!(x => normalize(x))
                .each!((x) {
                    writeln(x.serializeToAsdf.md5sumObject);
                    atomicOp!"+="(*output_count, 1);
                });
        }else{
            range.each!((x) {
                    writeln(x.serializeToAsdf.md5sumObject);
                    atomicOp!"+="(*output_count, 1);
                });
        }
    } else if(allele) {
        auto numAlts = (*(*obj)["ALT"].value).tryMatch!(
            (string x) => 1,
            (JsonArray x) => x.length
        );
        auto range = expandMultiAllelicSites!false(obj, numAlts);
        if(norm){
            range.map!(x => normalize(x))
                .each!((x) {
                    writeln(x.serializeToAsdf.md5sumObject);
                    atomicOp!"+="(*output_count, 1);
                });
        }else{
            range.each!((x) {
                    writeln(x.serializeToAsdf.md5sumObject);
                    atomicOp!"+="(*output_count, 1);
                });
        }
    } else if(anno){
        auto numAlts = (*(*obj)["ALT"].value).tryMatch!(
            (string x) => 1,
            (JsonArray x) => x.length
        );
        auto range = expandMultiAllelicSites!true(obj, numAlts)
            .map!(x => expandByAnno(x, "ANN"))
            .joiner;
        if(norm){
            range.map!(x => normalize(x))
                .each!((x) {
                    writeln(x.serializeToAsdf.md5sumObject);
                    atomicOp!"+="(*output_count, 1);
                });
        }else{
            range.each!((x) {
                    writeln(x.serializeToAsdf.md5sumObject);
                    atomicOp!"+="(*output_count, 1);
                });
        }
    } else {
        if(norm){
            auto val = normalize(obj);
            writeln(val.serializeToAsdf.md5sumObject);
            atomicOp!"+="(*output_count, 1);
        }else{
            writeln(obj.serializeToAsdf.md5sumObject);
            atomicOp!"+="(*output_count, 1);
        }
    }
}