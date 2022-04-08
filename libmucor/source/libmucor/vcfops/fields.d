module libmucor.vcfops.fields;

import std.stdio;
import std.algorithm : splitter, map;
import std.array : array, split;
import std.conv : to;
import std.range : enumerate, chunks;


import asdf;
import dhtslib.vcf;
import htslib.hts_log;
import libmucor.vcfops;
import libmucor.jsonlops;

/// JSON types
enum TYPES{
    FLOAT,
    INT,
    STRING,
    BOOL,
    NULL
}

string[16] ANN_FIELDS = [
        "allele",
		"effect",
		"impact",
		"gene_name",
		"gene_id",
		"feature_type",
		"feature_id",
		"transcript_biotype",
		"rank_total",
		"hgvs_c",
		"hgvs_p",
		"cdna_position",
		"cds_position",
		"protein_position",
		"distance_to_feature",
		"errors_warnings_info"];

string[4] LOF_FIELDS =["Gene","ID","num_transcripts","percent_affected"];
TYPES[4] LOF_TYPES =[TYPES.STRING,TYPES.STRING,TYPES.INT,TYPES.FLOAT];

void parseAnnotationField(JsonValue * info_root, string key, string[] field_identifiers, TYPES[] types = [], bool condense = true)
{
    // if not in INFO, return
    if(!(key in (*(*info_root).asObjectRef))) return;

    // if types empty, all types are encoded as strings
    if(types == []) {
        types.length = field_identifiers.length;
        types[] = TYPES.STRING;
    }
    string field_value = (*info_root)[key].asValue!string;
    
    if (field_value.length ==0) return;
	// specs page 2:
	// Multiple effects / consequences are separated by comma.
    string[] anns = field_value.split(",");

    auto ann_array = makeJsonArray(anns.length);
	foreach(i,ann;anns){
        if(ann[0]=='(') ann=ann[1..$];
        if(ann[$-1]==')') ann=ann[0..$-1];
        auto ann_root = makeJsonObject;
        foreach(j,field;ann.splitter("|").enumerate){
            if(field=="" && condense == true) continue;
            else if(field=="") field=".";
            string[] values = field.splitter("&").array;
            int[] int_values;
            float[] float_values;
            auto id = field_identifiers[j];
            final switch(types[j]){
                case TYPES.STRING:
                    if(values.length==1 && condense == true)         
                        ann_root[id]=values[0];
                    else
                        ann_root[id]=values;
                    break;
                case TYPES.FLOAT:
                    //writeln(field_identifiers[i]," ",values);
                    if(values.length==1 && condense == true)         
                        ann_root[id]=values[0].to!float;
                    else
                        ann_root[id]=values.map!(to!float).array;
                    break;
                case TYPES.INT:
                    if(values.length==1 && condense == true)         
                        ann_root[id]=values[0].to!int;
                    else
                        ann_root[id]=values.map!(to!int).array;
                    break;
                case TYPES.BOOL:
                case TYPES.NULL:
                    break;
            }
        }
		ann_array[i] = ann_root;
	}
    (*info_root)[key] = ann_array;
    // writeln(cast(Asdf)root);
}

// unittest{
//     string ann = "\"A|intron_variant|MODIFIER|PLCXD1|ENSG00000182378|Transcript|ENST00000381657|"~
//                 "protein_coding||1/6|ENST00000381657.2:c.-21-26C>A|||||,A|intron_variant|MODIFIER"~
//                 "|PLCXD1|ENSG00000182378|Transcript|ENST00000381663|protein_coding||1/7|ENST00000381663.3:c.-21-26C>A||"~
//                 "|||\"";
//     auto root = AsdfNode(`{"INFO":{}}`.parseJson);
//     root["INFO","ANN"] = AsdfNode(ann.parseJson);
//     import std.stdio;
//     writeln(cast(Asdf)parseAnnotationField(root,"ANN",[],[]));
// }

Asdf parseInfoFields(VCFRecord record, HeaderConfig cfg) {
    // prepare info root object
    auto info_root=AsdfNode("{}".parseJson);
    info_root["by_allele"] = AsdfNode("{}".parseJson);
    auto alleles = record.allelesAsArray();
    AsdfNode[] info_values = new AsdfNode[(alleles.length - 1)]; 
    info_values[] = AsdfNode("{}".parseJson);
    auto infos = record.getInfos;
    // Go by each vcf info field and by type
    // convert to native type then to asdf
    //  
    foreach (key, info; infos)
    {
        auto hdrInfo = cfg.infos[key];
        
        final switch(info.type){
            // char/string
            case BcfRecordType.Char:
                info_root[key]=AsdfNode(info.to!string.serializeToAsdf);
                break;
            // float or float array
            case BcfRecordType.Float:
                parseFieldsMixin!(InfoField, float)(info_root, info, key, info_values, alleles, [], hdrInfo);
                break;
            // int type or array
            case BcfRecordType.Int8:
            case BcfRecordType.Int16:
            case BcfRecordType.Int32:
            case BcfRecordType.Int64:
                parseFieldsMixin!(InfoField, long)(info_root, info, key, info_values, alleles, [], hdrInfo);
                break;
            case BcfRecordType.Null:
                info_root[key]=AsdfNode("null".parseJson);
                break;
        }
    }
    info_root["by_allele"] = AsdfNode(makeAsdfArray(info_values.map!(x=>cast(Asdf)x).array));
    return cast(Asdf) info_root;
}

Asdf parseFormatFields(VCFRecord record, HeaderConfig cfg) {
    // prepare info root object
    auto format_root=AsdfNode("{}".parseJson);
    auto alleles = record.allelesAsArray();
    auto genotypes = record.getGenotypes;

    //
    AsdfNode[] format_values = new AsdfNode[cfg.samples.length * (alleles.length - 1)];
    format_values[] = AsdfNode("{}".parseJson);
    foreach (i, sample; cfg.samples)
    {
        format_root[sample] = AsdfNode("{}".parseJson);
        format_root[sample]["by_allele"] = AsdfNode("{}".parseJson);
    }
    auto fmts = record.getFormats;
    fmts.remove("GT");
    // Go by each vcf info field and by type
    // convert to native type then to asdf
    //  
    foreach (i,sample; cfg.samples) {
        format_root[sample]["GT"]= AsdfNode(genotypes[i].toString.serializeToAsdf);
        format_root[sample]["Ploidy"]= AsdfNode(genotypes[i].getPloidy.serializeToAsdf);
    }
    foreach (key, fmt; fmts)
    {
        auto hdrInfo = cfg.fmts[key];
        
        final switch(fmt.type){
            // char/string
            case BcfRecordType.Char:
                auto vals = fmt.to!string;
                foreach (i,sample; cfg.samples) {
                    format_root[sample][key]= AsdfNode(vals[i][0].serializeToAsdf);
                }
                break;
            // float or float array
            case BcfRecordType.Float:
                parseFieldsMixin!(FormatField, float)(format_root, fmt, key, format_values, alleles, cfg.samples, hdrInfo);
                break;
            // int type or array
            case BcfRecordType.Int8:
            case BcfRecordType.Int16:
            case BcfRecordType.Int32:
            case BcfRecordType.Int64:
                parseFieldsMixin!(FormatField, long)(format_root, fmt, key, format_values, alleles, cfg.samples, hdrInfo);
                break;
            case BcfRecordType.Null:
                format_root[key]=AsdfNode("null".parseJson);
                break;
        }
    }
    auto view = format_values.chunks(alleles.length - 1);
    foreach (i, sample; cfg.samples)
    {
        format_root[sample]["by_allele"] = AsdfNode(makeAsdfArray(view[i].map!(x=>cast(Asdf)x).array));
    }
    return cast(Asdf) format_root;
}

void parseFieldsMixin(T, V)(ref AsdfNode root, ref T item, string key, ref AsdfNode[] item_vals, string[] alleles, string[] samples, FieldInfo hdrInfo) {
    static if(is(T == FormatField)) {
        auto itemLen = item.n;
        alias vtype = V;
    } else {
        auto itemLen = item.len;
        alias vtype = V[];
    }
    switch(hdrInfo.n){
        case HeaderLengths.OnePerAllele:
            if(alleles.length != itemLen) {
                hts_log_warning(__FUNCTION__,T.stringof ~" "~key~" doesn't have same number of values as header indicates! Skipping...");
                break;
            }
            auto vals = item.to!vtype;
            static if(is(T == FormatField)) {
                foreach (i,val; vals.enumerate)
                {
                    assert(itemLen == val.length);
                    assert(itemLen == alleles.length);
                    
                    for(auto j=0; j < itemLen - 1; j++){
                        item_vals[(i * (itemLen - 1)) + j][key] = AsdfNode([val[0], val[j + 1]].serializeToAsdf);
                    }
                }
            } else {
                foreach (i,val; vals)
                {
                    if (i==0) continue;
                    item_vals[i-1][key] = AsdfNode([vals[0], val].serializeToAsdf);
                }
            }     
            break;
        case HeaderLengths.OnePerAltAllele:
            if((alleles.length - 1) != itemLen) {
                hts_log_warning(__FUNCTION__,"Format field "~key~" doesn't have same number of values as header indicates! Skipping...");
                break;
            }
            auto vals = item.to!vtype;
            static if(is(T == FormatField)) {
                foreach (i,val; vals.enumerate)
                {
                    for(auto j=0; j< itemLen; j++){
                        item_vals[i*j][key] = AsdfNode(val[j].serializeToAsdf);
                    }
                }
            } else {
                foreach (i,val; vals)
                {
                    item_vals[i][key] = AsdfNode(val.serializeToAsdf);
                }
            }
            
            break;
        default:
            auto vals = item.to!vtype;
            static if(is(T == FormatField)) {
                foreach (i,val; vals.enumerate)
                {
                    auto sam = samples[i];
                    for(auto j=0; j< itemLen; j++){
                        if(val.length == 1)
                            root[sam][key] = AsdfNode(val[0].serializeToAsdf);
                        else
                            root[sam][key] = AsdfNode(val.serializeToAsdf);
                    }
                }
            } else {
                if(itemLen > 1)
                    root[key]=AsdfNode(vals.serializeToAsdf);
                else
                    root[key]=AsdfNode(vals[0].serializeToAsdf);
            }
            break;
    }
}
