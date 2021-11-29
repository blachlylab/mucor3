module fields;

import std.stdio;
import std.algorithm : splitter, map;
import std.array : array;
import std.conv : to;
import asdf;

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

AsdfNode parseAnnotationField(AsdfNode root, string key, string[] field_identifiers, TYPES[] types = [], bool condense = true){
    if(!(key in root["INFO"].children)) return root;
    if(types == []) {
        types.length = field_identifiers.length;
        types[] = TYPES.STRING;
    }
    string[] anns;
    if(root["INFO",key].data.kind==Asdf.Kind.array)
        anns = deserialize!(string[])(root["INFO",key].data);
    else
        anns ~= deserialize!string(root["INFO",key].data);
    if (anns.length ==0) return root;
	// specs page 2:
	// Multiple effects / consequences are separated by comma.
    auto copy = anns.dup;
    anns = [];
	foreach(ann;copy){
        anns~=ann.splitter(",").array;
    }
    AsdfNode[] ann_array;
	foreach(ann;anns){
        if(ann[0]=='(') ann=ann[1..$];
        if(ann[$-1]==')') ann=ann[0..$-1];
        AsdfNode ann_root = AsdfNode("{}".parseJson);
        foreach(i,field;ann.splitter("|").array){
            if(field=="" && condense == true) continue;
            else if(field=="") field=".";
            string[] values = field.splitter("&").array;
            int[] int_values;
            float[] float_values;
            final switch(types[i]){
                case TYPES.STRING:
                    if(values.length==1 && condense == true)         
                        ann_root[field_identifiers[i]]=AsdfNode(serializeToAsdf(values[0]));
                    else
                        ann_root[field_identifiers[i]]=AsdfNode(serializeToAsdf(values));
                    break;
                case TYPES.FLOAT:
                    //writeln(field_identifiers[i]," ",values);
                    if(values.length==1 && condense == true)         
                        ann_root[field_identifiers[i]]=AsdfNode(serializeToAsdf(values[0].to!float));
                    else
                        ann_root[field_identifiers[i]]=AsdfNode(serializeToAsdf(values.map!(to!float).array));
                    break;
                case TYPES.INT:
                    if(values.length==1 && condense == true)         
                        ann_root[field_identifiers[i]]=AsdfNode(serializeToAsdf(values[0].to!int));
                    else
                        ann_root[field_identifiers[i]]=AsdfNode(serializeToAsdf(values.map!(to!int).array));
                    break;
                case TYPES.BOOL:
                case TYPES.NULL:
                    break;
            }
        }
		ann_array~=ann_root;
	}
    root["INFO",key] = AsdfNode(serializeToAsdf(ann_array));
    // writeln(cast(Asdf)root);
    return root;
}

unittest{
    string ann = "\"A|intron_variant|MODIFIER|PLCXD1|ENSG00000182378|Transcript|ENST00000381657|"~
                "protein_coding||1/6|ENST00000381657.2:c.-21-26C>A|||||,A|intron_variant|MODIFIER"~
                "|PLCXD1|ENSG00000182378|Transcript|ENST00000381663|protein_coding||1/7|ENST00000381663.3:c.-21-26C>A||"~
                "|||\"";
    auto root = AsdfNode(`{"INFO":{}}`.parseJson);
    root["INFO","ANN"] = AsdfNode(ann.parseJson);
    import std.stdio;
    writeln(cast(Asdf)parseAnnotationField(root,"ANN",ANN_FIELDS[],ANN_TYPES[]));
}
