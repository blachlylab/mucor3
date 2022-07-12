module libmucor.atomize.ann;

import std.stdio;
import std.algorithm : splitter, map, count, canFind, countUntil;
import std.array : array, split;
import std.conv : to;
import std.range : enumerate, chunks;
import std.traits : ReturnType;
import std.typecons : No;
import std.range;

import dhtslib.vcf;
import htslib.hts_log;
import libmucor.vcfops;
import libmucor.jsonlops;
import libmucor.error;

/// Structured VCF String field 
/// List of objects
struct Annotations {
    /// field names
    string[] fieldnames;
    /// field types
    TYPES[] types;
    /// slice of original string 
    string original;
    /// range of individual annotations
    ReturnType!(getRange) annotations;

    this(string val, string[] fieldnames, TYPES[] types = []) {
        // if types empty, all types are encoded as strings
        if(types == []) this.types = new TYPES[fieldnames.length], this.types[] = TYPES.STRING;
        else this.types = types;
        this.fieldnames = fieldnames;
        this.original = val;
        this.annotations = getRange;
    }

    /// helper function
    auto getRange(){
        return original.splitter(",");
    }

    /// get number of annotations
    auto length() {
        return getRange.count;
    }

    /// range functions
    auto front() {
        return Annotation(this.annotations.front, this.fieldnames, this.types);
    }

    /// range functions
    void popFront() {
        this.annotations.popFront;
    }

    /// range functions
    auto empty() {
        return this.annotations.empty;
    }

    auto opIndex(size_t i) {
        return Annotation(getRange.drop(i).front,this.fieldnames, this.types);
    }

    auto toString() {
        return this.original;
    }
}

struct Annotation {
    /// field names
    string[] origfieldnames;

    /// field names arr that is mutated
    string[] fieldnames;

    /// field types
    TYPES[] origtypes;

    /// field type arr that is mutated
    TYPES[] types;

    /// slice of original string 
    string original;

    /// range of annotation fields
    ReturnType!(getRange) fields;

    this(string val, string[] fieldnames, TYPES[] types) {
        this.origtypes = this.types = types;
        this.fieldnames = this.origfieldnames = fieldnames;
        if (val[0] == '(')
            val = val[1 .. $];
        if (val[$ - 1] == ')')
            val = val[0 .. $ - 1];
        this.original = val;
        
        this.fields = getRange;
        assert(this.fieldnames.length == original.splitter("|").count);
    }

    auto getRange() {
        return original.splitter("|");
    }

    auto length() {
        return getRange.count;
    }

    auto front() {
        return AnnotationField(this.fieldnames.front, this.types.front, this.fields.front);
    }

    void popFront() {
        this.fields.popFront;
        this.fieldnames.popFront;
        this.types.popFront;
    }

    auto empty() {
        return this.fields.empty;
    }

    auto opIndex(string index)
    {
        assert(this.fieldnames.canFind(index));
        auto i = this.fieldnames.countUntil(index);
        if(i < 0) log_err(__FUNCTION__, "Index doesn't exist: %s", index);
        return AnnotationField(this.origfieldnames[i], this.origtypes[i], getRange.dropExactly(i).front);
    }
    auto toString() {
        return this.original;
    }
}

struct AnnotationField {
    string name;
    string origvalue;
    string[] value;
    TYPES type;

    this(string name, TYPES type, string val) {
        this.type = type;
        this.name = name;
        this.origvalue = val;
        this.value = val.split("&");
    }

    auto isNull(){
        return this.origvalue == "";
    }

    auto parse(T)(){
        return this.value.map!(x => x.to!T).array;
    }
    
    auto toString() {
        return this.origvalue;
    }
}

/// JSON types
enum TYPES
{
    FLOAT,
    INT,
    STRING,
    BOOL,
    NULL
}

string[16] ANN_FIELDS = [
    "allele", "effect", "impact", "gene_name", "gene_id", "feature_type",
    "feature_id", "transcript_biotype", "rank_total", "hgvs_c", "hgvs_p",
    "cdna_position", "cds_position", "protein_position",
    "distance_to_feature", "errors_warnings_info"
];

string[4] LOF_FIELDS = ["Gene", "ID", "num_transcripts", "percent_affected"];
TYPES[4] LOF_TYPES = [TYPES.STRING, TYPES.STRING, TYPES.INT, TYPES.FLOAT];

void parseAnnotationField(Json* info_root, string key,
        string[] field_identifiers, TYPES[] types = [], bool condense = true)
{
    // if not in INFO, return
    if (!(key in (*(*info_root).asObjectRef)))
        return;

    string field_value = (*info_root)[key].asValue!string;

    if (field_value.length == 0)
        return;
    
    // specs page 2:
    // Multiple effects / consequences are separated by comma.
    Annotations anns = Annotations(field_value, field_identifiers, types);
    auto ann_array = makeJsonArray(anns.length);
    foreach(i, ann; anns.enumerate)
    {
        auto ann_root = makeJsonObject;
        foreach (field; ann)
        {
            if (field.isNull && condense == true)
                continue;
            else if (field.isNull)
                field.value[0] = ".";
            final switch (field.type)
            {
            case TYPES.STRING:
                auto values = field.value;
                if (values.length == 1 && condense == true)
                    ann_root[field.name] = values[0];
                else
                    ann_root[field.name] = values;
                break;
            case TYPES.FLOAT:
                auto values = field.parse!float;
                if (values.length == 1
                        && condense == true)
                    ann_root[field.name] = values[0];
                else
                    ann_root[field.name] = values;
                break;
            case TYPES.INT:
                auto values = field.parse!int;
                if (values.length == 1 && condense == true)
                    ann_root[field.name] = values[0];
                else
                    ann_root[field.name] = values;
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
