module varquery.invertedindex;
import std.algorithm.setops;
import std.regex;
import std.algorithm : sort, uniq, map, std_filter = filter, joiner;
import std.range : iota;
import std.array : array;
import std.conv : to;
import std.format : format;
import std.string : indexOf;

import asdf: deserializeAsdf = deserialize, Asdf, AsdfNode, parseJson, serializeToAsdf;
import varquery.wideint : uint128;
import varquery.singleindex;

char sep = '/';

struct JSONInvertedIndex{
    uint128[] recordMd5s;
    InvertedIndex[char[]] fields;
    this(Asdf root){
        recordMd5s = root["md5s"].deserializeAsdf!(string[]).map!((x){ uint128 a; a.fromHexString(x); return a;}).array;
        root["md5s"].remove;
        foreach (field,idx; root.byKeyValue)
        {
            fields[field] = InvertedIndex(deserializeAsdf!string(idx.byKeyValue.front.value["type"]).to!TYPES);
            foreach (key, value; idx.byKeyValue)
            {
                float floatval;
                string stringval;
                bool boolval;
                switch(fields[field].type){
                    case TYPES.FLOAT:
                        floatval=key.to!float;
                        fields[field].hashmap[serialize(&floatval)]=deserializeAsdf!(ulong[])(value["values"]);
                        break;
                    case TYPES.STRING:
                        stringval = key.dup;
                        fields[field].hashmap[serialize(&stringval)]=deserializeAsdf!(ulong[])(value["values"]);
                        break;
                    case TYPES.BOOL:
                        boolval=key.to!bool;
                        fields[field].hashmap[serialize(&boolval)]=deserializeAsdf!(ulong[])(value["values"]);
                        break;
                    default:
                        break;
                }
            }
        }

    }
    void addJsonObject(Asdf root, const(char)[] path = ""){
        if(path == ""){
            uint128 a;
            auto md5 = root["md5"].deserializeAsdf!string;
            root["md5"].remove;
            a.fromHexString(md5);
            this.recordMd5s ~= a;
        }
        foreach (key,value; root.byKeyValue)
        {
            const(byte)[] valkey;
            float floatval;
            string stringval;
            bool boolval;
            TYPES keytype;
            final switch(value.kind){
                    case Asdf.Kind.number:
                        floatval =deserializeAsdf!float(value);
                        valkey = serialize(&floatval);
                        keytype = TYPES.FLOAT;
                        break;
                    case Asdf.Kind.string:
                        stringval =deserializeAsdf!string(value);
                        if(stringval=="nan") goto case Asdf.Kind.number;
                        valkey = serialize(&stringval);
                        keytype = TYPES.STRING;
                        break;
                    case Asdf.Kind.true_:
                        boolval =true;
                        valkey = serialize(&boolval);
                        keytype = TYPES.BOOL;
                        break;
                    case Asdf.Kind.false_:
                        boolval =false;
                        valkey = serialize(&boolval);
                        keytype = TYPES.BOOL;
                        break;
                    case Asdf.Kind.object:
                        addJsonObject(value, path~sep~key);
                        keytype = TYPES.NULL;
                        break;
                    case Asdf.Kind.array:
                        addJsonArray(value, path~sep~key);
                        keytype = TYPES.NULL;
                        break;
                    case Asdf.Kind.null_:
                        keytype = TYPES.NULL;
                        break;
                }
            if(keytype==TYPES.NULL) continue;
            auto p = path~sep~key in fields;
            if(p){
                (*p).hashmap[valkey]~= this.recordMd5s.length - 1; 
            }else{
                fields[path~sep~key] = InvertedIndex(keytype);
                fields[path~sep~key].hashmap[valkey] ~= this.recordMd5s.length - 1;
            }
        }
        
    }
    void addJsonArray(Asdf root, const(char)[] path){
        assert(path != "");
        foreach (value; root.byElement)
        {
            const(byte)[] valkey;
            float floatval;
            string stringval;
            bool boolval;
            TYPES keytype;
            // import std.stdio;
            // writeln(path," ",value);
            final switch(value.kind){
                    case Asdf.Kind.number:
                        floatval =deserializeAsdf!float(value);
                        // writeln(floatval);
                        valkey = serialize(&floatval);
                        // writeln(valkey);
                        // writeln(deserialize!float(valkey));
                        keytype = TYPES.FLOAT;
                        break;
                    case Asdf.Kind.string:
                        stringval =deserializeAsdf!string(value);
                        if(stringval=="nan") goto case Asdf.Kind.number;
                        valkey = serialize(&stringval);
                        keytype = TYPES.STRING;
                        break;
                    case Asdf.Kind.true_:
                        boolval =true;
                        valkey = serialize(&boolval);
                        keytype = TYPES.BOOL;
                        break;
                    case Asdf.Kind.false_:
                        boolval =false;
                        valkey = serialize(&boolval);
                        keytype = TYPES.BOOL;
                        break;
                    case Asdf.Kind.object:
                        addJsonObject(value,path);
                        keytype = TYPES.NULL;
                        break;
                    case Asdf.Kind.array:
                        addJsonArray(value,path);
                        keytype = TYPES.NULL;
                        break;
                    case Asdf.Kind.null_:
                        keytype = TYPES.NULL;
                        break;
                }
            if(keytype==TYPES.NULL) continue;
            auto p = path in fields;
            if(p){
                (*p).hashmap[valkey]~= this.recordMd5s.length - 1; 
            }else{
                fields[path] = InvertedIndex(keytype);
                fields[path].hashmap[valkey] ~= this.recordMd5s.length - 1;
            }
        }
        
    }

    Asdf toJson(){
        AsdfNode root = AsdfNode("{}".parseJson);
        root["md5s"] = AsdfNode(serializeToAsdf(this.recordMd5s.map!(x => format("%x", x)).array));
        foreach (field,idx; fields)
        {
            if(field.length>255) continue;
            root[field] = AsdfNode("{}".parseJson);
            foreach (key, value; idx.hashmap)
            {
                string newkey;
                switch(idx.type){
                    case TYPES.FLOAT:
                        newkey = deserialize!float(key).to!string;
                        if(newkey.length>255) continue;
                        root[field,newkey]=AsdfNode("{}".parseJson);
                        root[field,newkey,"values"] = AsdfNode(serializeToAsdf(value));
                        root[field,newkey,"type"] = AsdfNode(serializeToAsdf(TYPES.FLOAT));
                        break;
                    case TYPES.STRING:
                        newkey = deserialize!string(key);
                        if(newkey.length>255) continue;
                        root[field,newkey]=AsdfNode("{}".parseJson);
                        root[field,newkey,"values"] = AsdfNode(serializeToAsdf(value));
                        root[field,newkey,"type"] = AsdfNode(serializeToAsdf(TYPES.STRING));
                        break;
                    case TYPES.BOOL:
                        newkey = deserialize!bool(key).to!string;
                        if(newkey.length>255) continue;
                        root[field,newkey]=AsdfNode("{}".parseJson);
                        root[field,newkey,"values"] = AsdfNode(serializeToAsdf(value));
                        root[field,newkey,"type"] = AsdfNode(serializeToAsdf(TYPES.BOOL));
                        break;
                    default:
                        break;
                }
            }
        }
        return cast(Asdf) root;
    }

    ulong[] allIds(){
        return iota(0, this.recordMd5s.length).array;
        // return this.recordMd5s;
    }

    InvertedIndex*[] getFields(string key)
    {
        if(key[0] != '/') throw new Exception("key is missing leading /");
        auto wildcard = key.indexOf('*');
        if(wildcard == -1){
            auto p = key in fields;
            if(!p) throw new Exception(" key "~key~" is not found");
            return [p];
        }else{
            key = key[0..wildcard] ~"."~ key[wildcard..$];
            wildcard = key.indexOf('*');
            while(wildcard != -1){
                key = key[0..wildcard] ~"."~ key[wildcard..$];
                wildcard = key.indexOf('*');
            }
            auto reg = regex("^" ~ key ~"$");
            return fields.byKey.std_filter!(x => !(x.matchFirst(reg).empty)).map!(x=> &(fields[x])).array;
        }
    }

    ulong[] query(T)(string key,T value){
        auto matchingFields = getFields(key);
        return matchingFields
                .map!(x=> (*x).filter([value]))
                .joiner.array.sort.uniq.array;
    }
    ulong[] queryRange(T)(string key,T first,T second){
        auto matchingFields = getFields(key);
        return matchingFields
                .map!(x=> (*x).filterRange([first,second]))
                .joiner.array.sort.uniq.array;
    }
    ulong[] queryOp(string op)(string key,float val){
        auto matchingFields = getFields(key);
        return matchingFields
                .map!(x=> (*x).filterOp!op(val))
                .joiner.array.sort.uniq.array;
    }
    ulong[] queryAND(T)(string key,T[] values){
        auto matchingFields = getFields(key);
        return matchingFields.map!((x){
            auto results = values.map!(y=>(*x).filter([y]).sort.uniq.array).array;
            auto intersect = results[0];
            foreach (item; results)
                intersect = setIntersection(intersect,item).array;
            return intersect.array;
        }).joiner.array.sort.uniq.array;
    }
    ulong[] queryOR(T)(string key,T[] values){
        auto matchingFields = getFields(key);
        return matchingFields.map!(x=> (*x).filter(values)).joiner.array.sort.uniq.array;
    }
    ulong[] queryNOT(ulong[] values){
        return allIds.sort.uniq.setDifference(values).array;
    }

    uint128[] convertIds(ulong[] ids)
    {
        return ids.map!(x => this.recordMd5s[x]).array;
    }
}

// unittest{
//     import asdf:Asdf,AsdfNode,parseJson;
//     import varquery.fields;
//     string ann = "\"A|intron_variant|MODIFIER|PLCXD1|ENSG00000182378|Transcript|ENST00000381657|"~
//                 "protein_coding||1/6|ENST00000381657.2:c.-21-26C>A|||||,A|intron_variant|MODIFIER"~
//                 "|PLCXD1|ENSG00000182378|Transcript|ENST00000381663|protein_coding||1/7|ENST00000381663.3:c.-21-26C>A||"~
//                 "|||\"";
//     auto root = AsdfNode(`{"INFO":{}}`.parseJson);
//     root["INFO","ANN"] = AsdfNode(ann.parseJson);
//     import std.stdio;
//     JSONInvertedIndex idx;
//     idx.addJsonObject(cast(Asdf)parseAnnotationField(root,"ANN",ANN_FIELDS[],ANN_TYPES[]),"1");
//     ann = "\"A|intron_variant|MODIFIER|PLCXD1|ENSG00000182378|Transcript|ENST00000381657|"~
//                 "protein_coding||1/6|ENST00000381657.2:c.-21-26C>A|||||,A|missense_variant|MODIFIER"~
//                 "|PLCXD1|ENSG00000182378|Transcript|ENST00000381663|protein_coding||1/7|ENST00000381663.3:c.-21-26C>A||"~
//                 "|||\"";
//     root["INFO","ANN"] = AsdfNode(ann.parseJson);
//     idx.addJsonObject(cast(Asdf)parseAnnotationField(root,"ANN",ANN_FIELDS[],ANN_TYPES[]),"2");
//     writeln(idx.fields["/INFO/ANN/effect"]);
//     writeln(idx.fields["/INFO/ANN/effect"].hashmap.keys.map!(x=>deserialize!string(x)));
//     writeln(idx.fields["/INFO/ANN/effect"].filter(["missense_variant"]));
// }
