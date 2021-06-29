module varquery.invertedindex;
import std.algorithm.setops;
import std.regex;
import std.algorithm : sort, uniq, map, std_filter = filter, joiner, each;
import std.range : iota;
import std.array : array, replace;
import std.conv : to;
import std.format : format;
import std.string : indexOf;
import std.bitmanip: nativeToLittleEndian, littleEndianToNative;
import std.stdio;
import std.exception : enforce;

import asdf: deserializeAsdf = deserialize, Asdf, AsdfNode, parseJson, serializeToAsdf;
import varquery.wideint : uint128;
import varquery.singleindex;

char sep = '/';

struct JSONInvertedIndex{
    uint128[] recordMd5s;
    InvertedIndex[char[]] fields;
    this(File f){
        fromFile(f);
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
            JSONValue valkey;
            if(value.kind == Asdf.Kind.object){
                addJsonObject(value, path~sep~key);
                continue;
            }else if(value.kind == Asdf.Kind.array){
                addJsonArray(value, path~sep~key);
                continue;
            }else if(value.kind == Asdf.Kind.null_){
                continue;
            }else{
                valkey = JSONValue(value);
            }
            auto p = path~sep~key in fields;
            if(p){
                auto val = (*p).hashmap.require(valkey,[]);
                (*val) ~= this.recordMd5s.length - 1;
            }else{
                fields[path~sep~key] = InvertedIndex();
                auto val = fields[path~sep~key].hashmap.require(valkey,[]);
                (*val) ~= this.recordMd5s.length - 1;
            }
        }
        
    }
    void addJsonArray(Asdf root, const(char)[] path){
        assert(path != "");
        foreach (value; root.byElement)
        {
            JSONValue valkey;
            if(value.kind == Asdf.Kind.object){
                addJsonObject(value, path);
                continue;
            }else if(value.kind == Asdf.Kind.array){
                addJsonArray(value, path);
                continue;
            }else if(value.kind == Asdf.Kind.null_){
                continue;
            }else{
                valkey = JSONValue(value);
            }
            auto p = path in fields;
            if(p){
                auto val = (*p).hashmap.require(valkey,[]);
                (*val) ~= this.recordMd5s.length - 1; 
            }else{
                fields[path] = InvertedIndex();
                auto val = fields[path].hashmap.require(valkey,[]);
                (*val) ~= this.recordMd5s.length - 1;
            }
        }
        
    }

    void fromFile(File f){
        // read const sequence
        auto buf = new ubyte[8];
        f.rawRead(buf);
        enforce((cast(string)buf) == "VQ_INDEX","File doesn't contain VQ_INDEX sequence");

        // read md5 array length
        buf = new ubyte[ulong.sizeof];
        f.rawRead(buf);
        this.recordMd5s.length = littleEndianToNative!(ulong, 8)(buf[0..8]);

        // read md5 array
        buf = new ubyte[ulong.sizeof + ulong.sizeof];
        foreach(ref md5; this.recordMd5s){
            f.rawRead(buf);
            md5.hi = littleEndianToNative!(ulong, 8)(buf[0..8]);
            md5.lo = littleEndianToNative!(ulong, 8)(buf[8..16]);
        }

        // read number of fields
        buf = new ubyte[ulong.sizeof];
        f.rawRead(buf);
        auto numfields = littleEndianToNative!(ulong, 8)(buf[0..8]);
        foreach (i; iota(numfields))
        {
            InvertedIndex idx;

            // read key length
            buf = new ubyte[ulong.sizeof];
            f.rawRead(buf);
            auto keyLen = littleEndianToNative!(ulong, 8)(buf[0..8]);

            // read key
            buf = new ubyte[keyLen];
            f.rawRead(buf);
            auto field = (cast(string) buf.idup);

            // read number of values
            buf = new ubyte[ulong.sizeof];
            f.rawRead(buf);
            auto numValues = littleEndianToNative!(ulong, 8)(buf[0..8]);
            foreach (j; iota(numValues))
            {
                JSONValue key;

                // read JSONValue type
                buf = new ubyte[1];
                f.rawRead(buf);
                key.type = buf[0].to!TYPES;
                final switch(key.type){
                    case TYPES.NULL:
                        debug assert(0); // Shouldn't be writing any nulls
                        else continue;
                    case TYPES.FLOAT: // read JSONValue float
                        buf = new ubyte[double.sizeof];
                        f.rawRead(buf);
                        key.val.f = littleEndianToNative!(double, 8)(buf[0..8]);
                        break;
                    case TYPES.STRING: // read JSONValue str
                        buf = new ubyte[ulong.sizeof];
                        f.rawRead(buf);
                        key.val.s.length = littleEndianToNative!(ulong, 8)(buf[0..8]);
                        buf = new ubyte[key.val.s.length];
                        f.rawRead(buf);
                        key.val.s = (cast(string) buf.idup);
                        break;
                    case TYPES.INT: // read JSONValue int
                        buf = new ubyte[long.sizeof];
                        f.rawRead(buf);
                        key.val.f = littleEndianToNative!(long, 8)(buf[0..8]);
                        break;
                    case TYPES.BOOL: // read JSONValue bool
                        buf = new ubyte[bool.sizeof];
                        f.rawRead(buf);
                        key.val.b = buf[0].to!bool;
                        break;
                }
                // read id values array length
                ulong[] values;
                buf = new ubyte[ulong.sizeof];
                f.rawRead(buf);
                values.length = littleEndianToNative!(ulong, 8)(buf[0..8]);

                // read id values array
                foreach(ref v; values){
                    f.rawRead(buf);
                    v = littleEndianToNative!(ulong, 8)(buf[0..8]);
                }

                // assign key and values in hashmap
                idx.hashmap[key] = values;
            }
            // assign InvertedIndex in hashmap
            fields[field] = idx;
        }
    }

    void writeToFile(File f){
        f.rawWrite("VQ_INDEX");
        f.rawWrite(this.recordMd5s.length.nativeToLittleEndian);
        this.recordMd5s.each!(x=> f.rawWrite(x.hi.nativeToLittleEndian ~ x.lo.nativeToLittleEndian));
        f.rawWrite(fields.length.nativeToLittleEndian);
        foreach (field,idx; fields)
        {
            f.rawWrite(field.length.nativeToLittleEndian);
            f.rawWrite(field);
            f.rawWrite(idx.hashmap.count.to!ulong.nativeToLittleEndian);
            import std.stdio;
            foreach (key; idx.hashmap.byKey)
            {
                auto value = idx.hashmap[key];
                assert(key.type != TYPES.NULL);
                f.rawWrite([cast(ubyte)key.type]);
                final switch(key.type){
                    case TYPES.NULL:
                        debug assert(0); // Shouldn't be reading any nulls
                        else continue;
                    case TYPES.FLOAT:
                        f.rawWrite(key.val.f.nativeToLittleEndian);
                        break;
                    case TYPES.STRING:
                        f.rawWrite(key.val.s.length.nativeToLittleEndian);
                        f.rawWrite(key.val.s);
                        break;
                    case TYPES.INT:
                        f.rawWrite(key.val.i.nativeToLittleEndian);
                        break;
                    case TYPES.BOOL:
                        f.rawWrite(key.val.b.nativeToLittleEndian);
                        break;
                }
                f.rawWrite(value.length.nativeToLittleEndian);
                value.each!(x=> f.rawWrite(x.nativeToLittleEndian));
            }
        }
    }

    ulong[] allIds(){
        return iota(0, this.recordMd5s.length).array;
        // return this.recordMd5s;
    }

    InvertedIndex*[] getFields(string key)
    {
        auto keycopy = key.idup;
        if(key[0] != '/') throw new Exception("key is missing leading /");
        InvertedIndex*[] ret;
        auto wildcard = key.indexOf('*');
        if(wildcard == -1){
            auto p = key in fields;
            if(!p) throw new Exception(" key "~key~" is not found");
            ret = [p];
            if(ret.length == 0){
                stderr.writeln("Warning: Key"~ keycopy ~" was not found in index!");
            }
        }else{
            key = key.replace("*",".*");
            auto reg = regex("^" ~ key ~"$");
            ret = fields.byKey.std_filter!(x => !(x.matchFirst(reg).empty)).map!(x=> &(fields[x])).array;
            if(ret.length == 0){
                stderr.writeln("Warning: Key wildcards sequence "~ keycopy ~" matched no keys in index!");
            }
        }
        
        return ret;
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
