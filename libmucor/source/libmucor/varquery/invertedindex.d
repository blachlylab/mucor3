module libmucor.varquery.invertedindex;
import std.algorithm.setops;
import std.regex;
import std.algorithm : sort, uniq, map, std_filter = filter, joiner, each;
import std.range : iota, takeExactly;
import std.array : array, replace;
import std.conv : to;
import std.format : format;
import std.string : indexOf;
import std.bitmanip: nativeToLittleEndian, littleEndianToNative;
import std.stdio;
import std.exception : enforce;

import asdf: deserializeAsdf = deserialize, Asdf, AsdfNode, parseJson, serializeToAsdf;
import libmucor.wideint : uint128;
import libmucor.varquery.singleindex;
import libmucor.khashl: khashl;

char sep = '/';

struct JSONInvertedIndex{
    uint128[] recordMd5s;
    khashl!(const(char)[], InvertedIndex) fields;
    this(string f){
        fromFile(f);
    }
    void addJsonObject(Asdf root, const(char)[] path = ""){
        if(path == ""){
            uint128 a;
            debug if(root["md5"] == Asdf.init) stderr.writeln("record with no md5");
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
                ulong[] arr = new ulong[0];
                auto val = (*p).hashmap.require(valkey,arr);
                (*val) ~= this.recordMd5s.length - 1;
            }else{
                ulong[] arr = new ulong[0];
                fields[path~sep~key] = InvertedIndex();
                InvertedIndex * hm= (path~sep~key) in fields;
                auto val = hm.hashmap.require(valkey,arr);
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
                ulong[] arr = new ulong[0];
                auto val = (*p).hashmap.require(valkey,arr);
                (*val) ~= this.recordMd5s.length - 1; 
            }else{
                ulong[] arr = new ulong[0];
                fields[path] = InvertedIndex();
                InvertedIndex * hm= path in fields;
                auto val = hm.hashmap.require(valkey,arr);
                (*val) ~= this.recordMd5s.length - 1;
            }
        }
        
    }

    void fromFile(string f){
        import std.file : read;
        auto bytesRange = cast(ubyte[])(f.read());
        ubyte[] buf;
        // read const sequence
        enforce((cast(string)bytesRange[0..8]) == "VQ_INDEX","File doesn't contain VQ_INDEX sequence");
        bytesRange = bytesRange[8..$];

        // read md5 array length
        this.recordMd5s.length = littleEndianToNative!(ulong, 8)(bytesRange[0..8]);
        bytesRange = bytesRange[8..$];

        // read md5 array
        buf = bytesRange[0 .. (ulong.sizeof + ulong.sizeof)*this.recordMd5s.length];
        bytesRange = bytesRange[(ulong.sizeof + ulong.sizeof)*this.recordMd5s.length..$];

        // convert array
        foreach(i,ref md5; this.recordMd5s){
            md5.hi = littleEndianToNative!(ulong, 8)(buf[(i*16)..(i*16)+8][0..8]);
            md5.lo = littleEndianToNative!(ulong, 8)(buf[(i*16)+8..(i*16)+16][0..8]);
        }

        // read number of fields
        auto numfields = littleEndianToNative!(ulong, 8)(bytesRange[0..8]);
        bytesRange = bytesRange[8..$];
        foreach (i; iota(numfields))
        {
            InvertedIndex idx;

            // read key length
            auto keyLen = littleEndianToNative!(ulong, 8)(bytesRange[0..8]);
            bytesRange = bytesRange[8..$];

            // read key
            buf = bytesRange[0..keyLen];
            auto field = (cast(string) buf.idup);
            bytesRange = bytesRange[keyLen..$];

            // read size of the values in bytes
            auto valuesSize = littleEndianToNative!(ulong, 8)(bytesRange[0..8]);
            bytesRange = bytesRange[8..$];

            // load values as bytes
            buf = bytesRange[0 .. valuesSize];
            while(buf!=[])
            {
                JSONValue key;

                key.type = buf[0].to!TYPES;
                buf = buf[1..$];

                final switch(key.type){
                    case TYPES.NULL:
                        debug assert(0); // Shouldn't be writing any nulls
                        else continue;
                    case TYPES.FLOAT: // read JSONValue float
                        key.val.f = littleEndianToNative!(double, 8)(buf[0..8]);
                        buf = buf[8..$];
                        break;
                    case TYPES.STRING: // read JSONValue str
                        key.val.s.length = littleEndianToNative!(ulong, 8)(buf[0..8]);
                        buf = buf[8..$];
                        key.val.s = (cast(string) buf[0..key.val.s.length].idup);
                        buf = buf[key.val.s.length..$];
                        break;
                    case TYPES.INT: // read JSONValue int
                        key.val.i = littleEndianToNative!(long, 8)(buf[0..8]);
                        buf = buf[8..$];
                        break;
                    case TYPES.BOOL: // read JSONValue bool
                        key.val.b = buf[0].to!bool;
                        buf = buf[1..$];
                        break;
                }
                // read id values array length
                ulong[] values;
                values.length = littleEndianToNative!(ulong, 8)(buf[0..8]);
                buf = buf[8..$];

                // read id values array
                foreach(ref v; values){
                    v = littleEndianToNative!(ulong, 8)(buf[0..8]);
                    buf = buf[8..$];
                }

                // assign key and values in hashmap
                idx.hashmap[key] = values;
            }
            // assign InvertedIndex in hashmap
            fields[field] = idx;
            bytesRange = bytesRange[valuesSize..$];
        }
    }

    void writeToFile(File f){

        // write constant
        f.rawWrite("VQ_INDEX");

        // write md5 length
        f.rawWrite(this.recordMd5s.length.nativeToLittleEndian);
        // write md5 array
        f.rawWrite(this.recordMd5s.map!(x=> x.hi.nativeToLittleEndian ~ x.lo.nativeToLittleEndian).joiner.array);
        // write num fields
        f.rawWrite(fields.byKey.array.length.nativeToLittleEndian);
        foreach (kv; fields.byKeyValue)
        {
            auto field = kv.key;
            auto idx = kv.value;
            // write field length
            f.rawWrite(field.length.nativeToLittleEndian);
            // write field string
            f.rawWrite(field);

            // generate values bytes
            import std.stdio;
            ubyte[] towrite = [];
            foreach (key; idx.hashmap.byKey)
            {
                auto value = idx.hashmap[key];
                towrite ~= key.toBytes;
                towrite ~= value.length.nativeToLittleEndian;
                towrite ~= value.map!(x=> x.nativeToLittleEndian.dup).joiner.array;
            }
            // write values byte size
            f.rawWrite(towrite.length.nativeToLittleEndian);
            // write values bytes
            f.rawWrite(towrite);
        }
    }

    ulong[] allIds(){
        return iota(0, this.recordMd5s.length).array;
        // return this.recordMd5s;
    }

    const(InvertedIndex)*[] getFields(string key)
    {
        auto keycopy = key.idup;
        if(key[0] != '/') throw new Exception("key is missing leading /");
        const(InvertedIndex)*[] ret;
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
            ret = fields.byKey.std_filter!(x => !(x.matchFirst(reg).empty)).map!(x=> &fields[x]).array;
            if(ret.length == 0){
                stderr.writeln("Warning: Key wildcards sequence "~ keycopy ~" matched no keys in index!");
            }
        }
        debug stderr.writefln("Key %s matched %d keys",keycopy,ret.length);
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
    template queryOp(string op)
    {
        ulong[] queryOp(T)(string key,T val){
            auto matchingFields = getFields(key);
            return matchingFields
                    .map!(x=> (*x).filterOp!op(val))
                    .joiner.array.sort.uniq.array;
        }
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

    auto opBinaryRight(string op)(JSONInvertedIndex lhs)
    {
        static if(op == "+") {
            JSONInvertedIndex ret;
            ret.recordMd5s = this.recordMd5s.dup;
            ret.fields = this.fields.dup;
            foreach(kv; lhs.fields.byKeyValue()) {
                auto v = kv.key in ret.fields;
                if(v) {
                    foreach(kv2; kv.value.hashmap.byKeyValue){
                        auto v2 = kv2.key in v.hashmap;
                        if(v2) {
                            *v2 = *v2 ~ kv2.value;
                        } else {
                            v.hashmap[kv2.key] = kv2.value;
                        }
                    }
                } else {
                    ret.fields[kv.key] = kv.value;
                }
            }
            return ret;
        } else
            static assert(false, "Op not implemented");
    }
}

// unittest{
//     import asdf:Asdf,AsdfNode,parseJson;
//     import libmucor.varquery.fields;
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
