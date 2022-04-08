module libmucor.jsonlops.range;

import std.range;
import std.algorithm : map, fold, joiner, find, uniq;
import std.traits;
import std.array : split;
import std.conv : to;
import std.functional : unaryFun, binaryFun;

import asdf;
import libmucor.jsonlops.basic;

struct GroupByObject{
    Asdf[] index;
    Asdf[] objs;
    string[] keys;

    Asdf toAsdf()
    {
        auto objNode = AsdfNode("{}".parseJson);
        objNode["index"] = AsdfNode("{}".parseJson);
        foreach (i, key; keys)
        {
            objNode["index", key] = AsdfNode(index[i]);
        }
        objNode["rows"] = AsdfNode(objs.makeAsdfArray);
        return cast(Asdf)objNode;
    }
}

auto groupby(Range)(Range range, string[] keys)
if(is(ElementType!Range == Asdf))
{
    
    GroupByObject[ubyte[]] hashmap;
    foreach (obj; range)
    {
        obj = obj.copy;
        ubyte[] hashKey;
        Asdf[] idx;
        foreach (key; keys)
        {
            auto asdfKeys = key.split("/");
            auto prevLen = hashKey.length;
            if(obj[asdfKeys] != Asdf.init){
                hashKey ~= obj[asdfKeys].data;
            }else{
                hashKey ~= `null`.parseJson.data;
            }
            idx ~= Asdf(hashKey[prevLen .. $]);
        }
        auto grpPtr = hashKey in hashmap;
        if(grpPtr){
            grpPtr.objs ~= obj;
        }else{
            hashmap[hashKey.idup] = GroupByObject(idx, [obj], keys);
        }
    }

    return hashmap.byValue;
}

auto groups(Range)(Range range)
if(is(ElementType!Range == GroupByObject))
{
    return range.map!"a.toAsdf";
}


unittest
{
    import std.stdio;
    import std.conv : to;
    auto text = `{"foo":"bar","inner":{"a":true,"b":false,"c":"32323","d":null,"e":{}}}`;
    auto textJson =  text.parseJson;
    auto textJson2 =  text.parseJson;
    auto range = [textJson,textJson2];
    auto res = makeAsdfArray(range.groupby(["foo","inner/c"]).groups.array).to!string;
    auto exp = `[{"index":{"inner/c":"32323","foo":"bar"},"rows":[{"foo":"bar","inner":{"a":true,"b":false,"c":"32323","d":null,"e":{}}},{"foo":"bar","inner":{"a":true,"b":false,"c":"32323","d":null,"e":{}}}]}]`;

    assert(res == exp);
}

unittest
{
    import std.stdio;
    import std.conv : to;
    auto textJson = `{"foo":"bar","inner":{"a":true,"b":false,"c":"32323","d":null,"e":{}}}`.parseJson;
    auto textJson2 =  `{"foo":"bar","inner":{"a":true,"b":false,"c":"32","d":null,"e":{}}}`.parseJson;
    auto range = [textJson,textJson2];
    auto res = makeAsdfArray(range.groupby(["foo","inner/c"]).groups.array).to!string;
    auto exp = `[{"index":{"inner/c":"32323","foo":"bar"},"rows":[{"foo":"bar","inner":{"a":true,"b":false,"c":"32323","d":null,"e":{}}}]},{"index":{"inner/c":"32","foo":"bar"},"rows":[{"foo":"bar","inner":{"a":true,"b":false,"c":"32","d":null,"e":{}}}]}]`;
    assert(res == exp);
}

/// apply function to any asdf range
template apply(fun)
{
    auto apply(Range)(Range range)
    if(isInputRange!Range && is(ElementType!Range == Asdf))
    {
        auto f = unaryFun(fun);
        return range.map!f;
    }
    auto apply(Range)(Range range)
    if(isInputRange!Range && is(ElementType!Range == GroupByObject))
    {
        auto f = unaryFun(fun);
        return range.map!(x=>x["objs"].map!f).joiner;
    }
}

/// apply reducing function (folding function) to any groupby range 
template aggregate(fun...)
{
    auto aggregate(Range)(Range range)
    if(is(ElementType!Range == GroupByObject))
    {
        alias f = binaryFun!(fun);
        return range.map!(x=>x.objs.fold!f);
    }
}


unittest
{
    import std.stdio;
    import std.conv : to;
    auto textJson = `{"foo":"bar","inner":{"a":true,"b":false,"c":"32323","d":null,"e":{}}}`.parseJson;
    auto textJson2 =  `{"foo":"bar","inner":{"a":true,"b":false,"c":"32","d":null,"e":{}}}`.parseJson;
    auto textJson3 =  `{"foo":"bar","inner":{"a":true,"b":false,"c":"32","d":false,"e":{}}}`.parseJson;
    auto range = [textJson,textJson2,textJson3];
    auto res = makeAsdfArray(range.groupby(["foo","inner/c"]).aggregate!merge.array).to!string;
    auto exp = `[{"foo":"bar","inner":{"a":true,"b":false,"c":"32323","d":null,"e":{}}},{"foo":["bar","bar"],"inner":{"c":["32","32"],"a":[true,true],"e":{},"b":[false,false],"d":[null,false]}}]`;
    assert(res == exp);
}

/// apply reducing function (folding function) to any groupby range 
template pivot(fun...)
{
    static if(fun[0] == "self"){
        auto pivot(Range)(Range range, string on, string val, string[] extraCols=[])
        if(is(ElementType!Range == GroupByObject))
        {
            return range.map!((x) {
                return x.objs.map!((y) {
                    auto ret = AsdfNode(`{}`.parseJson);
                    foreach (i,idx; x.index)
                    {
                        ret[x.keys[i]] = AsdfNode(idx);
                    }
                    assert(y[on] != Asdf.init, "pivot on value must be in groupby index");
                    auto onFields = on.split("/");
                    auto onVal = y[on];
                    string onStr;
                    foreach (col; extraCols)
                    {
                        auto colFields = col.split("/");
                        if(y[colFields] != Asdf.init)
                            ret[col] = AsdfNode(y[colFields]);
                    }
                    switch(onVal.kind){
                        case Asdf.Kind.array:
                        case Asdf.Kind.object:
                            onStr = onVal.to!string;
                            break;
                        default:
                            onStr = deserialize!string(onVal);
                    }
                    auto valFields = val.split("/");
                    if(y[valFields] != Asdf.init)
                        ret[onStr] = AsdfNode(y[valFields]);
                    return cast(Asdf) ret;
                }).fold!merge.unique;
           });
        }
    }else{

    }
    auto aggregate(Range)(Range range)
    if(is(ElementType!Range == GroupByObject))
    {
        alias f = binaryFun!(fun);
        return range.map!(x=>x.objs.fold!f);
    }
}


unittest
{
    import std.stdio;
    import std.conv : to;
    auto textJson = `{"foo":"bar","inner":{"a":true,"b":false,"c":"32323","d":null,"e":{}}}`.parseJson;
    auto textJson2 =  `{"foo":"bar","inner":{"a":true,"b":false,"c":"32","d":null,"e":{}}}`.parseJson;
    auto textJson3 =  `{"foo":"bar","inner":{"a":true,"b":false,"c":"32","d":false,"e":{}}}`.parseJson;
    auto range = [textJson,textJson2,textJson3];
    auto res = makeAsdfArray(range.groupby(["foo","inner/c"]).aggregate!merge.array).to!string;
    auto exp = `[{"foo":"bar","inner":{"a":true,"b":false,"c":"32323","d":null,"e":{}}},{"foo":["bar","bar"],"inner":{"c":["32","32"],"a":[true,true],"e":{},"b":[false,false],"d":[null,false]}}]`;
    assert(res == exp);
}


auto expandBySample(R)(R objs, bool active) 
if (is(ElementType!R == Asdf))
{
    return objs.map!((x) {
        if(!active)
            return [x];
        if(x["FORMAT"] == Asdf.init)
            return [x];
        auto samples = x["FORMAT"].byKeyValue;
        return samples.map!((y) {
            auto root = AsdfNode(Asdf(x.data.dup));
            root["sample"] = AsdfNode(y.key.serializeToAsdf);
            root["FORMAT"] = AsdfNode(y.value);
            return cast(Asdf)root;
        }).array;
    }).joiner;
}

auto expandMultiAllelicSites(R)(R objs, bool active) 
if (is(ElementType!R == Asdf))
{
    return objs.map!((x) {
        if(!active)
            return [x];
        if(x["FORMAT"] == Asdf.init)
            return [x];
        if(x["INFO"]["by_allele"] == Asdf.init)
            return [x];
        // if expanded by sample already
        if(x["sample"] != Asdf.init) {
            auto allele_vals = x["FORMAT"]["by_allele"].byElement;
            auto info_vals = x["INFO"]["by_allele"].byElement.array;
            return allele_vals.enumerate.map!((y) {
                auto root = Asdf(x.data.dup);
                root["FORMAT"]["by_allele"].remove();
                root["INFO"]["by_allele"].remove();
                auto rootNode = AsdfNode(root);
                foreach (obj; y.value.byKeyValue)
                {
                    rootNode["FORMAT"][obj.key] = AsdfNode(obj.value);
                }
                foreach (obj; info_vals[y.index].byKeyValue)
                {
                    rootNode["INFO"][obj.key] = AsdfNode(obj.value);
                }
                if (info_vals.length > 1)
                    rootNode["ALT"] = AsdfNode(root["ALT"].byElement.array[y.index]);
                return cast(Asdf) rootNode;
            }).array;
        }else {
            auto info_vals = x["INFO"]["by_allele"].byElement.array;
            return info_vals.enumerate.map!((y) {
                auto root = Asdf(x.data.dup);
                root["INFO"]["by_allele"].remove();
                foreach(sample;root["FORMAT"].byKeyValue){
                    root["FORMAT"][sample.key]["by_allele"].remove;
                    auto rootNode = AsdfNode(root);
                    foreach (obj; x["FORMAT"][sample.key]["by_allele"].byKeyValue)
                    {
                        rootNode["FORMAT"][sample.key][obj.key] = AsdfNode(obj.value.byElement.array[y.index]);
                    }
                    root = cast(Asdf) rootNode;
                }
                auto rootNode = AsdfNode(root);
                foreach (obj; info_vals[y.index].byKeyValue)
                {
                    rootNode["INFO"][obj.key] = AsdfNode(obj.value);
                }
                if (info_vals.length > 1)
                    rootNode["ALT"] = AsdfNode(root["ALT"].byElement.array[y.index]);
                return cast(Asdf) rootNode;
            }).array;
        }
    }).joiner;
}

auto dropNullGenotypes(R)(R objs, bool active) 
if (is(ElementType!R == Asdf))
{
    return objs.map!((x) {
        if(active) {
            if (x["FORMAT"] != Asdf.init) {
                foreach(sample;x["FORMAT"].byKeyValue) {
                    if (sample.value["GT"].deserialize!string[0] =='.')
                        sample.value.remove();
                }
            }
            return x;
        }
        else
            return x;
    });
}

auto subset(Range)(Range range, string[] keys)
if(is(ElementType!Range == Asdf))
{
    string[] byVarKeys = ["CHROM", "POS", "REF", "ALT"];
    string[] bySamVarKeys = ["sample", "CHROM", "POS", "REF", "ALT"];
    return range.map!(x => subset(x, keys))
        .uniq;
}


Asdf subset(Asdf obj, string[] keys)
{
    auto node =  AsdfNode("{}".parseJson);
    foreach (string key; keys)
        node[key] = AsdfNode(obj[key]);
    return cast(Asdf)node;
}

auto to_table(R)(R json_stream, string[] fields, string delimiter="\t",string fill="."){
    struct Result{
        R json_stream;

        @property bool empty(){
            return json_stream.empty;
        }

        string front(){
            string[] to_write;
            auto val = json_stream.front;
            foreach(key; fields){
                if(val[key] != Asdf.init)
                    to_write ~= to!(string)(val[key]);
                else
                    to_write ~= fill;
            }
            return to_write.join(delimiter);
        }

        void popFront(){
            json_stream.popFront;
        }
    }
    //auto json_stream=stdin.byChunk(4096).parseJsonByLine;
    return Result(json_stream);
}

