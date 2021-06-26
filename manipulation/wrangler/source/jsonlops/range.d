module jsonlops.range;

import std.range;
import std.algorithm : map, fold, joiner, find;
import std.traits;
import std.array : split;
import std.conv : to;
import std.functional : unaryFun, binaryFun;

import asdf;
import jsonlops.basic;

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
            return range.map!((x){
                auto ret = AsdfNode(`{}`.parseJson);
                assert(x.keys.find(on), "pivot on value must be in groupby index");

                // set onVal and add keys to return obj
                Asdf onVal;
                foreach (i,idx; x.index)
                {
                    if(x.keys[i] == on)
                        onVal = idx;
                    else
                        ret[x.keys[i]] = AsdfNode(idx);
                }
                // collect and add any extra columns
                foreach (col; extraCols)
                {
                    auto vals = x.objs.map!(y => y[col]).array;
                    if(vals.length == 1){
                        ret[col] = AsdfNode(vals[0]);
                    }else if(vals.length == 0){
                        ret[col] = AsdfNode(Asdf.init);
                    }else{
                        ret[col] = AsdfNode(makeAsdfArray(vals));
                    }
                }
                auto vals = x.objs.map!(y => y[val]).array;
                // convert onVal to a string
                string onStr;
                switch(onVal.kind){
                    case Asdf.Kind.array:
                    case Asdf.Kind.object:
                        onStr = onVal.to!string;
                        break;
                    default:
                        onStr = deserialize!string(onVal);
                }
                
                // assign vals to onStr in ret obj
                if(vals.length == 1){
                    ret[onStr] = AsdfNode(vals[0]);
                }else if(vals.length == 0){
                    ret[onStr] = AsdfNode(Asdf.init);
                }else{
                    ret[onStr] = AsdfNode(makeAsdfArray(vals));
                }
                return cast(Asdf) ret;
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
