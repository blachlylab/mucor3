module jsonlops.basic;

import std.algorithm: map, sort, uniq, joiner, sum, fold, count;
import std.array : array;

import asdf;

pragma(inline, true)
Asdf copy(const(Asdf) val)
{
    return Asdf(val.data.dup);
}

pragma(inline, true)
/// append an asdf array with any asdf obj
Asdf appendAsdfArray(const(Asdf) arr, const(Asdf) value)
{
    // make sure its an array
    assert(arr.kind == Asdf.Kind.array);

    // copy arr and modify to new length
    auto newDataLen = value.data.length;
    auto newArr = arr.copy;
    auto lenPtr = cast(uint*)(newArr.data[1..5].ptr);
    *lenPtr += newDataLen;
    // append array
    newArr.data ~= value.data;
    return newArr;
}

unittest
{
    auto res = appendAsdfArray(`["32323","32"]`.parseJson,`"32323"`.parseJson);
    assert(res == `["32323","32","32323"]`.parseJson);
}

pragma(inline, true)
/// append an asdf array with any asdf obj
Asdf combineAsdfArray(const(Asdf) arr1, const(Asdf) arr2)
{
    // make sure its an array
    assert(arr1.kind == Asdf.Kind.array);
    assert(arr2.kind == Asdf.Kind.array);

    // create new array
    auto arr = `[]`.parseJson;
    // get new length
    auto arr1Len = cast(uint*)(arr1.data[1..5].ptr);
    auto arr2Len = cast(uint*)(arr2.data[1..5].ptr);
    auto lenPtr = cast(uint*)(arr.data[1..5].ptr);
    // set length
    *lenPtr += *arr1Len + *arr2Len;
    // append data
    arr.data ~= (arr1.data[5..$] ~ arr2.data[5..$]);
    return arr;
}

unittest
{
    auto res = combineAsdfArray(`["32323","32"]`.parseJson,`["32323"]`.parseJson);
    assert(res == `["32323","32","32323"]`.parseJson);
}

/// create a asdf array obj from two asdf objs
Asdf makeAsdfArray(Asdf value1, Asdf value2)
{
    return makeAsdfArray([value1, value2]);
}

unittest
{
    auto res = makeAsdfArray(`["32323","32"]`.parseJson,`["32323"]`.parseJson);
    assert(res == `[["32323","32"],["32323"]]`.parseJson);
}

pragma(inline, true)
/// create a asdf array obj from an array of asdf objs 
Asdf makeAsdfArray(Asdf[] vals)
{
    // create new array
    auto arr = `[]`.parseJson;
    // get new length
    auto newDataLen = vals.map!"a.data.length".sum;
    // set length
    auto lenPtr = cast(uint*)(arr.data[1..5].ptr);
    *lenPtr += newDataLen;
    // append data
    arr.data ~= vals.map!"a.data".joiner.array;
    return arr;
}

unittest
{
    auto res = makeAsdfArray([`"32323"`.parseJson,`"32"`.parseJson,`"32323"`.parseJson]);
    assert(res == `["32323","32","32323"]`.parseJson);
}

pragma(inline, true)
/// recursively normalize or flatten asdf objects
Asdf normalize(Asdf obj, char[] sep = ['.']){
    auto mut = "{}".parseJson;
    normalize(obj,[],mut, sep);
    return cast(Asdf) mut;
}

/// recursively normalize or flatten asdf objects
void normalize(Asdf value, char[] path, ref Asdf mut, char[] sep){
    switch(value.kind){
        case Asdf.Kind.array:
            foreach (val; value.byElement)
            {
                normalize(val,path, mut, sep);
            }
            break;
        case Asdf.Kind.object:
            if(path != [])
                path ~= sep;
            foreach (kv; value.byKeyValue)
            {
                normalize(kv[1],path~kv[0], mut, sep);
            }
            break;
        default:
            auto mutNode = AsdfNode(mut);
            if(mut[path] != Asdf.init){    
                assert(mut[path].kind != Asdf.Kind.object);
                if(mut[path].kind == Asdf.Kind.array){
                    mutNode[path] = AsdfNode(appendAsdfArray(mut[path], value));
                    mut = cast(Asdf)mutNode;
                }else{
                    mutNode[path] = AsdfNode(makeAsdfArray(mut[path], value));
                    mut = cast(Asdf)mutNode;
                }
            }else{
                mutNode[path] = AsdfNode(value);
                mut = cast(Asdf)mutNode;
            }
            break;
    }
}

unittest
{
    auto text = `{"foo":"bar","inner":{"a":true,"b":false,"c":"32323","d":null,"e":{}}}`;
    auto textJson =  text.parseJson;
    auto mut = normalize(textJson);
    auto exp = `{"foo":"bar","inner.c":"32323","inner.b":false,"inner.a":true,"inner.d":null}`.parseJson;
    assert(mut == exp);
}

unittest
{
    auto text = `{"foo":"bar","inner":[{"a":true,"b":false,"c":"32323","d":null,"e":{}},{"a":true,"b":true,"c":"32","d":false,"e":{}}]}`;
    auto textJson =  text.parseJson;
    auto mut = normalize(textJson);
    auto exp = `{"foo":"bar","inner.c":["32323","32"],"inner.b":[false,true],"inner.a":[true,true],"inner.d":[null,false]}`.parseJson;
    assert(mut == exp);
}

/// recursively sort and uniqify arrays in asdf obj
Asdf unique(Asdf value){
    auto valueNode = AsdfNode(value.copy);
    switch(value.kind){
        case Asdf.Kind.array:
            auto arr = value.byElement.map!"a.data".array.sort.uniq.map!(x=>Asdf(x)).array;
            if(arr.length == 1)
                return arr[0];
            return arr.makeAsdfArray;
        case Asdf.Kind.object:
            foreach (kv; value.byKeyValue)
            {
                valueNode[kv[0]] = AsdfNode(unique(kv[1]));
            }
            return cast(Asdf) valueNode;
        default:
            return value;
    }
}

unittest
{
    auto text = `{"foo":"bar","inner":[{"a":true,"b":false,"c":"32323","d":null,"e":{}},{"a":true,"b":true,"c":"32","d":false,"e":{}}]}`;
    auto textJson =  text.parseJson;
    auto mut = normalize(textJson);
    auto res = mut.unique;
    auto exp = `{"foo":"bar","inner.c":["32","32323"],"inner.b":[true,false],"inner.a":true,"inner.d":[null,false]}`.parseJson;
    assert(res == exp);
}

/// recursively merge two asdf objs
Asdf merge(Asdf value1, Asdf value2){ 
    auto type1 = value1.kind();
    auto type2 = value2.kind();
    // combine arrays
    if(type1 == type2 && type1 == Asdf.Kind.array){
        auto arr = combineAsdfArray(value1, value2);
        return mergeObjectsInArray(arr);
    // combine objects
    }else if(type1 == type2 && type1 == Asdf.Kind.object){
        auto node = AsdfNode(value1);
        foreach (kv; value1.byKeyValue)
        {
            if(value2[kv[0]] != Asdf.init){
                node[kv[0]] = AsdfNode(merge(value1[kv[0]],value2[kv[0]]));
            }else{
                node[kv[0]] = AsdfNode(kv[1]);
            }
        }
        foreach (kv; value2.byKeyValue)
        {
            if(value1[kv[0]] == Asdf.init){
                node[kv[0]] = AsdfNode(kv[1]);
            }
        }
        return cast(Asdf) node;
    // append objects
    }else if(type1 != type2 && type1 == Asdf.Kind.object){
        auto node = AsdfNode(value1);
        node["_overlap_value_"] = AsdfNode(value2);
        return cast(Asdf) node;
    // append objects
    }else if(type1 != type2 && type2 == Asdf.Kind.object){
        auto node = AsdfNode(value2);
        node["_overlap_value_"] = AsdfNode(value1);
        return cast(Asdf) node;
    // append array
    }else if(type1 != type2 && type1 == Asdf.Kind.array){
        return appendAsdfArray(value1, value2);
    // append array
    }else if(type1 != type2 && type2 == Asdf.Kind.array){
        return appendAsdfArray(value2, value1);
    // make a new array
    }else{
        return makeAsdfArray(value1, value2);
    }
}

Asdf mergeObjectsInArray(Asdf value1)
{
    assert(value1.kind == Asdf.Kind.array);
    Asdf[] objs;
    auto valCount = value1.byElement.count;
    foreach (val; value1.byElement)
    {
        if(val.kind == Asdf.Kind.object){
            objs ~= Asdf(val.data.dup);
            val.remove;
        }
    }
    if(objs.length > 0){
        auto merged = objs.fold!merge;
        if(valCount == objs.length)
        {
            return merged;
        }else{
            return makeAsdfArray([merged]~value1.byElement.array);
        }
    }else{
        return value1;
    }

}

unittest
{
    import std.stdio;
    auto text = `[{"a":true,"b":false,"c":"32323","d":null,"e":{}},{"a":true,"b":true,"c":"32","d":false,"e":{}}, 2, false]`;
    auto textJson =  text.parseJson;
    auto res = mergeObjectsInArray(textJson);
    auto exp = `[{"c":["32323","32"],"a":[true,true],"e":{},"b":[false,true],"d":[null,false]},2,false]`.parseJson;
    assert(res == exp);
}

unittest
{
    import std.stdio;
    auto text = `[{"a":true,"b":false,"c":"32323","d":null,"e":{}},{"a":true,"b":true,"c":"32","d":false,"e":{}}]`;
    auto textJson =  text.parseJson;
    auto res = mergeObjectsInArray(textJson);
    auto exp = `{"c":["32323","32"],"a":[true,true],"e":{},"b":[false,true],"d":[null,false]}`.parseJson;
    assert(res == exp);
}

unittest
{
    import std.stdio;
    auto text = `{"foo":"bar","inner":[{"a":true,"b":false,"c":"32323","d":null,"e":{}},{"a":true,"b":true,"c":"32","d":false,"e":{}}]}`;
    auto textJson =  text.parseJson;
    auto res = merge(textJson, textJson);
    auto exp = `{"foo":["bar","bar"],"inner":{"c":["32323","32","32323","32"],"a":[true,true,true,true],"e":{},"b":[false,true,false,true],"d":[null,false,null,false]}}`.parseJson;
    assert(res == exp);
}
