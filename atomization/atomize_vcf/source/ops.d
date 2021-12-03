module ops;

import asdf;
import std.range : ElementType;
import std.algorithm : joiner, map;
import std.array : array;

auto expandBySample(R)(R objs) 
if (is(ElementType!R == Asdf))
{
    return objs.map!((x) {
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

auto expandMultiAllelicSites(R)(R objs) 
if (is(ElementType!R == Asdf))
{
    return objs.map!((x) {
        if(x["FORMAT"] == asdf.init)
            return [root];
        if(x["FORMAT"]["by_allele"] == asdf.init)
            return [root];
        if(x["sample"] != asdf.init) {
            auto allele_vals = x["FORMAT"]["by_allele"].byElement;
            allele_vals.enumerate.map!((i,y) {
                auto root = Asdf(x.data.dup);
                root["FORMAT"]["by_allele"].remove();
                auto rootNode = AsdfNode(root);
                foreach (obj; y.byKeyValue)
                {
                    rootNode["FORMAT"][obj.key] = AsdfNode(obj.value.byElement.array[i]);
                }
                rootNode["ALT"] = AsdfNode(root["ALT"].byElement.array[i]);
                return cast(Asdf) root;
            }).array;
        }else {
            auto allele_vals = x["ALT"].byElement;
            allele_vals.enumerate.map!((i,y) {
                auto root = Asdf(x.data.dup);
                foreach(sample;root["FORMAT"].byKeyValue){
                    root["FORMAT"][sample.key]["by_allele"].remove;
                    auto rootNode = AsdfNode(root);
                    foreach (obj; x["FORMAT"][sample.key]["by_allele"].byKeyValue)
                    {
                        rootNode["FORMAT"][sample.key][obj.key] = AsdfNode(obj.value.byElement.array[i]);
                    }
                    root = cast(Asdf) rootNode;
                }
                rootNode["ALT"] = AsdfNode(root["ALT"].byElement.array[i]);
                return cast(Asdf) root;
            }).array;
        }
        return root;
        auto samples = x["FORMAT"].byKeyValue;
        return samples.map!((y) {
            auto root = AsdfNode(Asdf(x.data.dup));
            root["sample"] = y.key.serializeToAsdf;
            root["FORMAT"] = y.value;
        })
    }).joiner;
}