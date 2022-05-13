module mucor3.mucor.table;

import libmucor.khashl;
import asdf;
import std.stdio;
import htslib.hts_log;
import std.format : format;
import core.stdc.stdlib : exit;
import std.array : array, split;
import libmucor.jsonlops;
import std.algorithm : map, count;
import std.typecons : tuple, Tuple;
import libmucor.error;
import std.array : split;
import libmucor.invertedindex : sep;

// auto calulateTotalDepth(Asdf val) {
//     auto lenIdx = 0;
//     foreach(c; ["FORMAT/DP", "FORMAT/AD"]){
//         if(val[c.split("/")] == Asdf.init){
//             hts_log_warning(__FUNCTION__, format("Value %s not present in data", c));
//             return val;
//         }
//     }
//     auto ad = val["FORMAT","AD"].deserialize!long;
//     auto td = val["FORMAT","DP"].deserialize!long;
//     auto node = AsdfNode(val);
//     node["TotalDepth"] = AsdfNode((float(ad) / float(td)).serializeToAsdf);
//     return cast(Asdf)node;
// }

auto calulatePositiveColumns(Asdf val, string[] indexCols, string[] samples)
{
    auto lenIdx = 0;
    foreach (c; indexCols)
    {
        if (val[c] != Asdf.init)
            lenIdx++;
    }
    auto totalLen = val.byKeyValue.count;

    auto node = AsdfNode(val);
    node["Positive results"] = AsdfNode((totalLen - lenIdx).serializeToAsdf);
    node["Positive rate"] = AsdfNode((float(totalLen - lenIdx) / float(samples.length))
            .serializeToAsdf);
    return cast(Asdf) node;
}

auto flattenAndMakeMaster(string fn, string[] index, string[] extra, string outfile)
{
    auto output = File(outfile, "w");
    auto range = File(fn).byChunk(4096).parseJsonByLine.map!(x => normalize(x,
            ['/'])).to_table(index);
    foreach (line; range)
    {
        output.writeln(line);
    }
}

auto pivotAndMakeTable(string fn, string[] index, string on, string val,
        string[] extra, string[] samples, string outfile)
{
    auto output = File(outfile, "w");
    auto intermediateIndex = index ~ extra;
    auto totalIndex = intermediateIndex ~ ["Positive results", "Positive rate"];
    auto range = File(fn).byChunk(4096).parseJsonByLine.groupby(index)
        .pivot!"self"(on, val, extra).apply!(x => calulatePositiveColumns(x,
                intermediateIndex, samples)).to_table(totalIndex);

    foreach (line; range)
    {
        output.writeln(line);
    }
}
