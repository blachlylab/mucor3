module mucor3.mucor.table;

import libmucor.khashl;
import asdf;
import std.stdio;
import std.path;
import htslib.hts_log;
import std.format : format;
import core.stdc.stdlib : exit;
import std.array : array, split;
import libmucor.jsonlops;
import std.algorithm : map, count;
import std.range : tee;
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

auto flattenAndMakeMaster(string fn, string[] index, string[] cols, string prefix)
{
    auto masterJson = buildPath(prefix, "master.json");
    auto masterTsv = buildPath(prefix, "master.tsv");
    auto outputJson = File(masterJson, "w");
    auto outputTsv = File(masterTsv, "w");
    auto range = File(fn).byChunk(4096).parseJsonByLine.map!(x => normalize(x,
            ['/'])).tee!(x => outputJson.writeln(x)).createTable(index, cols);
    foreach (line; range)
    {
        outputTsv.writeln(line);
    }
}

auto pivotAndMakeTable(string fn, string[] index, string on, string val,
        string[] extra, string[] samples, string prefix)
{
    import std.stdio;
    auto pivName = val.split("/")[$-1];
    auto pivotJson = buildPath(prefix, format("%s.json", pivName));
    auto pivotTsv = buildPath(prefix, format("%s.tsv", pivName));
    auto outputJson = File(pivotJson, "w");
    auto outputTsv = File(pivotTsv, "w");
    auto cols = index ~ extra ~ ["Positive results", "Positive rate"] ~ samples;
    auto range = File(fn).byChunk(4096).parseJsonByLine.groupby(index)
        .pivot!"self"(on, val, extra)
        .tee!(x=> outputJson.writeln(x))
        .apply!(x => calulatePositiveColumns(x, index ~ extra, samples))
        .createTable(index,  cols);

    foreach (line; range)
    {
        outputTsv.writeln(line);
    }
}


auto createTable(R)(R json_stream, string[] indexes, string[] fields, string delimiter = "\t", string fill = ".")
{
    import std.array : join;
    import std.conv : to;
    import std.algorithm : sort;
    import std.range;
    
    struct CreateTable
    {
        Asdf[] rows;
        bool first;
        string[] fields;

        this(Asdf[] rows, string[] fields) {
            this.rows = rows;
            first = true;
            this.fields = fields;
        }

        @property bool empty()
        {
            return rows.empty;
        }

        string front()
        {
            if(first) {
                first = false;
                return fields.join(delimiter);
            }
            string[] to_write;
            auto val = rows.front;
            foreach (key; fields)
            {
                if (val[key] != Asdf.init)
                    to_write ~= to!(string)(val[key]);
                else
                    to_write ~= fill;
            }
            return to_write.join(delimiter);
        }

        void popFront()
        {
            rows.popFront;
        }
    }
    
    Asdf[] rows = json_stream.array;
    rows.sort!((a, b) => cmp(a, b, indexes));

    return CreateTable(rows, fields);
}

bool cmp(Asdf a, Asdf b, string[] indexes)
{
    foreach (idx; indexes)
    {
        if (a[idx].data < b[idx].data)
            return true;
        else if (a[idx].data > b[idx].data)
            return false;
    }
    return false;
}