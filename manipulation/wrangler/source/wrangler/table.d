module wrangler.table;

import std.stdio;
import asdf;
import std.conv:to;
import std.range;
import std.getopt;
import std.array;
import std.traits:ReturnType;
import std.algorithm: sort, each;
import std.range.interfaces:ForwardRange;

void run(string[] args){
    string delimiter="\t";
    string fill=".";
    string[] indexes;
    string[] extras;
    arraySep = ",";
    auto res = getopt(args,
            "i|index",&indexes,
            "e|extras", &extras,
            "d|delimiter",&delimiter,
            "f|fill",&fill);
    Asdf[] lines;
    stdin.byChunk(4096).parseJsonByLine.each!(x=>lines~=Asdf(x.data.dup));
    auto fields = lines.get_keys(indexes ~ extras);

    writeln(fields.join(delimiter));
    lines.sort!((a, b) => cmp(a, b, indexes));
    lines.to_table(fields,delimiter,fill).each!(x=>writeln(x));
}

bool cmp(Asdf a, Asdf b, string[] indexes)
{
    foreach(idx; indexes)
    {
        if(a[idx].data < b[idx].data)
            return true;
        else if(a[idx].data > b[idx].data)
            return false;
    }
    return false;
}

string[] get_keys(Asdf[] json_stream, string[] indexes){
    bool[string] fields;
    foreach (obj;json_stream){
        foreach(kv;obj.byKeyValue){
            fields[kv[0].idup]=true;
        }
    }
    string[] ret;
    foreach(idx; indexes){
        if(!(idx in fields)){
            throw new Exception("Index "~ idx ~" not in values");
        }
        fields.remove(idx);
        ret ~= idx;
    }
    ret ~= fields.byKey.array;
    return ret;
}

string get_header(string[] fields,string delimiter="\t"){
    string to_write;
    foreach(key;fields){
        to_write~=key;
        to_write~=delimiter;
    }
    return to_write;
}

auto to_table(Asdf[] json_stream,string[] fields, string delimiter="\t",string fill="."){
    struct Result{
        @property bool empty(){
            return json_stream.empty;
        }
        string front(){
            string[] to_write;
            auto val=json_stream.front;
            foreach(key;fields){
                if(val[key]!=Asdf.init)
                    to_write~=to!(string)(val[key]);
                else
                    to_write~=fill;
            }
            return to_write.join(delimiter);
        }
        void popFront(){
            json_stream.popFront;
        }
    }
    //auto json_stream=stdin.byChunk(4096).parseJsonByLine;
    return Result();
}


