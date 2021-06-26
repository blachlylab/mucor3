module wrangler.table;

import std.stdio;
import asdf;
import std.conv:to;
import std.range;
import std.traits:ReturnType;
import std.algorithm.iteration:each;
import std.range.interfaces:ForwardRange;

void run(string[] args){
    string delimiter="\t";
    string fill=".";
    for(auto i=0;i<args.length;i++){
        if(args[i]=="-d"){
            delimiter=args[i+1];
        }
        if(args[i]=="-f"){
            delimiter=args[i+1];
        }
    }
    Asdf[] lines;
    stdin.byChunk(4096).parseJsonByLine.each!(x=>lines~=Asdf(x.data.dup));
    writeln(lines.get_keys.get_header(delimiter));
    lines.to_table(lines.get_keys,delimiter,fill).each!(x=>writeln(x));
}
bool[string] get_keys(Asdf[] json_stream){
    bool[string] fields;
    foreach (obj;json_stream){
        foreach(kv;obj.byKeyValue){
            fields[kv[0].idup]=true;
        }
    }
    return fields;
}

string get_header(bool[string] fields,string delimiter="\t"){
    string to_write;
    foreach(key;fields.keys){
        to_write~=key;
        to_write~=delimiter;
    }
    return to_write;
}

auto to_table(Asdf[] json_stream,bool[string] fields, string delimiter="\t",string fill="."){
    struct Result{
        @property bool empty(){
            return json_stream.empty;
        }
        string front(){
            string to_write;
            auto val=json_stream.front;
            foreach(key;fields.keys){
                if(val[key]!=Asdf.init){
                    to_write~=to!(string)(val[key]);
                }else{
                    to_write~=fill;
                }
                to_write~=delimiter;
            }
            return to_write;
        }
        void popFront(){
            json_stream.popFront;
        }
    }
    //auto json_stream=stdin.byChunk(4096).parseJsonByLine;
    return Result();
}


