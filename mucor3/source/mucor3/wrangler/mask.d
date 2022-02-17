module wrangler.mask;

import std.stdio;
import std.array:split;
import asdf;

void run_mask_key(string[] args){
    if(args.length==0){
        writeln(
        "Join multiple documents together based on given index. Overlapping"~
        " fields in rows are combined into arrays."
        );
    }else{
        mask_keys(args[1],args[0]);
    }
}
void run_mask_value(string[] args){
    if(args.length==0){
        writeln(
        "Join multiple documents together based on given index. Overlapping"~
        " fields in rows are combined into arrays."
        );
    }else{
        mask_values(args[0],args[2],args[1]);
    }
}

void mask_keys(string mapping_file,string mapping){
    string[] fields=mapping.split("=");
    string[string] masker;
    foreach(obj;File(mapping_file).byChunk(4096).parseJsonByLine){
        string key;
        string value;
        if(obj[fields[0]]!=Asdf.init){
            key=cast(string)obj[fields[0]];
        }else{
            continue;
        }
        if(obj[fields[1]]!=Asdf.init){
            value=cast(string)obj[fields[1]];
        }else{
            continue;
        }
        masker[key]=value;
    }
    foreach(obj;stdin.byChunk(4096).parseJsonByLine){
        AsdfNode master=AsdfNode(obj);
        foreach(key;masker.keys){
            if(obj[key]!=Asdf.init){
                master[masker[key]]=AsdfNode(obj[key]);
                Asdf node=cast(Asdf)master;
                node[key].remove;
                master=AsdfNode(node);
            }
        }
        writeln(cast(Asdf)master);
    }
}

void mask_values(string file_key,string mapping_file,string mapping){
    string[] fields=mapping.split("=");
    ubyte[][ubyte[]] masker;
    foreach(obj;File(mapping_file).byChunk(4096).parseJsonByLine){
        ubyte[] key;
        ubyte[] value;
        if(obj[fields[0]]!=Asdf.init){
            key=obj[fields[0]].data;
        }else{
            continue;
        }
        if(obj[fields[1]]!=Asdf.init){
            value=obj[fields[0]].data.dup;
        }else{
            continue;
        }
        masker[key.idup]=value;
    }
    foreach(obj;stdin.byChunk(4096).parseJsonByLine){
        AsdfNode master=AsdfNode(obj);
        if(obj[file_key]!=Asdf.init){
            auto check=(obj[file_key].data in masker);
            if(check !is null){
                master[file_key]=AsdfNode(Asdf(masker[obj[file_key].data]));
            }
        }
        writeln(cast(Asdf)master);
    }
}