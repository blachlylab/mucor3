module mucor3.view;

import libmucor.serde;

import std.stdio;
import core.stdc.stdlib : exit;

import std.getopt;

bool json;

void view_main(string[] args)
{
    auto res = getopt(args, config.bundling,
        "json|j", "output json rather than ion text", &json);
    File input;
    if (res.helpWanted)
    {
        defaultGetoptPrinter("",res.options);
        exit(0);
    } else if(args.length > 2) {
        stderr.writeln("view usage: mucor3 view [options] <ion file in>");
        exit(1);
    }
    
    if(args.length == 1)
        input = stdin;
    else if(args.length == 2)
        input = File(args[1]);

    auto rdr = VcfIonDeserializer(input);
        
    foreach (rec; rdr)
    {
        auto r = rec.unwrap;
        if(json) {
            writeln(vcfIonToJson(r));
        } else {
            writeln(vcfIonToText(r));
        }
    }
}