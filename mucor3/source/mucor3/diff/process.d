module mucor3.diff.process;

import std.stdio;
import std.algorithm : map, joiner, each;
import std.sumtype;
import std.math: std_round = round, pow, isClose;
import std.range: tee;
import std.file: mkdirRecurse, exists;
import std.conv: to;


import dhtslib.vcf;
import dhtslib.coordinates;
import asdf : deserializeAsdf = deserialize, Asdf, AsdfNode, serializeToAsdf;
import libmucor.vcfops;
import libmucor.jsonlops;
import libmucor.varquery;

import mucor3.diff.stats;
import mucor3.diff.util;
import mucor3.diff: varKeys, samVarKeys, Set;

InvertedIndex*[2] processVcfRaw(R)(R aRange, R bRange, string prefix){

    Set aVar, bVar, aSamVar, bSamVar, aAFVar, bAFVar;

    VarStats stats;

    mkdirRecurse(prefix);

    auto aname = prefix ~"a";
    auto bname = prefix ~"b";
    aRange
        .tee!(x => stats.aCount++)
        .tee!(x => aVar.insert(subset(x, varKeys)))
        .tee!(x => aSamVar.insert(subset(x, samVarKeys)))
        .tee!(x => aAFVar.insert(getAFValue(x, 0.01)))
        .index(aname);
    bRange
        .tee!(x => stats.bCount++)
        .tee!(x => bVar.insert(subset(x, varKeys)))
        .tee!(x => bSamVar.insert(subset(x, samVarKeys)))
        .tee!(x => bAFVar.insert(getAFValue(x, 0.01)))
        .index(bname);
    
    InvertedIndex * aIdx = new InvertedIndex(aname, false);
    InvertedIndex * bIdx = new InvertedIndex(bname, false);
    
    writeAllSets(aVar, bVar, varKeys, stats.varCounts[], prefix, "unique.var");
    writeAllSets(aSamVar, bSamVar, samVarKeys, stats.samVarCounts[], prefix, "unique.sample.var");
    writeAllSets(aAFVar, bAFVar, samVarKeys ~ ["AF"], stats.afVarCounts[], prefix, "unique.AF.sample.var");

    stderr.writeln("Raw stats");
    stderr.writeln(stats.toString);
    InvertedIndex*[2] ret; 
    ret[0] = aIdx;
    ret[1] = bIdx;
    return ret;
}

void processVcfFiltered(R)(R aRange, R bRange, string queryStr, InvertedIndex*[2] indexes, string prefix){
    Set aVar, bVar, aSamVar, bSamVar, aAFVar, bAFVar;

    VarStats stats;

    mkdirRecurse(prefix);

    aRange.query(indexes[0], queryStr)
        .tee!(x => stats.aCount++)
        .tee!(x => aVar.insert(subset(x, varKeys)))
        .tee!(x => aSamVar.insert(subset(x, samVarKeys)))
        .each!(x => aAFVar.insert(getAFValue(x, 0.01)));

    bRange.query(indexes[1], queryStr)
        .tee!(x => stats.bCount++)
        .tee!(x => bVar.insert(subset(x, varKeys)))
        .tee!(x => bSamVar.insert(subset(x, samVarKeys)))
        .each!(x => bAFVar.insert(getAFValue(x, 0.01)));
    
    writeAllSets(aVar, bVar, varKeys, stats.varCounts[], prefix, "unique.var");
    writeAllSets(aSamVar, bSamVar, samVarKeys, stats.samVarCounts[], prefix, "unique.sample.var");
    writeAllSets(aAFVar, bAFVar, samVarKeys ~ ["AF"], stats.afVarCounts[], prefix, "unique.AF.sample.var");
    
    stderr.writeln("Filtered stats");
    stderr.writeln(stats.toString);
}

void writeSet(Set s, string[] fields, ulong * count, string fn)
{
    auto f = File(fn, "w");
    s.byKey
        .tee!(x => (*count)++)
        .map!(x => *cast(Asdf*)&x)
        .to_table(fields)
        .each!(x => f.writeln(x));
}

void writeAllSets(Set a, Set b, string[] keys, ulong[] counts, string prefix, string name)
{
    a.writeSet(keys, &counts[0], prefix ~ "a." ~ name ~ ".tsv");
    b.writeSet(keys, &counts[1], prefix ~ "b." ~ name ~ ".tsv");
    (a & b).writeSet(keys, &counts[2], prefix ~ "conserved." ~ name ~ ".tsv");
    (a - b).writeSet(keys, &counts[3], prefix ~ "lost." ~ name ~ ".tsv");
    (b - a).writeSet(keys, &counts[4], prefix ~ "gained." ~ name ~ ".tsv");
}
