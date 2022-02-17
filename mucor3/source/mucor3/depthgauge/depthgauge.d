module mucor3.depthgauge.depthgauge;
import std.stdio;
import std.algorithm.searching:until;
import std.algorithm.sorting:sort;
import std.algorithm.iteration:uniq,filter;
import std.algorithm:map,count;
import std.range:array;
import std.path:buildPath;
import std.conv:to;
import std.parallelism:parallel,defaultPoolThreads;
import std.getopt;
import dhtslib.sam:SAMFile;
import dhtslib.coordinates;

int threads=0;

import mucor3.depthgauge.csv;
void depthgauge(string[] args)
{
	auto res=getopt(args,config.bundling,
	"threads|t","threads for running depth gauge",&threads);
	if (res.helpWanted) {
		defaultGetoptPrinter("usage: ./depthgauge [input tsv] [number of first sample column] [bam folder] [output tsv]", res.options);
		stderr.writeln();
		return;
	}
	if(args.length!=5){
		writeln("usage: ./depthgauge [input tsv] [number of first sample column] [bam folder] [output tsv]");
		return;
	}else{
		if(threads!=0){
			defaultPoolThreads(threads);
		}
		auto t =Table(args[1],args[2].to!int-1);
		SAMFile s=SAMFile(buildPath(args[3],t.samples[0]~".bam"),0);
		t.parseRecords(&s,args[2].to!int-1);
		getDepths(t,args[3]);
		File f = File(args[4],"w");
		t.write(f);
		f.close;
	}

}

auto depth_at_pos(ref SAMFile bam,int chr,uint pos){
	return bam[chr, OB(pos)].count;
}

void getDepths(ref Table t,string prefix){
	foreach(j,sample;parallel(t.samples)){
		auto bam = SAMFile(buildPath(prefix,sample~".bam"),0);
		foreach(i,rec;t.records){
			t.matrix[i][j]=depth_at_pos(bam,rec.chr,rec.pos);
		}
		writeln(sample);
	}
}




