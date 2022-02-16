import std.stdio;
import std.getopt;
import std.traits;
import jsonlops.range;
import asdf: parseJsonByLine;

string[] byVarKeys = ["CHROM", "POS", "REF", "ALT"];
string[] bySamVarKeys = ["sample", "CHROM", "POS", "REF", "ALT"];

string output_prefix = "./";
void main(string[] args)
{
	auto res = getopt(args,
		"p|prefix",&output_prefix);
	assert(args.length == 3);
	
	auto a = args[1];
	auto b = args[2];

	auto aVar = File(output_prefix ~ "a.variants.tsv");
	auto bVar = File(output_prefix ~ "b.variants.tsv");

	auto aSamVar = File(output_prefix ~ "a.sample.variants.tsv");
	auto bSamVar = File(output_prefix ~ "b.sample.variants.tsv");

	File(a).byChunk(4096).parseJsonByLine.getUniqueVariants;
	File(b).byChunk(4096).parseJsonByLine.getUniqueVariants;
	
		

}

auto getUniqueVariants(Range)(Range range)
if(is(ElementType!Range == Asdf))
{
    return range.subset(byVarKeys);
}

auto getUniqueSamVariants(Range)(Range range)
if(is(ElementType!Range == Asdf))
{
    return range.subset(bySamVarKeys);
}
