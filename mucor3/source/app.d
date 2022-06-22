import std.stdio;
import mucor3;
import htslib.hts_log;
import libmucor;
import std.getopt;
import libmucor.error;

string help = "
depthgauge		Takes in an AF.tsv table, a directory path to a
			folder that contains bam files, and the column 
			number of the first sample. Replaces AF matrix values
			with bam file depths. Matches bam file to sample names
			in header of AF.tsv.

atomize			Transform VCF/BCF into intermediate representation used
			by mucor3, line-delimited JSON.

index			Creates inverted index of input JSON variant data. Used
			by query for querying/filtering of JSON data.

query			Takes JSONL data, an index (created with index), and a 
			string query to filter JSON data. Please see documentation 
			to understand query string semantics.

merge			Merge two json streams together while matching on input
			fields.

table			Transform JSON data into CSV/TSV format.

pivot			Create pivoted table data in JSON format.
			Can be used to generate AF.tsv tables.

norm			Flatten JSON data, removing nested JSON objects.

uniq			Uniqifies JSON arrays in JSON data.
";

void main(string[] args)
{
    // set_log_level(LogLevel.Trace);
    if (args.length == 1)
    {
        stderr.writeln(help);
        return;
    }
    switch (args[1])
    {
    case "depthgauge":
        depthgauge(args[1 .. $]);
        return;
    case "atomize":
        atomize(args[1 .. $]);
        return;
    case "query":
        query_main(args[1 .. $]);
        return;
    case "index":
        index_main(args[1 .. $]);
        return;
    case "merge":
        merge_main(args[2 .. $]);
        return;
    case "table":
        table_main(args[1 .. $]);
        return;
    case "pivot":
        piv_main(args[1 .. $]);
        return;
    case "norm":
        norm_main(args[1 .. $]);
        return;
    case "uniq":
        unique_main(args[1 .. $]);
        return;
    case "diff":
        diff_main(args[1 .. $]);
        return;
    case "principal":
        principal(args[1 .. $]);
        return;
    default:
        mucor_main(args);
    }
}
