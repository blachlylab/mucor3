module mucor3.mucor;
import std.getopt;
import std.stdio;
import std.format : format;
import std.algorithm : map, sort, uniq;
import std.array : array;
import std.file : exists, isDir, mkdirRecurse;
import std.path : isValidPath, buildPath, baseName;
import htslib.hts_log;
import core.stdc.stdlib : exit;
import std.parallelism;

import mucor3.mucor.vcf;
import mucor3.mucor.query;
import mucor3.mucor.table;
import libmucor.error;

int threads = 0;
string bam_dir = "";
string[] extra_fields;
string prefix = "";
string config_file = "";
string query = "";

string[] requiredCols = ["sample", "CHROM", "POS", "REF", "ALT"];

string help_str = "mucor3 <options> [input vcfs]";

void mucor_main(string[] args)
{
    hts_set_log_level(htsLogLevel.HTS_LOG_INFO);
    auto res = getopt(args, config.bundling,
            "threads|t", "threads for running mucor", &threads,
            "bam-dir|b", "folder of bam files", &bam_dir, "extra-fields|e",
            "extra fields from VCF to be displayed in pivot tables",
            &extra_fields, "prefix|p",
            "output directory for files (can be directory or file prefix)", &prefix, "config|c",
            "specify json config file",
            &config_file, "query|q", "filter vcf data using varquery syntax", &query);

    if (res.helpWanted)
    {
        defaultGetoptPrinter(help_str, res.options);
        exit(0);
    }
    if (args.length == 1)
    {
        defaultGetoptPrinter(help_str, res.options);
        log_err(__FUNCTION__, "Please specify input vcfs");
        exit(1);
    }

    if (threads == 0)
    {
        defaultPoolThreads(totalCPUs);
    }
    else
    {
        defaultPoolThreads(threads);
    }

    /// create prefix folder
    if (prefix.exists)
    {
        if (!prefix.isDir)
        {
            log_err(__FUNCTION__, "Specified prefix is not a folder");
            exit(1);
        }
    }
    else
    {
        mkdirRecurse(prefix);
    }

    /// create vcf data folder
    auto vcf_json_dir = buildPath(prefix, "vcf_data");
    mkdirRecurse(vcf_json_dir);

    string[] vcfFiles;

    foreach (f; args[1 .. $])
    {
        if (!f.exists)
        {
            log_err(__FUNCTION__, format("VCF file: %s does not exist", f));
            exit(1);
        }
        if (f.isDir)
        {
            log_err(__FUNCTION__, format("Not a vcf_file: %s", f));
            exit(1);
        }
        vcfFiles ~= f;
    }

    if (vcfFiles.map!(x => baseName(x)).array.sort.uniq.array.length != vcfFiles.length)
    {
        log_err(__FUNCTION__, "Overlapping base file names for input vcf files");
        exit(1);
    }

    auto vcfJsonFiles = vcfFiles.map!(x => buildPath(vcf_json_dir, baseName(x))).array;

    atomizeVcfs(args[0], vcfFiles, vcf_json_dir);

    auto index_dir = buildPath(prefix, "indexes");
    mkdirRecurse(index_dir);

    string combined_json_file;

    if (query != "")
    {
        auto indexFile = buildPath(prefix, "all.index");

        log_info(__FUNCTION__, "Indexing vcf data ...");
        indexJsonFiles(args[0], vcfJsonFiles, index_dir, indexFile);

        log_info(__FUNCTION__, "Filtering vcf data...");
        combined_json_file = buildPath(prefix, "filtered.json");

        queryJsonFiles(vcfJsonFiles, indexFile, query, combined_json_file);
    }
    else
    {
        combined_json_file = buildPath(prefix, "all.json");
        File output = File(combined_json_file, "w");
        log_info(__FUNCTION__, "Combining vcf data...");
        foreach (f; vcfJsonFiles)
        {
            foreach (line; File(f).byLine)
            {
                output.writeln(line);
            }
        }
    }

    auto pivReqCols = requiredCols ~= ["AF"];

    auto colData = validateDataAndCollectColumns(combined_json_file, pivReqCols, extra_fields);
    auto master = buildPath(prefix, "master.tsv");

    flattenAndMakeMaster(combined_json_file, requiredCols, extra_fields, master);

    auto piv = buildPath(prefix, "AF.tsv");
    pivotAndMakeTable(piv, requiredCols, "sample", "AF", extra_fields, colData.samples, piv);

    // #create EFFECT column
    // if("ANN_hgvs_p" in master):
    //     master["EFFECT"]=master["ANN_hgvs_p"]
    //     master["EFFECT"].fillna(master["ANN_effect"],inplace=True)

    // #create Total Depth column
    // if(("Ref_Depth" in master) and ("Alt_depths" in master)):
    //     master["Total_depth"]=master["Ref_Depth"]+master["Alt_depths"].apply(sum)
    // samples=set(master["sample"])

    // extra_fields=[]
    // if args.extra is not None:
    //     missing_fields = set(args.extra.split(",")) - set(master.columns)
    //     if(len(missing_fields)!=0):
    //         print("Warning: missing column(s) ",missing_fields)

    //     extra_fields=[x for x in args.extra.split(",") if x in list(master)]

    // print("sorting")
    // master.set_index(required_fields, inplace=True)
    // cols = list(master)
    // for x in extra_fields[::-1]:
    //     cols.insert(0, cols.pop(cols.index(x)))
    // master = master.loc[:, cols]
    // master.sort_index(inplace=True)
    // master.reset_index(inplace=True)

    // merged = master
    // condensed = master
    // if args.merge:
    //     print("merging")
    //     #write the merged datasets - merged on CHROM POS REF ALT sample to remove duplicate entrys related to alternate annotations
    //     write_jsonl(merge.merge_rows(master,required_fields),os.path.join(args.prefix,"__merge_sample.jsonl"))
    //     write_jsonl(merge.merge_rows_unique(master,required_fields),os.path.join(args.prefix,"__merge_sample_u.jsonl"))

    //     #import merged dataset
    //     merged=pd.read_json(os.path.join(args.prefix,"__merge_sample.jsonl"),orient="records",lines=True)

    // #write master tsv
    // jsonlcsv.jsonl2tsv(
    //     merged,
    //     required_fields,
    //     os.path.join(args.prefix,"master.tsv")
    // )
    // condensed=merge.merge_rows_unique(merged,["CHROM","POS","REF","ALT"])
    // #write Variants tsv
    // jsonlcsv.jsonl2tsv(
    //     condensed,
    //     required_fields,
    //     os.path.join(args.prefix,"Variants.tsv")
    // )

    // #load uniquely merged dataset
    // #merged=pd.read_json(os.path.join(args.prefix,"__merge_sample_u.jsonl"),orient="records",lines=True)

    // #pivot AF
    // pivot=aggregate.pivot(merged,
    //                 ["CHROM", "POS", "REF", "ALT"],#["ANN_gene_name","EFFECT","INFO_cosmic_ids", "INFO_dbsnp_ids"],
    //                 ["sample"],[args.value],"string_agg",".")

    // #if any samples removed add them back
    // for x in (samples-set(pivot.columns)):
    //     pivot[x]="."
    //     print(x)
    // if args.merge:
    //     pivot=aggregate.join_columns(condensed,pivot,["CHROM", "POS", "REF", "ALT"],
    //                                  extra_fields)
    // else:
    //     pivot=aggregate.join_columns_unmerged(master,pivot,
    //                                           ["CHROM", "POS", "REF", "ALT"],
    //                                             extra_fields)
    // pivot.set_index(["CHROM", "POS", "REF", "ALT"],inplace=True)

    // cols = list(pivot)
    // for x in extra_fields[::-1]:
    //     cols.insert(0, cols.pop(cols.index(x)))
    // pivot = pivot.loc[:, cols]

    // pivot.reset_index(inplace=True)
    // pivot=aggregate.add_result_metrics(pivot,["CHROM", "POS", "REF", "ALT"]+extra_fields)
    // pivot = pivot.applymap(fix_cells)

    // #write AF pivot table
    // jsonlcsv.jsonl2tsv(pivot,
    //                    ["CHROM", "POS", "REF", "ALT"],
    //                    os.path.join(args.prefix,"AF.tsv")
    // )
}
