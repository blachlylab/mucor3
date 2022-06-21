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
import libmucor.khashl;
import libmucor: setup_global_pool;

int threads = -1;
string bam_dir = "";
string[] extraFields;
string prefix = "";
string config_file = "";
string query_str = "";
ulong fileCacheSize = 8192;
ulong smallsSize = 128;

string pivotValue = "FORMAT/AF";
string pivotOn = "sample";

string[] requiredCols = ["CHROM", "POS", "REF", "ALT"];

string help_str = "mucor3 <options> [input vcfs]";

void mucor_main(string[] args)
{
    hts_set_log_level(htsLogLevel.HTS_LOG_INFO);
    auto res = getopt(args, config.bundling,
            "threads|t", "threads for running mucor", &threads,
            "bam-dir|b", "folder of bam files", &bam_dir, 
            "extra-fields|e", "extra fields from VCF to be displayed in pivot tables",&extraFields, 
            "prefix|p", "output directory for files (can be directory or file prefix)", &prefix, 
            "config|c", "specify json config file (not yet working)", &config_file, 
            "query|q", "filter vcf data using varquery syntax", &query_str,
            "file-cache-size|f", "number of highly used files kept open", &fileCacheSize,
            "ids-cache-size|i", "number of ids that can be stored per key before a file is opened", &smallsSize);

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

    setup_global_pool(threads);

    log_info(__FUNCTION__, "Using %d threads", defaultPoolThreads);

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

    auto pivReqCols = pivotOn ~ requiredCols ~ ["FORMAT/AF"];
    auto allsamples = validateVcfData(vcfJsonFiles, pivReqCols);

    auto index_dir = buildPath(prefix, "index");
    mkdirRecurse(index_dir);

    string combined_json_file;

    if (query_str != "")
    {

        log_info(__FUNCTION__, "Indexing vcf data ...");
        indexJsonFiles(args[0], query_str, vcfJsonFiles, index_dir, threads, fileCacheSize, smallsSize);

        log_info(__FUNCTION__, "Filtering vcf data...");
        combined_json_file = buildPath(prefix, "filtered.json");

        queryJsonFiles(args[0], query_str, vcfJsonFiles, index_dir, threads, combined_json_file);
    }
    else
    {
        combined_json_file = buildPath(prefix, "all.json");
        log_info(__FUNCTION__, "Combining vcf data...");
        combineJsonFiles(vcfJsonFiles, combined_json_file);
    }

    auto colData = collectColumns(combined_json_file, extraFields);
    auto master = buildPath(prefix, "master.json");

    khashlSet!(string, true) initialColsSet;

    foreach(col; pivReqCols ~ extraFields){
        initialColsSet.insert(col);
    }

    auto otherCols = colData.cols - initialColsSet;

    auto masterCols = pivReqCols ~ extraFields ~ otherCols.byKey.map!(x => cast(string)x).array.sort.array;

    flattenAndMakeMaster(combined_json_file, pivReqCols, masterCols, prefix);

    log_warn(__FUNCTION__, "The following samples had no rows after filtering: %s", (allsamples - colData.samples).byKey.array);
    auto samples = allsamples.byKey.map!(x => cast(string)x).array.sort.array;

    pivotAndMakeTable(master, requiredCols, pivotOn, pivotValue, extraFields, samples, prefix);
}
