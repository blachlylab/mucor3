module mucor3.mucor.vcf;

import htslib.hts_log;
import core.stdc.stdlib: exit;
import std.parallelism: parallel;
import core.sync.mutex: Mutex;
import progress;
import std.stdio;
import dhtslib.vcf;
import libmucor.vcfops;
import std.format: format;
import std.path: buildPath, baseName;


void defaultAtomize(string fn, string outfile)
{
    auto output = File(outfile, "w");
	//open vcf
    auto vcf = VCFReader(fn,-1, UnpackLevel.All);

    //get info needed from header 
    auto cfg = getHeaderConfig(vcf.vcfhdr);
    int vcf_row_count = 0;
    int output_count = 0;
    // loop over records and parse
    foreach(x; vcf){
        
        auto obj = parseRecord(x, cfg);
        applyOperations(
            obj, 
            false, 
            false, 
            false, 
            false, 
            &vcf_row_count, 
            &output_count, output); 
    }
    if(vcf_row_count == 0) {
        hts_log_error(__FUNCTION__, format("VCF File %s had no records", fn));
        exit(1);
    }
}

void atomizeVcfs(string[] vcf_files, string vcf_json_dir) {
    Bar b = new Bar();
    b.message = {return "Atomizing vcfs";};
    b.max = vcf_files.length;
    b.fill = "#";
    auto m = new Mutex();
    foreach (string f; parallel(vcf_files))
    {
        defaultAtomize(f, buildPath(vcf_json_dir, baseName(f)));
        m.lock;
        b.next;
        m.unlock;
    }
    b.finish();
}
