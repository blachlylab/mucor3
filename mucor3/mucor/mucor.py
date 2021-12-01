import mucor.aggregate as aggregate
import mucor.merge as merge
import mucor.jsonlcsv as jsonlcsv
import argparse
from shutil import copyfile
import os
import sys
import pandas as pd

# convert arrays to strings and fix excel wrapping issue
def fix_cells(x):
    ret = x
    if type(x) is list:
        new_list = list()
        for item in x:
            if type(item) is list:
                new_list.append("&".join([str(y) for y in item]))
            else:
                new_list.append(str(item))
        ret = ",".join([str(y) for y in new_list])
    if type(ret) is str:
        if len(ret) > 32767:
            ret = ret[0::32767]
    return ret

def form_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    #default=["ANN_gene_name","EFFECT","INFO_cosmic_ids", "INFO_dbsnp_ids"]
    parser.add_argument("-e","--extra",help="comma delimited list of extra columns to include in pivoted table index",type=str)
    parser.add_argument("-a","--value",default="AF", help="Value to be displayed in pivoted table values")
    parser.add_argument("-m","--merge", action="store_true", help="Merge rows togther to deal with annotation explosion")
    parser.add_argument("datafile", help="input jsonl data from vcf_atomizer")
    parser.add_argument("prefix", help="directory for output")
    return parser


def write_jsonl(master: pd.DataFrame,fn: str):
    master.to_json(fn,orient="records",lines=True)


def main():
    #parse args
    args=form_parser().parse_args()
    if not os.path.exists(args.prefix):
        os.mkdir(args.prefix)

    #take json datafile and copy it
    print("copying data")
    copyfile(args.datafile,os.path.join(args.prefix,"__master.jsonl"))

    #import jsonl
    print("importing")
    master=pd.read_json(os.path.join(args.prefix,"__master.jsonl"),orient="records",lines=True)

    required_fields=["sample", "CHROM", "POS", "REF", "ALT"]
    missing_fields = set(required_fields) - set(master.columns)
    if(len(missing_fields)!=0):
        print("Error: missing column ",missing_fields)
        sys.exit(0)

    if(args.value not in master):
        print("Error: missing column ",args.value)
        sys.exit(0)

    #create EFFECT column
    if("ANN_hgvs_p" in master):
        master["EFFECT"]=master["ANN_hgvs_p"]
        master["EFFECT"].fillna(master["ANN_effect"],inplace=True)

    #create Total Depth column
    if(("Ref_Depth" in master) and ("Alt_depths" in master)):
        master["Total_depth"]=master["Ref_Depth"]+master["Alt_depths"].apply(sum)
    samples=set(master["sample"])

    extra_fields=[]
    if args.extra is not None:
        missing_fields = set(args.extra.split(",")) - set(master.columns)
        if(len(missing_fields)!=0):
            print("Warning: missing column ",missing_fields)

        extra_fields=list(set(master.columns) & set(args.extra.split(",")))

    print("sorting")
    master.set_index(required_fields, inplace=True)
    cols = list(master)
    for x in extra_fields[::-1]:
        cols.insert(0, cols.pop(cols.index(x)))
    master = master.loc[:, cols]
    master.sort_index(inplace=True)
    master.reset_index(inplace=True)

    merged = master
    condensed = master
    if args.merge:
        print("merging")
        #write the merged datasets - merged on CHROM POS REF ALT sample to remove duplicate entrys related to alternate annotations
        write_jsonl(merge.merge_rows(master,required_fields),os.path.join(args.prefix,"__merge_sample.jsonl"))
        write_jsonl(merge.merge_rows_unique(master,required_fields),os.path.join(args.prefix,"__merge_sample_u.jsonl"))

        #import merged dataset
        merged=pd.read_json(os.path.join(args.prefix,"__merge_sample.jsonl"),orient="records",lines=True)

    #write master tsv
    jsonlcsv.jsonl2tsv(
        merged,
        required_fields,
        os.path.join(args.prefix,"master.tsv")
    )
    condensed=merge.merge_rows_unique(merged,["CHROM","POS","REF","ALT"])
    #write Variants tsv
    jsonlcsv.jsonl2tsv(
        condensed,
        required_fields,
        os.path.join(args.prefix,"Variants.tsv")
    )

    #load uniquely merged dataset
    #merged=pd.read_json(os.path.join(args.prefix,"__merge_sample_u.jsonl"),orient="records",lines=True)

    #pivot AF
    pivot=aggregate.pivot(merged,
                    ["CHROM", "POS", "REF", "ALT"],#["ANN_gene_name","EFFECT","INFO_cosmic_ids", "INFO_dbsnp_ids"],
                    ["sample"],[args.value],"string_agg",".")

    #if any samples removed add them back
    for x in (samples-set(pivot.columns)):
        pivot[x]="."
        print(x)
    if args.merge:
        pivot=aggregate.join_columns(condensed,pivot,["CHROM", "POS", "REF", "ALT"],
                                     extra_fields)
    else:
        pivot=aggregate.join_columns_unmerged(master,pivot,
                                              ["CHROM", "POS", "REF", "ALT"],
                                                extra_fields)
    pivot.set_index(["CHROM", "POS", "REF", "ALT"],inplace=True)

    cols = list(pivot)
    for x in extra_fields[::-1]:
        cols.insert(0, cols.pop(cols.index(x)))
    pivot = pivot.loc[:, cols]

    pivot.reset_index(inplace=True)
    pivot=aggregate.add_result_metrics(pivot,["CHROM", "POS", "REF", "ALT"]+extra_fields)
    pivot = pivot.applymap(fix_cells)

    #write AF pivot table
    jsonlcsv.jsonl2tsv(pivot,
                       ["CHROM", "POS", "REF", "ALT"],
                       os.path.join(args.prefix,"AF.tsv")
    )



if __name__=="__main__":
    main()




