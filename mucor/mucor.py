import mucor.aggregate as aggregate
import mucor.merge as merge
import argparse
import mucor.jsonlcsv as jsonlcsv
from shutil import copyfile
import os
import sys
import pandas as pd


def form_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("-e","--extra",default=["ANN_gene_name","EFFECT","INFO_cosmic_ids", "INFO_dbsnp_ids"],nargs="+")
    parser.add_argument("-i","--index",default="AF")
    parser.add_argument("datafile")
    parser.add_argument("prefix")
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

    if(args.index not in master):
        print("Error: missing column ",args.index)
        sys.exit(0)

    #create EFFECT column
    if("ANN_hgvs_p" in master):
        master["EFFECT"]=master["ANN_hgvs_p"]
        master["EFFECT"].fillna(master["ANN_effect"],inplace=True)

    #create Total Depth column
    if(("Ref_Depth" in master) and ("Alt_depths" in master)):
        master["Total_depth"]=master["Ref_Depth"]+master["Alt_depths"].apply(sum)
    samples=set(master["sample"])

    missing_fields = set(args.extra) - set(master.columns)
    if(len(missing_fields)!=0):
        print("Warning: missing column ",missing_fields)

    extra_fields=list(set(master.columns) & set(args.extra))

    print("sorting")
    master.set_index(required_fields+[col for col in master if col.startswith('ANN_')],inplace=True)
    master.sort_index(inplace=True)
    master.reset_index(inplace=True)
    print("merging")
    #write the merged datasets - merged on CHROM POS REF ALT sample to remove duplicate entrys related to alternate annotations
    write_jsonl(merge.merge_rows(master,required_fields),os.path.join(args.prefix,"__merge_sample.jsonl"))
    write_jsonl(merge.merge_rows_unique(master,required_fields),os.path.join(args.prefix,"__merge_sample_u.jsonl"))

    #import merged dataset
    merged=pd.read_json(os.path.join(args.prefix,"__merge_sample.jsonl"),orient="records",lines=True)

    #write master tsv
    jsonlcsv.jsonl2tsv(
        merged,
        required_fields + extra_fields,
        os.path.join(args.prefix,"master.tsv")
    )
    condensed=merge.merge_rows_unique(merged,["CHROM","POS","REF","ALT"])
    #write Variants tsv
    jsonlcsv.jsonl2tsv(
        condensed,
        required_fields + extra_fields,
        os.path.join(args.prefix,"Variants.tsv")
    )

    #load uniquely merged dataset
    #merged=pd.read_json(os.path.join(args.prefix,"__merge_sample_u.jsonl"),orient="records",lines=True)

    #pivot AF
    pivot=aggregate.pivot(merged,
                    ["CHROM", "POS", "REF", "ALT"],#["ANN_gene_name","EFFECT","INFO_cosmic_ids", "INFO_dbsnp_ids"],
                    ["sample"],[args.index],"string_agg",".")

    #if any samples removed add them back
    for x in (samples-set(pivot.columns)):
        pivot[x]="."
        print(x)

    pivot=aggregate.join_columns(condensed,pivot,["CHROM", "POS", "REF", "ALT"],
                                 extra_fields)
    pivot.set_index(["CHROM", "POS", "REF", "ALT"]+extra_fields,inplace=True)
    pivot.reset_index(inplace=True)
    pivot=aggregate.add_result_metrics(pivot,["CHROM", "POS", "REF", "ALT"]+extra_fields)

    #write AF pivot table
    jsonlcsv.jsonl2tsv(pivot,
                       ["CHROM", "POS", "REF", "ALT"]+extra_fields,
                       os.path.join(args.prefix,"AF.tsv")
    )



if __name__=="__main__":
    main()




