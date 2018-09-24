import aggregate
import merge
import argparse
import jsonlcsv
from shutil import copyfile
import os
import pandas as pd


def form_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("datafile")
    parser.add_argument("prefix")
    return parser


def write_jsonl(master: pd.DataFrame,fn: str):
    master.to_json(fn,orient="records",lines=True)


if __name__=="__main__":
    args=form_parser().parse_args()
    if not os.path.exists(args.prefix):
        os.mkdir(args.prefix)
    copyfile(args.datafile,os.path.join(args.prefix,"__master.jsonl"))
    master=pd.read_json(os.path.join(args.prefix,"__master.jsonl"),orient="records",lines=True)
    master["EFFECT"]=master["ANN_hgvs_p"]
    master["EFFECT"].fillna(master["ANN_effect"],inplace=True)
    write_jsonl(merge.merge_rows(master,["sample","CHROM","POS","REF","ALT"]),os.path.join(args.prefix,"__merge_sample.jsonl"))
    write_jsonl(merge.merge_rows_unique(master,["sample","CHROM","POS","REF","ALT"]),os.path.join(args.prefix,"__merge_sample_u.jsonl"))
    merged=pd.read_json(os.path.join(args.prefix,"__merge_sample.jsonl"),orient="records",lines=True)
    jsonlcsv.jsonl2tsv(
        merge.merge_rows_unique(
            merged,["CHROM","POS","REF","ALT"]
        ),
        ["sample", "CHROM", "POS", "REF", "ALT",
         "ANN_gene_name", "EFFECT","INFO_cosmic_ids", "ID"],
        os.path.join(args.prefix,"Variants.tsv")
    )
    merged=pd.read_json(os.path.join(args.prefix,"__merge_sample_u.jsonl"),orient="records",lines=True)
    pivot=aggregate.pivot(merged,
                    ["CHROM", "POS", "REF", "ALT","ANN_gene_name",
                     "EFFECT","INFO_cosmic_ids", "ID"],
                    ["sample"],["AF"],"string_agg",".")
    jsonlcsv.jsonl2tsv(pivot,
                       ["CHROM", "POS", "REF", "ALT",
                        "ANN_gene_name", "EFFECT","INFO_cosmic_ids", "ID"],
                       os.path.join(args.prefix,"AF.tsv")
    )

