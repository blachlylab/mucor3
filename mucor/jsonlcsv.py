import pandas as pd
import sys
import argparse

def jsonl2tsv(master: pd.DataFrame,index: list,fn: str):
    master.set_index(index,inplace=True)
    master.reset_index(inplace=True)
    master.to_csv(fn,sep="\t",index=False)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-o","--output",default=None)
    parser.add_argument("-d","--delimiter",default=",")
    parser.add_argument("-i","--index",nargs="+",default=[])
    args=parser.parse_args()
    data=pd.read_json(sys.stdin,orient="records",lines=True)
    if args.index!=[]:
        data.set_index(args.index,inplace=True)
        data.reset_index(inplace=True)
    if args.output:
        data.to_csv(args.output,sep=args.delimiter)
    else:
        data.to_csv(sys.stdout,sep=args.delimiter,index=False)
