import pandas as pd
import sys
import argparse

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-o","--output",default=None)
    parser.add_argument("-d","--delimiter",default=",")
    parser.add_argument("-t","--tsv",action="store_true")
    parser.add_argument("-i","--index",nargs="+",default=[])
    args=parser.parse_args()
    data=pd.read_json(sys.stdin,orient="records",lines=True)
    if args.index!=[]:
        data.set_index(args.index,inplace=True)
        data.reset_index(inplace=True)
    if args.tsv:
        args.delimiter="\t"
    if args.output:
        data.to_csv(args.output,sep=args.delimiter,index=False)
    else:
        data.to_csv(sys.stdout,sep=args.delimiter,index=False)
