import pandas as pd
import sys
import argparse

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-o","--output",default=None)
    parser.add_argument("-d","--delimiter",default=",")
    args=parser.parse_args()
    if args.output:
        pd.read_json(sys.stdin,orient="records",lines=True).to_csv(args.output,sep=args.delimiter)
    else:
        pd.read_json(sys.stdin,orient="records",lines=True).to_csv(sys.stdout,sep=args.delimiter,index=False)
