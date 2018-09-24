import numpy as np
import pandas as pd
import argparse
import sys
delim=";"
# Make Tuples from ANN sections
def MakeList(x):
    ret=[]
    for item in x.dropna():
        if type(item)==list:
            ret+=item
        else:
            ret.append(item)
    un=np.unique(ret)
    if len(un)==1:
        return un[0]
    elif len(ret) > 1:
        return delim.join(str(x) for x in ret)

# merge annotations on an index
def merge_rows(sub: pd.DataFrame, index: list) -> list:
    """
    Merges rows of a dataframe together using groupby and an index to groupby.

    :param sub: Dataframe to have rows merged
    :type sub: pd.Dataframe
    :param index: list of columns to groupby
    :type index: list
    :return: pd.Dataframe

    """
    gb = sub.groupby(([x for x in index]))
    sub = gb.aggregate(MakeList)
    return sub.reset_index()

def MakeUn(x):
    ret=[]
    for item in x.dropna():
        if type(item)==list:
            ret+=item
        else:
            ret.append(item)
    ret = np.unique(ret)
    if len(ret) > 1:
        return delim.join(str(x) for x in ret)
    elif len(ret) > 0:
        return ret[0]

# merge annotations on an index uniquely
def merge_rows_unique(sub: pd.DataFrame, index: list) -> list:
    """
    Merges rows of a dataframe together using groupby and an index to groupby.
    Items merged together as a set rather than a list or tuple.

    :param sub: Dataframe to have rows merged
    :type sub: pd.Dataframe
    :param index: list of columns to groupby
    :type index: list
    :return: pd.Dataframe
    """
    gb = sub.groupby(([x for x in index]))

    sub = gb.aggregate(MakeUn)
    return sub.reset_index()

def form_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Merges rows with same indices")

    parser.add_argument('-u', '--unique', action="store_true",
                        help="merges uniquely")
    parser.add_argument('-d','--delimiter')
    parser.add_argument('indices', nargs="+")
    return parser

if __name__=="__main__":
    args=form_parser().parse_args()
    data=pd.read_json(sys.stdin,orient="records",lines=True)
    if args.delimiter:
        delim=args.delimiter
    for x in args.indices:
        if x not in data.columns:
            print(x+" field not in stream")
            sys.exit(1)
    if args.unique:
        merge_rows_unique(data,args.indices).to_json(sys.stdout,orient="records",lines=True)
    else:
        merge_rows(data,args.indices).to_json(sys.stdout,orient="records",lines=True)
