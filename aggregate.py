import argparse
import pandas as pd
import numpy as np
import sys


# Filter based on depth
# TODO: Make this more intelligent
def alter_table(master: pd.DataFrame, conf: dict) -> pd.DataFrame:
    """
    Filters table based on a depth parameter in the conf dictionary.
    Relies on the AD or DP fields being present in the dataframe.

    :param master:Dataframe to be filtered
    :type master: pd.Dataframe
    :param conf: config files dictionary
    :type conf: dict
    :return:pd.Dataframe
    """
    if "Alt_depths" in master.columns and "Ref_Depth" in master.columns and "QSS" in master.columns:
        master["Total_depth"]=master["Alt_depths"].apply(sum)+master["Ref_Depth"]
        master = master[ master["Total_depth"]> conf["depth"]]
        #master["Avg_QSS_Per_Read_by_Allele"]= pd.Series([[a/b for a,b in zip(x,y)] for x,y in zip(master["QSS"],[[b]+a for a,b in zip(master["Ref_Depth"],master["Alt_depths"])])])
        master=master.assign(Avg_QSS_Per_Read_by_Allele=pd.Series([[a/b for a,b in zip(x,y)] for x,y in zip(master["QSS"],[[a]+b for a,b in zip(master["Ref_Depth"],master["Alt_depths"])])]).values)

    return master

def string_agg(x):
    return np.unique(x)

# pivot dataframe based on parameters provided at runtime
def pivot(master: pd.DataFrame, pivot_index: list,
          pivot_on: list,pivot_value: list ,agg_func: str,fill_value: str)->pd.DataFrame:
    """
    Creates a pivot table using master dataframe and the arguments provided at runtime.

    :param master: Dataframe to be pivoted.
    :type master: pd.Dataframe
    :param args: runtime variables from argparse
    :type args: argparse.ArgumentParser
    :return: pd.Dataframe
    """
    master[pivot_index] = master[pivot_index].fillna(".")
    sub = master[pivot_index + pivot_on + pivot_value]
    func=agg_func
    if agg_func=="string_agg":
        func=eval(func)
    piv = pd.pivot_table(sub, index=pivot_index, columns=pivot_on,
                          fill_value=fill_value, values=pivot_value,
                          aggfunc=func)
    piv.columns = piv.columns.droplevel(0)
    piv.reset_index(inplace=True)
    piv.insert(len(pivot_index),
        "Positive results",
        (piv.shape[1]-len(pivot_index)-(piv.iloc[:,len(pivot_index):] == ".")
               .sum(axis=1)))
    piv.insert(len(pivot_index)+1,
        "Positive rate",
        (piv.shape[1]-(len(pivot_index)+1)-(piv.iloc[:,len(pivot_index)+1:] == ".")
               .sum(axis=1))/(piv.shape[1]-(len(pivot_index)+1)))
    return piv


def form_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("-pi", "--pivot_index", nargs="+",required=True)
    parser.add_argument("-po", "--pivot_on", nargs="+",required=True)
    parser.add_argument("-pv", "--pivot_value", nargs="+",required=True)
    parser.add_argument("-f", "--fill_value",default=".")
    parser.add_argument("-t", "--from_tsv",action="store_true")
    parser.add_argument("-a", "--agg-func",default="string_agg")
    return parser


if __name__ == "__main__":

    # parse args and open elasticsearch client
    args = form_parser().parse_args()
    data=[]
    if args.from_tsv:
        data=pd.read_csv(sys.stdin,delimiter="\t")
    else:
        data=pd.read_json(sys.stdin,orient="records",lines=True)
    for i,x in enumerate(args.pivot_value):
        if x in args.pivot_index:
            col=x+"2"
            data[col]=data[x]
            args.pivot_value[i]=col
        elif x in args.pivot_on:
            col=x+"2"
            data[col]=data[x]
            args.pivot_value[i]=col
    # do pivot
    piv = pivot(data, args.pivot_index,args.pivot_on,args.pivot_value,args.agg_func,args.fill_value)
    piv.to_json(sys.stdout,orient="records",lines=True)
