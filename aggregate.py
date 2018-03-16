import argparse
import pandas as pd
import numpy as np
import sys
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import json


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
    # Make Tuples from ANN sections
    def MakeList(x):
        if 'ANN' in x.name:
            T = tuple(x)
            if len(T) > 1:
                return T
            else:
                return T[0]
        else:
            return ";".join(np.unique(x.astype(str)))

    gb = sub.groupby(([x for x in index]))
    sub = gb.aggregate(MakeList)
    return sub.reset_index()


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

    def MakeUn(x):
        ret = tuple(np.unique(list(x)))
        if len(ret) > 1:
            return ret
        else:
            return ret[0]

    sub = gb.aggregate(MakeUn)
    return sub.reset_index()


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


# pivot dataframe based on parameters provided at runtime
def pivot(master: pd.DataFrame, args: argparse.ArgumentParser)->pd.DataFrame:
    """
    Creates a pivot table using master dataframe and the arguments provided at runtime.

    :param master: Dataframe to be pivoted.
    :type master: pd.Dataframe
    :param args: runtime variables from argparse
    :type args: argparse.ArgumentParser
    :return: pd.Dataframe
    """
    master[args.pivot_index] = master[args.pivot_index].fillna(".")
    sub = master[args.pivot_index + args.pivot_on + args.pivot_value]
    return pd.pivot_table(sub, index=args.pivot_index, columns=args.pivot_on,
                          fill_value=".", values=args.pivot_value)


# query elasticsearch with provided index, doctype, and query string
def query(es:Elasticsearch, index:str, str_q:str, doc:str)->pd.DataFrame:
    """
    Searches an Elasticsearch database by a given index and doctype.
    Uses a Elasticsearch Query String to do filtering.
    Documentation on the Elasticsearch Query String can be found here:
    https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html

    This function takes the direct query string query and uses Elasticsearch-dsl to form the rest of
    the query.

    :param es: Elasticsearch Client
    :type es: Elasticsearch
    :param index: Elasticsearch index to be searched
    :type index:str
    :param str_q: Elasticsearch query string
    :type str_q:str
    :param doc: Elasticsearch doctype to be searched
    :type doc:str
    :return: pd.Dataframe
    """
    res = []
    s = Search(using=es, index=index, doc_type=doc) \
        .query("query_string", query=str_q)
    for hit in s.scan():
        res.append(hit.to_dict())
    print("query returned")
    return pd.DataFrame(res)


# import high confidence query and execute
def hc_query(es:Elasticsearch, index:str, doc:str, conf:dict)->pd.DataFrame:
    """
    Serves as a wrapper for the query function to perform a high confidence query.
    This was determined arbitrarily by our experiences in minimizing false positives.

    :param es: Elasticsearch Client
    :type es: Elasticsearch
    :param index: Elasticsearch index to be searched
    :type index:str
    :param doc: Elasticsearch doctype to be searched
    :type doc:str
    :param conf: dictionary of config files
    :type conf:dict
    :return: pd.Dataframe
    """
    conf["HighConf"]["query"] = conf["HighConf"]["query"] + " AND " + conf["ids_field"] + ":" + str(
        tuple(config["ids"])).replace("'", "").replace(",", " OR")
    hc_q = conf["HighConf"]["query"] + conf["HighConf"]["filter"] + conf["HighConf"]["effect"] + conf["HighConf"][
        "gene"]
    hc_rq = conf["HighConf"]["query"] + conf["HighConf"]["filterfn"] + conf["HighConf"]["queryfn"] + conf["HighConf"][
        "effect"] + conf["HighConf"]["gene"]
    df = query(es, index, hc_q, doc)
    return pd.concat([df, query(es, index, hc_rq, doc)])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-id", "--index", default="test")
    parser.add_argument("-q", "--query", nargs="+")
    parser.add_argument("-m", "--merge", nargs="+")
    parser.add_argument("-pi", "--pivot_index", nargs="+")
    parser.add_argument("-po", "--pivot_on", nargs="+")
    parser.add_argument("-pv", "--pivot_value", nargs="+")
    parser.add_argument("-o", "--out_prefix", default="test")
    parser.add_argument("-hc", "--high_confidence", action="store_true")
    parser.add_argument("-c", "--config", nargs="+", required=True)
    parser.add_argument("-d", "--doctype", default="variant_vcf")
    parser.add_argument("-l", "--column_list")

    # parse args and open elasticsearch client
    args = parser.parse_args()
    client = Elasticsearch(timeout=30, max_retries=10, retry_on_timeout=True)
    config = {}
    for file in args.config:
        config.update(json.load(open(file, "r")))
    # do some query or exit
    if args.query:
        data = query(client, args.index, " ".join(args.query), args.doctype).fillna("")
    elif args.high_confidence:
        data = hc_query(client, args.index, args.doctype, config).fillna("")
    else:
        print("must run some query")
        sys.exit(1)
    print("dataframe built")
    data = alter_table(data, config)
    # do sort
    if "ANN_gene_name" in data.columns:
        data.sort_values("ANN_gene_name", inplace=True)

    # merge rows or don't
    if args.merge:
        merge = merge_rows(data, args.merge)
        merge = merge.fillna(".")
        merge.to_csv(args.out_prefix + "_" + "master.tsv", sep="\t", na_rep=".",float_format='%.4f')
    else:
        merge = data.fillna(".")
        merge.to_csv(args.out_prefix + "_" + "master.tsv", sep="\t", na_rep=".",float_format='%.4f')

    # pivot if all args are present
    if args.pivot_index and args.pivot_on and args.pivot_value:

        # if merge uniquely merge rows on index
        if args.merge:
            data = merge_rows_unique(data, args.merge)
            if "ANN_hgvs_p" in data:
                data['ANN_hgvs_p'] = np.where(data['ANN_hgvs_p'] == "", data['ANN_effect'], data['ANN_hgvs_p'])

        # do pivot
        piv = pivot(data, args)
        piv.columns = piv.columns.droplevel(0)
        piv.reset_index(inplace=True)

        # if column list is provided sort dataframe and select columns based
        # on file order and contents
        # should contain only columns name one for each line
        if args.column_list:
            order = pd.read_table(args.column_list, header=None)
            order = order[0].tolist()
            piv = piv[args.pivot_index + order]

        # if ref and alt in index separate data into two dataframes (indels and snps) and write
        if "REF" in args.pivot_index and "ALT" in args.pivot_index:
            piv[(piv["REF"].str.len() > 1) | (piv["ALT"].str.len() > 1)].to_csv(args.out_prefix + "_INDEL_" + "piv.tsv",
                                                                                sep="\t", index=False)
            piv[(piv["REF"].str.len() == 1) & (piv["ALT"].str.len() == 1)].to_csv(
                args.out_prefix + "_SNPS_" + "piv.tsv", sep="\t", index=False)
        else:
            pivot(data, args).to_csv(args.out_prefix + "_" + "piv.tsv", sep="\t")
    else:
        print("To generate pivot table  the flags --pivot_index --pivot_on --pivot_value" + \
              " are required. If you didn't want to pivot ignore this.")
