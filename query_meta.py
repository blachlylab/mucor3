import argparse
import pandas as pd
import numpy as np
import sys
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import json

#query elasticsearch
def query(es, index, doctype,str_q):
    res = []
    s=""
    if str_q=="":
        s = Search(using=es, index=index, doc_type=doctype)
    else:
        s = Search(using=es, index=index, doc_type=doctype) \
            .query("query_string", query=str_q)
    for hit in s.scan():
        res.append(hit.to_dict())
    return pd.DataFrame(res)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-id", "--index", default="test")
    parser.add_argument("-q", "--query", nargs="+",default=[])
    parser.add_argument("-m","--meta_doctype",default="meta")
    parser.add_argument("-mid", "--meta_id", default=None)
    parser.add_argument("--write_meta",action="store_true")
    parser.add_argument("-host","--host",default=None)

    # parse args and open elasticsearch client
    args = parser.parse_args()
    client=None
    if args.host:
        client = Elasticsearch(args.host,timeout=30, max_retries=10, retry_on_timeout=True)
    else:
        client = Elasticsearch(timeout=30, max_retries=10, retry_on_timeout=True)

    if args.meta_id is None:
        args.meta_id=args.meta_doctype+"_id"
    data=query(client,args.index,args.meta_doctype," ".join(args.query))
    if(args.write_meta):
        data.to_csv("meta.tsv",sep="\t")
    ids=data[args.meta_id].tolist()
    print(json.dumps({"ids":ids,"ids_field":args.meta_id}))
