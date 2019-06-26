import argparse
import sys
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch_dsl import Search
import json

#query elasticsearch
def query(es:Elasticsearch, index:str, doctype:str,str_q:str):
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
    :param doc: Elasticsearch doctype to be searched
    :type doc:str
    :param str_q: Elasticsearch query string
    :type str_q:str
    :return: generator
    """
    # TODO: Query string is never empty due to doc requirement
    if str_q=="":
        s = Search(using=es, index=index)
    else:
        s = Search(using=es, index=index) \
            .query("query_string", query=str_q)
    for hit in s.scan():
        yield hit.to_dict()


def run_query(args,es):
    if args.query=="":
        args.query="type:"+args.doctype
    else:
        args.query=args.query+" AND type:"+args.doctype
    # TODO: Change scan to something else for cases where args.numlines is not -1
    if args.numlines!=-1:
        for i,x in enumerate(query(es,args.index,args.doctype,args.query)):
            if i==args.numlines:
                break
            print(json.dumps(x))
    else:
        for x in query(es,args.index,args.doctype,args.query):
            print(json.dumps(x))

def get_mapping(d):
    """
    Reports fields and types of a dictionary recursively
    :param object: dictionary to search
    :type object:dict
    """
    mapp=dict()
    for x in d:
        if type(d[x])==list:
            mapp[x]=str(type(d[x][0]).__name__)
        elif type(d[x])==dict:
            mapp[x]=get_mapping(d[x])
        else:
            mapp[x]=str(type(d[x]).__name__)
    return mapp

def query_fields(args,es):
    mapping=dict()
    for x in query(es,args.index,args.doctype,args.query):
        mapping.update(get_mapping(x))
    print(json.dumps(mapping))


def form_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Query data from Elasticsearch using Query String Query Syntax',
                                     epilog='See documentation for complete details')

    parser.add_argument("-e","--host",help="ip address of Elasticsearch host. Defaults to localhost:9200",default=None)
    parser.add_argument("-a","--aws",help="Using Amazon Web Services: requires boto3",action="store_true")
    parser.add_argument("-r","--aws_region",help="AWS region of Elasticsearch instance",default="us-east-2")
    parser.add_argument("-n","--numlines",help="Number of result lines returned: default is all",type=int,default=-1)
    parser.add_argument("index",help="Elasticsearch index to query.")
    parser.add_argument("doctype",help="Elasticsearch doc type to query.")
    parser.add_argument("query", type=str,help="Elasticsearch query string to query index.")

    parser.set_defaults(func=run_query)
    return parser


if __name__ == "__main__":
    # parse args and open elasticsearch client
    args = form_parser().parse_args()
    client=None
    if args.aws:
        if not(args.aws and args.aws_region and args.host):
            print("AWS region and AWS elasticsearch host needed with AWS flag")
            sys.exit(1)
        from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
        auth = BotoAWSRequestsAuth(aws_host=args.host,
                           aws_region=args.aws_region,
                           aws_service='es')
        client = Elasticsearch(host=args.host,
                          port=80,
                          connection_class=RequestsHttpConnection,
                          http_auth=auth,
                          timeout=30, max_retries=10, retry_on_timeout=True)
    if args.host:
        client = Elasticsearch(args.host,timeout=30, max_retries=10, retry_on_timeout=True)
    else:
        client = Elasticsearch(timeout=30, max_retries=10, retry_on_timeout=True)
    args.func(args,client)
