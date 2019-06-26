from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
import argparse
import json
import sys

def form_query(index,es):
    for x in sys.stdin:
        line = json.loads(x)
        yield {
            "_op_type": "index",
            "_index": index,
            "_source": line,
            "_type":"doc"
        }


def form_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Ingest jsonl data into an Elasticsearch instance.')

    parser.add_argument("-e","--host",help="ip address of Elasticsearch host. Defaults to localhost:9200",default=None)
    parser.add_argument("-a","--aws",help="Using Amazon Web Services: requires boto3",action="store_true")
    parser.add_argument("-r","--aws_region",help="AWS region of Elasticsearch instance",default="us-east-2")
    parser.add_argument("index",help="Elasticsearch index to be used.")
    return parser


if __name__ == "__main__":
    es=None
    args = form_parser().parse_args()
    if args.aws:
        if not(args.aws and args.aws_region and args.host):
            print("Error:AWS region and AWS elasticsearch host needed with AWS flag")
            sys.exit(1)
        from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
        auth = BotoAWSRequestsAuth(aws_host=args.host,
                           aws_region=args.aws_region,
                           aws_service='es')
        es = Elasticsearch(host=args.host,
                          port=80,
                          connection_class=RequestsHttpConnection,
                          http_auth=auth,
                          timeout=30, max_retries=10, retry_on_timeout=True)
    elif args.host:
        es = Elasticsearch(args.host,timeout=30, max_retries=10, retry_on_timeout=True)
    else:
        es = Elasticsearch(timeout=30, max_retries=10, retry_on_timeout=True)
    print(helpers.bulk(es, form_query(args.index,es)))
