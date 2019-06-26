# Using Elasticsearch as Datastore Alongside Mucor3

* [Introduction](#introduction)
* [Indexing](#indexing)
* [Query](#query)
* [AWS](#amazon-web-services-elasticsearch-instances)


### Introduction
Elasticsearch is a noSQL datastore that accepts documents or JSON objects as input into indexes. Using mucor3 along with an Elasticsearch instance has proved most useful for our lab as we have sequenced thousands of DNA samples for mutational analysis. We will not cover setting up your own Elasticsearch instance that information can be found [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/elasticsearch-intro.html). 

### Indexing
JSONL data is indexed into Elasticsearch under an index (used to separate documents into groups). The python script ```indexer.py``` can index jsonl from the vcf_atomizer into an Elasticsearch instance.

Install requirements (in addition to normal Mucor3 requirements):
```
pip install elasticsearch
pip install elasticsearch-dsl
pip install boto3 ##optional: required for AWS Elasticsearch instances

# Or via conda
conda install -c conda-forge elasticsearch
conda install -c conda-forge elasticsearch-dsl
conda install -c conda-forge boto3 ##optional: required for AWS Elasticsearch instances
```
**Note:** Your version of the elasticsearch python package must be [compatible](https://github.com/elastic/elasticsearch-py#compatibility) with your version of Elasticsearch.

#### Indexer Options
```
usage: indexer.py [-h] [-e HOST] [-a] [-r AWS_REGION] index

Ingest jsonl data into an Elasticsearch instance.

positional arguments:
  index                 Elasticsearch index to be used.

optional arguments:
  -h, --help            show this help message and exit
  -e HOST, --host HOST  ip address of Elasticsearch host. Defaults to
                        localhost:9200
  -a, --aws             Using Amazon Web Services: requires boto3
  -r AWS_REGION, --aws_region AWS_REGION
                        AWS region of Elasticsearch instance
```

The indexer reads jsonl from stdin and will store json objects in the specified index. If the index does not exist it will be created.
```
# we are using a locally hosted Elasticsearch
cat data.jsonl | python indexer.py myproject 
```

### Query
The query script uses the Elasticsearch [Query String](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html) syntax (also know as the Kibana Query Syntax).

#### Query Options
```
usage: query.py [-h] [-e HOST] [-a] [-r AWS_REGION] [-n NUMLINES]
                index doctype query

Query data from Elasticsearch using Query String Query Syntax

positional arguments:
  index                 Elasticsearch index to query.
  doctype               Elasticsearch doc type to query.
  query                 Elasticsearch query string to query index.

optional arguments:
  -h, --help            show this help message and exit
  -e HOST, --host HOST  ip address of Elasticsearch host. Defaults to
                        localhost:9200
  -a, --aws             Using Amazon Web Services: requires boto3
  -r AWS_REGION, --aws_region AWS_REGION
                        AWS region of Elasticsearch instance
  -n NUMLINES, --numlines NUMLINES
                        Number of result lines returned: default is all
```

The corresponding query from jq code block on the project readme can be achieved with Elasticseach:
```
// select only jsonl rows where protein change annotation (ANN_hgvs_p) from snpeff
// is not null and the variant allele frequency is > 0.01
python query.py myproject variant_vcf 'AF:> 0.01 AND _exists_:ANN_hgvs_p' > query.jsonl
```
```variant_vcf``` is a doctype added by the vcf_atomizer that labels the ```type``` field of the resulting json so it can be identified easily as json that has resulted from the vcf_atomizer. This makes querying elasticsearch easier in the case that other json data has been loded into the same Elasticsearch index that is not from the vcf_atomizer.

The results from this query can be directly given to mucor3:
```
mucor3 query.jsonl output_dir
```

### Amazon Web Services Elasticsearch Instances
The python scripts also allow usage with AWS Elasticsearch Instances. This functionality is provided through the boto3 package. If you are familiar with the AWS CLI, boto3 uses your existing AWS credentials (setup information found [here](https://docs.aws.amazon.com/polly/latest/dg/setup-aws-cli.html)). You must provide your AWS Elasticsearch host and AWS region.
```
cat data.jsonl | python indexer.py -a -e host -r us-east-1 myproject 
```
