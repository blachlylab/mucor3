#### Filter Data (optional)
The bioconda package includes [jq](https://github.com/stedolan/jq) as an option for simple filtering of JSONL variant data while combining:
```
// select only jsonl rows where protein change annotation (ANN_hgvs_p) from snpeff
// is not null and the variant allele frequency is > 0.01
cat *.jsonl | jq -c 'select(.ANN_hgvs_p!=null and .AF > 0.01)' > data.jsonl
```
You can read more about jq syntax [here](https://stedolan.github.io/jq/).


**Note:** If more extensive filtering is needed, Mucor3 should be flexible with any noSQL datastore that accepts 
JSONL input and outputs JSONL output. i.e. [Elasticsearch](https://www.elastic.co/), [Apache Drill](https://drill.apache.org/), or [couchDB](http://couchdb.apache.org/). Example python scripts for Elasticsearch are availiable in the examples folder. These scripts are not included currently in the bioconda package.