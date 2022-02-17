varquery
========

### Query Syntax
Varquery can technically query any JSON data which it has indexed. Though it is designed with VCF atomized JSON in mind. `/` is used as a kind of depth-based delimiter. i.e
```
{"key1":"A", "key2":{"key3":"B"}}
/key1 is querying the JSON root object for key1
/key2/key3 is querying the JSON "key2" object for key3
```
Generally you can query data like so:
```
// basic equals queries with operators
/key = val
/key1 = val1 AND /key2 = val2
/key1 = val1 OR /key2 = val2 
(/key1 = val1 OR /key2 = val2) AND /key3 = val3
NOT ((/key1 = val1 OR /key2 = val2) AND /key3 = val3)

// query multipe values with operators
/key1 = ( val1 OR val2) AND /key3 = val3

// Integer Numeric queries
/key1 == 1  
/key1 = 1:3 
/key1 > 1   
/key1 >= 1  
/key1 < 1   
/key1 <= 1  

// float Numeric queries
/key1 == 1f   
/key1 = 1f:3f 
/key1 > 1f    
/key1 >= 1f   
/key1 < 1f    
/key1 <= 1f   
```
Of note for VCF fields, since they are record oriented but have sub fields we represent this as such:
```
/INFO/AF > 0.5f
/FORMAT/AF > 0.5f
/FORMAT/GT = ./.
```
The `atomize_vcf` program supports the parsing of annotation fields which in VCF are often pipe-delimited (`|`). This allows the ability to search/filter  records based on annotation information. Currently we only support SnpEff's ANN field. This will change.
```
```
Since an ANN field can have multiple annotations this data is atomized into an array of objects in JSON.
```
This VCF field (not a real annotation, it has been modified for this example)                                                            | Second annotation starts here
ANN = A|intron_variant|MODIFIER|PLCXD1|ENSG00000182378|Transcript|ENST00000381657|protein_coding||1/6|ENST00000381657.2:c.-21-26C>A|||||,A|missense_variant|MODIFIER|PLCXD1|ENSG00000182378|Transcript|ENST00000381663|protein_coding||1/7|ENST00000381663.3:c.-21-26C>A|||||

becomes this JSON, blank fields are not reported
"ANN" : [{"allele" : "A", "effect" : "intron_variant", ...},{"allele" : "A", "effect" : "missense_variant", ...}]

// SNPEFF ANN field names
"allele",
"effect",
"impact",
"gene_name",
"gene_id",
"feature_type",
"feature_id",
"transcript_biotype",
"rank_total",
"hgvs_c",
"hgvs_p",
"cdna_position",
"cds_position",
"protein_position",
"distance_to_feature",
"errors_warnings_info"
```

### Current limitations
Keys and values cannot have any whitespace. This functionality will be added.