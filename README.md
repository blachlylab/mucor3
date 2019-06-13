# Mucor3
Mucor3 an iteration on the original Mucor [link]. Mucor3 translates VCF files into tabular tsv data, including pivoted data.

### Installation
```
conda install -c bioconda mucor3
```
or if you wish to build mucor3: requires go and dlang
```
git clone ...
cd vcf_atomizer
make
cd ..
cd depthGauge
dub build --build release
cd ..
python setup.py install
```

### Run Mucor3
VCFs must be atomized into line-delimited json (jsonl link). The vcf_atomizer [link] can take a vcf or gzipped vcf file and convert it to jsonl. Currently only GATK VCF files work with the vcf_atomizer. After combining all jsonl into one file Mucor3 can convert it to a tabular format and generate pivoted tables. depthgauge [link] serves to create a pivoted spreadsheet that shows the read depth at all positions in the pivoted AF table.
```
vcf_atomizer sample1.vcf >sample1.jsonl
vcf_atomizer sample2.vcf >sample2.jsonl
...
cat sample1.jsonl sample2.jsonl ... > data.jsonl
mucor3 data.jsonl output
depthgauge -t 4 output/AF.tsv 11 BAMS/ depthgauge.tsv
```