# Mucor3
Mucor3 an iteration on the original [Mucor](https://github.com/blachlylab/mucor). Mucor3 translates [VCF](https://samtools.github.io/hts-specs/VCFv4.2.pdf) files into tabular tsv data, including pivoted data.

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
VCFs must be atomized into line-delimited [json](http://jsonlines.org/). The [vcf_atomizer](https://github.com/blachlylab/vcf_atomizer) can take a vcf or gzipped vcf file and convert it to jsonl. Currently only GATK VCF files work with the vcf_atomizer. After combining all jsonl into one file Mucor3 can convert it to a tabular format and generate pivoted tables. [depthgauge](https://github.com/blachlylab/depthGauge) serves to create a pivoted spreadsheet that shows the read depth at all positions in the pivoted AF table.

Mucor3 is intended to be run on GATK VCFs that have undergone annotation with SnpEff, however, Mucor3 and the vcf_atomizer should work for most VCFs that are properly formatted. If you find that we couldn't atomize your particular VCF please open an issue either here or on the vcf_atomizer github.

#### Atomize VCFs
```
vcf_atomizer sample1.vcf.gz >sample1.jsonl
vcf_atomizer sample2.vcf >sample2.jsonl
```
#### Combine jsonl data
```
cat sample1.jsonl sample2.jsonl ... > data.jsonl
```
#### OR Filter Data
```
// select only jsonl rows where protein change annotation (ANN_hgvs_p) from snpeff
// is not null and the variant allele frequency is > 0.01
cat *.jsonl | jq -c 'select(.ANN_hgvs_p!=null and .AF > 0.01)' > data.jsonl
```

#### Mucor
Provide mucor3 with your combined data and an output folder.
```
mucor3 data.jsonl output_folder
```
Mucor3 will output a pivoted table that is every variant pivoted 
by sample and should have this general format:

| CHROM | POS  | REF | ALT    | ANN_gene_name | ANN_effect | sample1 | sample2 |
|-------|------|-----|--------|---------------|------------|---------|---------|
| chr1  | 2    | G   | T      | foo           | missense   | .       | 0.7     |
| chr1  | 5    | C   | T      | foo           | synonymous | 1       | 0.25    |
| chr1  | 1000 | TA  | T      | bar           | ...        | 0.45    | .       |
| chr1  | 3000 | G   | GATAGC | oncogene      | ...        | 0.01    | .       |

The values under sample1 and sample2 are the values from the AF field of FORMAT region of the VCF.

The master table however would represent this same data in
this format:

| CHROM | POS  | REF | ALT    | sample1 | sample  | ANN_gene_name | ANN_hgvs_p | ANN_effect |
|-------|------|-----|--------|---------|---------|---------------|------------|------------|
| chr1  | 2    | G   | T      | 0.7     | sample2 | foo           | p.Met1Ala  | missense   |
| chr1  | 5    | C   | T      | 1       | sample1 | foo           | ...        | synonymous |
| chr1  | 5    | C   | T      | 0.25    | sample2 | foo           | ...        | ...        |
| chr1  | 1000 | TA  | T      | 0.45    | sample1 | bar           | ...        | ...        |
| chr1  | 3000 | G   | GATAGC | 0.01    | sample1 | oncogene      | ...        | ...        |

**Note** The ANN_ fields will not be present for VCFs that have not been annotated using SnpEff.

#### DepthGauge
Before running depthgauge we need to know what the first sample name column is in our AF.tsv spreadsheet.
In the above data the column number is 7 for column sample1. We also provide a folder which contains the BAM files 
needed to run depthgauge. **Important**: The bams must have the same name as sample in the spreadsheet and VCFs and must be sorted and indexed.
For our example we would expect BAMs to contain ```sample1.bam, sample2.bam, sample1.bam.bai, and sample2.bam.bai```.
```
depthgauge -t 4 output_folder/AF.tsv 7 BAMS/ depthgauge.tsv
```
This will create an identical table to our first except with read depths instead of allele frequencies.

| CHROM | POS  | REF | ALT    | ANN_gene_name | ANN_effect | sample1 | sample2 |
|-------|------|-----|--------|---------------|------------|---------|---------|
| chr1  | 2    | G   | T      | foo           | missense   | 10      | 37      |
| chr1  | 5    | C   | T      | foo           | synonymous | 100     | 4       |
| chr1  | 1000 | TA  | T      | bar           | ...        | 20      | 45      |
| chr1  | 3000 | G   | GATAGC | oncogene      | ...        | 300     | 78      |