#### Running Mucor3
Provide Mucor3 with your combined data and an output folder.
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

| CHROM | POS  | REF | ALT    |   AF    | sample  | ANN_gene_name | ANN_hgvs_p | ANN_effect |
|-------|------|-----|--------|---------|---------|---------------|------------|------------|
| chr1  | 2    | G   | T      | 0.7     | sample2 | foo           | p.Met1Ala  | missense   |
| chr1  | 5    | C   | T      | 1       | sample1 | foo           | ...        | synonymous |
| chr1  | 5    | C   | T      | 0.25    | sample2 | foo           | ...        | ...        |
| chr1  | 1000 | TA  | T      | 0.45    | sample1 | bar           | ...        | ...        |
| chr1  | 3000 | G   | GATAGC | 0.01    | sample1 | oncogene      | ...        | ...        |

**Note:** The ANN_ fields will not be present for VCFs that have not been annotated using SnpEff.