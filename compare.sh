#!/bin/bash

bin/mucor3_atomize_vcf $1 > a.all.jsonl
bin/mucor3_atomize_vcf $2 > b.all.jsonl

# get uniq variants
cat a.all.jsonl | jq -c "{CHROM, POS, REF, ALT}" | sort | uniq > a.var.jsonl
cat b.all.jsonl | jq -c "{CHROM, POS, REF, ALT}" | sort | uniq > b.var.jsonl

comm -23 a.var.jsonl b.var.jsonl > var.lost.jsonl
comm -13 a.var.jsonl b.var.jsonl > var.gained.jsonl
comm -12 a.var.jsonl b.var.jsonl > var.retained.jsonl

# get uniq sample variants
bin/mucor3_atomize_vcf $1 | jq -c "{CHROM, POS, REF, ALT, sample}" | sort | uniq > a.sample.var.jsonl
bin/mucor3_atomize_vcf $2 | jq -c "{CHROM, POS, REF, ALT, sample}" | sort | uniq > b.sample.var.jsonl

comm -23 a.sample.var.jsonl b.sample.var.jsonl > sample.var.lost.jsonl
comm -13 a.sample.var.jsonl b.sample.var.jsonl > sample.var.gained.jsonl
comm -12 a.sample.var.jsonl b.sample.var.jsonl > sample.var.retained.jsonl

bin/mucor3_varquery index a.all.jsonl a.all.index 
bin/mucor3_varquery index b.all.jsonl b.all.index

bin/mucor3_varquery query a.all.jsonl a.all.index $3 > a.filtered.jsonl
bin/mucor3_varquery query b.all.jsonl b.all.index $3 > b.filtered.jsonl


# get uniq sample variants
cat a.filtered.jsonl | jq -c "{CHROM, POS, REF, ALT, sample}" | sort | uniq > a.filtered.sample.var.jsonl
cat b.filtered.jsonl | jq -c "{CHROM, POS, REF, ALT, sample}" | sort | uniq > b.filtered.sample.var.jsonl

comm -23 a.sample.var.jsonl b.sample.var.jsonl > filtered.sample.var.lost.jsonl
comm -13 a.sample.var.jsonl b.sample.var.jsonl > filtered.sample.var.gained.jsonl
comm -12 a.sample.var.jsonl b.sample.var.jsonl > filtered.sample.var.retained.jsonl

