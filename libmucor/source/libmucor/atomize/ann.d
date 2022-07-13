module libmucor.atomize.ann;

import std.stdio;
import std.algorithm : splitter, map, count, canFind, countUntil;
import std.array : array, split;
import std.conv : to;
import std.range : enumerate, chunks;
import std.traits : ReturnType;
import std.typecons : No;
import std.range;

import dhtslib.vcf;
import htslib.hts_log;
import libmucor.vcfops;
import libmucor.jsonlops;
import libmucor.error;
import libmucor.option;
import libmucor.atomize.util;
import mir.ser;
import mir.ser.interfaces;

/// Structured VCF String field 
/// List of objects
struct Annotations {
    @serdeIgnoreOut
    /// slice of original string 
    string original;
    @serdeIgnoreOut
    /// range of individual annotations
    ReturnType!(getRange) annotations;

    this(string val) {
        // if types empty, all types are encoded as strings
        this.original = val;
        this.annotations = getRange;
    }

    /// helper function
    auto getRange(){
        return original.splitter(",");
    }

    /// get number of annotations
    auto length() {
        return getRange.count;
    }

    /// range functions
    auto front() {
        return Annotation(this.annotations.front);
    }

    /// range functions
    void popFront() {
        this.annotations.popFront;
    }

    /// range functions
    auto empty() {
        return this.annotations.empty;
    }

    auto opIndex(size_t i) {
        return Annotation(getRange.drop(i).front);
    }

    auto toString() {
        return this.original;
    }
}

enum Modifier {
    HIGH, 
    MODERATE, 
    LOW, 
    MODIFIER
}

enum Effect {
    chromosome_number_variation,
    exon_loss_variant,
    frameshift_variant,
    stop_gained,
    stop_lost,
    start_lost,
    splice_acceptor_variant,
    splice_donor_variant,
    rare_amino_acid_variant,
    missense_variant,
    disruptive_inframe_insertion,
    conservative_inframe_insertion,
    disruptive_inframe_deletion,
    conservative_inframe_deletion,
    @serdeKeys("5_prime_UTR_truncation")
    _5_prime_UTR_truncation,
    @serdeKeys("3_prime_UTR_truncation")
    _3_prime_UTR_truncation,
    exon_loss,
    splice_branch_variant,
    splice_region_variant,
    stop_retained_variant,
    initiator_codon_variant,
    synonymous_variant,
    non_canonical_start_codon,
    coding_sequence_variant,
    @serdeKeys("5_prime_UTR_variant")
    _5_prime_UTR_variant,
    @serdeKeys("3_prime_UTR_variant")
    _3_prime_UTR_variant,
    @serdeKeys("5_prime_UTR_premature_start_codon_gain_variant")
    _5_prime_UTR_premature_start_codon_gain_variant,
    upstream_gene_variant,
    downstream_gene_variant,
    TF_binding_site_variant,
    regulatory_region_variant,
    miRNA,
    custom,
    sequence_feature,
    conserved_intron_variant,
    intron_variant,
    intragenic_variant,
    conserved_intergenic_variant,
    intergenic_region,
    non_coding_exon_variant,
    nc_transcript_variant,
    gene_variant,
    chromosome,
    non_coding_transcript_exon_variant,
    TFBS_ablation,
    gene_fusion,
    feature_ablation,
    feature_fusion,
    transcript_ablation,
    non_coding_transcript_variant,
    duplication,
    bidirectional_gene_fusion,
}

struct Annotation {
    /// Allele (or ALT)
    string allele;

    /// Annotation (a.k.a. effect or consequence): Annotated using Sequence Ontology terms. Multiple effects can be concatenated using ‘&’.
    Effect[] effect;

    /// Putative_impact: A simple estimation of putative impact / deleteriousness : {HIGH, MODERATE, LOW, MODIFIER}
    Modifier impact;

    /// Gene Name: Common gene name (HGNC).
    Option!string gene_name;

    /// Gene ID: Gene ID
    Option!string gene_id;

    /// Feature type: Which type of feature is in the next field
    string feature_type;

    /// Feature ID: Depending on the annotation, this may be: Transcript ID, Motif ID, miRNA, ChipSeq peak, Histone mark, etc.
    string feature_id;

    /// Transcript biotype. The bare minimum is at least a description on whether the transcript is {“Coding”, “Noncoding”}. Whenever possible, use ENSEMBL biotypes.
    Option!string transcript_biotype;
    
    /// Rank / total : Exon or Intron rank / total number of exons or introns.
    Option!long rank; // not required
    Option!long rtotal; // not required
    
    /// HGVS.c: Variant using HGVS notation (DNA level)
    string hgvs_c;

    /// HGVS.p: If variant is coding, this field describes the variant using HGVS notation (Protein level).
    /// Since transcript ID is already mentioned in ‘feature ID’, it may be omitted here.
    Option!string hgvs_p; // not required

    /// cDNA_position: Position in cDNA (one based).
    Option!long cdna_position; // not required
    
    /// cDNA_len: trancript’s cDNA length.
    Option!long cdna_length; // not required

    /// CDS_position: Position of coding bases (one based includes
    /// START and STOP codons).
    Option!long cds_position; // not required

    /// CDS_len: number of coding bases.
    Option!long cds_length; // not required

    /// Protein_position: Position of AA (one based, including START, but not STOP).
    Option!long protein_position; // not required
    /// Protein_len: number of AA.
    Option!long protein_length; // not required
    /// Distance to feature: All items in this field are options, so the field could be empty.
    ///     Up/Downstream: Distance to first / last codon
    ///     Intergenic: Distance to closest gene
    ///     Distance to closest Intron boundary in exon (+/- up/downstream). If same, use positive number.
    ///     Distance to closest exon boundary in Intron (+/- up/downstream)
    ///     Distance to first base in MOTIF
    ///     Distance to first base in miRNA
    ///     Distance to exon-intron boundary in splice_site or splice _region
    ///     ChipSeq peak: Distance to summit (or peak center)
    ///     Histone mark / Histone state: Distance to summit (or peak center)
    Option!long distance_to_feature;

    /// Errors, Warnings or Information messages. Add errors, warnings or informative message that can 
    /// affect annotation accuracy. It can be added using either ‘codes’ (as shown in column 1, e.g. W1)
    /// or ‘message types’ (as shown in column 2, e.g.
    /// WARNING_REF_DOES_NOT_MATCH_GENOME). All these errors, warnings or information
    /// messages messages are optional.
    Option!string errors_warnings_info;

    this(string ann) {
        import std.algorithm : findSplit;

        auto vals = ann.findSplit("|");
        this.allele = vals[0];

        vals = vals[2].findSplit("|");
        this.effect = vals[0].splitter("&").map!(x => enumFromStr!Effect(x)).array;

        vals = vals[2].findSplit("|");
        this.impact = enumFromStr!Modifier(vals[0]);

        vals = vals[2].findSplit("|");
        if(vals[0] != "") {
            this.gene_name = Some(vals[0]);
        }

        vals = vals[2].findSplit("|");
        if(vals[0] != "") {
            this.gene_id = Some(vals[0]);
        }

        vals = vals[2].findSplit("|");
        this.feature_type = vals[0];

        vals = vals[2].findSplit("|");
        this.feature_id = vals[0];

        vals = vals[2].findSplit("|");
        if(vals[0] != "") {
            this.transcript_biotype = Some(vals[0]);
        }

        vals = vals[2].findSplit("|");
        if(vals[0] != "") {
            auto f = vals[0].split("/");
            this.rank = Some(f[0].to!long);
            if(f.length == 2) 
                this.rtotal = Some(f[1].to!long);
        }

        vals = vals[2].findSplit("|");
        this.hgvs_c = vals[0];

        vals = vals[2].findSplit("|");
        if(vals[0] != "")
            this.hgvs_p = Some(vals[0]);
        
        vals = vals[2].findSplit("|");
        if(vals[0] != "") {
            auto f = vals[0].split("/");
            this.cdna_position = Some(f[0].to!long);
            if(f.length == 2) 
                this.cdna_length = Some(f[1].to!long);
        }

        vals = vals[2].findSplit("|");
        if(vals[0] != "") {
            auto f = vals[0].split("/");
            this.cds_position = Some(f[0].to!long);
            if(f.length == 2) 
                this.cds_length = Some(f[1].to!long);
        }

        vals = vals[2].findSplit("|");
        if(vals[0] != "") {
            auto f = vals[0].split("/");
            this.protein_position = Some(f[0].to!long);
            if(f.length == 2) 
                this.protein_length = Some(f[1].to!long);
        }

        vals = vals[2].findSplit("|");
        if(vals[0] != "")
            this.distance_to_feature = Some(vals[0].to!long);

        vals = vals[2].findSplit("|");

        if(vals[0] != "")
            this.errors_warnings_info = Some(vals[0]);
    }

    void serialize(S)(ref S serializer) {
        auto s = serializer.structBegin;
        
        serializer.putCompiletimeKey!"allele";
        serializer.putSymbol(this.allele);

        serializer.putCompiletimeKey!"effect";
        serializeValue(serializer, effect);

        serializer.putCompiletimeKey!"impact";
        serializeValue(serializer, impact);

        
        if(!this.gene_name.isNone) {
            serializer.putCompiletimeKey!"gene_name";
            serializer.putSymbol(this.gene_name.unwrap);
        }

        if(!this.gene_id.isNone) {
            serializer.putCompiletimeKey!"gene_id";
            serializer.putSymbol(this.gene_id.unwrap);
        }

        serializer.putCompiletimeKey!"feature_type";
        serializer.putSymbol(this.feature_type);

        serializer.putCompiletimeKey!"feature_id";
        serializer.putValue(this.feature_id);

        if(!this.transcript_biotype.isNone) {
            serializer.putCompiletimeKey!"transcript_biotype";
            serializer.putSymbol(this.transcript_biotype.unwrap);
        }

        if(!this.rank.isNone) {
            serializer.putCompiletimeKey!"rank";
            serializer.putValue(this.rank.unwrap);
        }

        if(!this.rtotal.isNone) {
            serializer.putCompiletimeKey!"rtotal";
            serializer.putValue(this.rtotal.unwrap);
        }
        
        serializer.putCompiletimeKey!"hgvs_c";
        serializer.putValue(this.hgvs_c);

        if(!this.hgvs_p.isNone) {
            serializer.putCompiletimeKey!"hgvs_p";
            serializer.putValue(this.hgvs_p.unwrap);
        }

        if(!this.cdna_position.isNone) {
            serializer.putCompiletimeKey!"cdna_position";
            serializer.putValue(this.cdna_position.unwrap);
        }
        if(!this.cdna_length.isNone) {
            serializer.putCompiletimeKey!"cdna_length";
            serializer.putValue(this.cdna_length.unwrap);
        }
        
        if(!this.cds_position.isNone) {
            serializer.putCompiletimeKey!"cds_position";
            serializer.putValue(this.cds_position.unwrap);
        }
        if(!this.cds_length.isNone) {
            serializer.putCompiletimeKey!"cds_length";
            serializer.putValue(this.cds_length.unwrap);
        }

        if(!this.protein_position.isNone) {
            serializer.putCompiletimeKey!"protein_position";
            serializer.putValue(this.protein_position.unwrap);
        }
        if(!this.protein_length.isNone) {
            serializer.putCompiletimeKey!"protein_length";
            serializer.putValue(this.protein_length.unwrap);
        }

        if(!this.distance_to_feature.isNone) {
            serializer.putCompiletimeKey!"distance_to_feature";
            serializer.putValue(this.distance_to_feature.unwrap);
        }

        if(!this.errors_warnings_info.isNone) {
            serializer.putCompiletimeKey!"errors_warnings_info";
            serializer.putValue(this.errors_warnings_info.unwrap);
        }

        serializer.structEnd(s);
    }
    
}

unittest{
    string ann = "A|intron_variant|MODIFIER|PLCXD1|ENSG00000182378|Transcript|ENST00000381657|"~
                "protein_coding|1/6|ENST00000381657.2:c.-21-26C>A|||||,A|intron_variant|MODIFIER"~
                "|PLCXD1|ENSG00000182378|Transcript|ENST00000381663|protein_coding|1/7|ENST00000381663.3:c.-21-26C>A||"~
                "|||";
    auto anns = Annotations(ann);
    import std.stdio;
    import mir.ser.ion;
    import mir.ion.conv;
    auto parsed = anns.array;
    assert(serializeIon(parsed[0]).ion2text == `{allele:A,effect:[intron_variant],impact:MODIFIER,gene_name:PLCXD1,gene_id:ENSG00000182378,feature_type:Transcript,feature_id:"ENST00000381657",transcript_biotype:protein_coding,rank:1,rtotal:6,hgvs_c:"ENST00000381657.2:c.-21-26C>A"}`);
    assert(serializeIon(parsed[1]).ion2text == `{allele:A,effect:[intron_variant],impact:MODIFIER,gene_name:PLCXD1,gene_id:ENSG00000182378,feature_type:Transcript,feature_id:"ENST00000381663",transcript_biotype:protein_coding,rank:1,rtotal:7,hgvs_c:"ENST00000381663.3:c.-21-26C>A"}`);

    assert(serializeIon(Effect._5_prime_UTR_premature_start_codon_gain_variant).ion2text == "'5_prime_UTR_premature_start_codon_gain_variant'");
    
}
