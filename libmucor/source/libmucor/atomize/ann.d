module libmucor.atomize.ann;

import option;
import libmucor.atomize.util;
import libmucor.serde;
import libmucor.utility;
import memory;

import std.array : array;
import std.range;
import std.container.array;

import mir.ser;
import mir.parse : fromString;


/// Structured VCF String field 
/// List of objects
struct Annotations
{
    /// slice of original string 
    const(char)[] original;
    /// range of individual annotations
    const(char)[] frontVal;
    const(char)[] other;
    bool empty;
    
    @safe @nogc nothrow pragma(inline, true):
    this(const(char)[] val)
    {
        // if types empty, all types are encoded as strings
        this.original = val;
        this.other = this.original;
        this.popFront;
    }

    /// range functions
    auto front()
    {
        return Annotation(this.frontVal);
    }

    /// range functions
    void popFront()
    {
        if(other == "") {
            this.empty = true;
            return;
        }
        auto v = this.other.findSplit(',');
        if(v == ["", ""]) {
            this.frontVal = this.other;
            this.other = "";
        } else {
            this.frontVal = v[0];
            this.other = v[1];
        }
    }

    auto opIndex(size_t i)
    {
        return Annotations(original).drop(i).front;
    }

    auto toString()
    {
        return this.original;
    }
}

enum Modifier
{
    HIGH,
    MODERATE,
    LOW,
    MODIFIER
}

enum Effect
{
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
    @serdeKeys("5_prime_UTR_truncation") _5_prime_UTR_truncation,
    @serdeKeys("3_prime_UTR_truncation") _3_prime_UTR_truncation,
    exon_loss,
    splice_branch_variant,
    splice_region_variant,
    stop_retained_variant,
    initiator_codon_variant,
    synonymous_variant,
    non_canonical_start_codon,
    coding_sequence_variant,
    @serdeKeys("5_prime_UTR_variant") _5_prime_UTR_variant,
    @serdeKeys("3_prime_UTR_variant") _3_prime_UTR_variant,
    @serdeKeys("5_prime_UTR_premature_start_codon_gain_variant") _5_prime_UTR_premature_start_codon_gain_variant,
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
    structural_interaction_variant,
    protein_protein_contact,
    start_retained_variant,
    exon_region,
}

/// This enum represents the transcript_biotype field of the ANN spec
/// Some are specific to the ANN spec but most follow the
/// Gene/Transcript Biotypes in GENCODE & Ensembl
/// 
/// https://www.gencodegenes.org/pages/biotypes.html
enum TranscriptBiotype {
    /// Immunoglobulin (Ig) variable chain and T-cell receptor (TcR) genes imported or annotated according to the IMGT.
    IG_C_gene,
    IG_D_gene,
    IG_J_gene,
    IG_LV_gene,
    IG_V_gene,
    TR_C_gene,
    TR_J_gene,
    TR_V_gene,
    TR_D_gene,
    /// Inactivated immunoglobulin gene.
    IG_pseudogene,
    IG_C_pseudogene,
    IG_J_pseudogene,
    IG_V_pseudogene,
    TR_V_pseudogene,
    TR_J_pseudogene,
    /// Non-coding RNA predicted using sequences from Rfam and miRBase
    Mt_rRNA,
    Mt_tRNA,
    miRNA,
    misc_RNA,
    rRNA,
    scRNA,
    snRNA,
    snoRNA,
    ribozyme,
    sRNA,
    scaRNA,
    /// Generic long non-coding RNA biotype that replaced the following biotypes: 3prime_overlapping_ncRNA, antisense, bidirectional_promoter_lncRNA, lincRNA, macro_lncRNA, non_coding, processed_transcript, sense_intronic and sense_overlapping.
    lncRNA,
    /// Non-coding RNA predicted to be pseudogene by the Ensembl pipeline
    Mt_tRNA_pseudogene,
    tRNA_pseudogene,
    snoRNA_pseudogene,
    snRNA_pseudogene,
    scRNA_pseudogene,
    rRNA_pseudogene,
    misc_RNA_pseudogene,
    miRNA_pseudogene,
    /// To be Experimentally Confirmed. This is used for non-spliced EST clusters that have polyA features. This category has been specifically created for the ENCODE project to highlight regions that could indicate the presence of protein coding genes that require experimental validation, either by 5' RACE or RT-PCR to extend the transcripts, or by confirming expression of the putatively-encoded peptide with specific antibodies.
    /// nonsense_mediated_decay	If the coding sequence (following the appropriate reference) of a transcript finishes >50bp from a downstream splice site then it is tagged as NMD. If the variant does not cover the full reference coding sequence then it is annotated as NMD if NMD is unavoidable i.e. no matter what the exon structure of the missing portion is the transcript will be subject to NMD.
    TEC,
    /// Transcript that has polyA features (including signal) without a prior stop codon in the CDS, i.e. a non-genomic polyA tail attached directly to the CDS without 3' UTR. These transcripts are subject to degradation.
    non_stop_decay,
    /// Alternatively spliced transcript believed to contain intronic sequence relative to other, coding, variants.
    retained_intron,
    /// Contains an open reading frame (ORF).
    protein_coding,
    /// Not translated in the reference genome owing to a SNP/DIP but in other individuals/haplotypes/strains the transcript is translated. Replaces the polymorphic_pseudogene transcript biotype.
    protein_coding_LoF,
    /// Doesn't contain an ORF.
    processed_transcript,
    /// Transcript which is known from the literature to not be protein coding.
    non_coding,
    /// Transcript believed to be protein coding, but with more than one possible open reading frame.
    ambiguous_orf,
    /// Long non-coding transcript in introns of a coding gene that does not overlap any exons.
    sense_intronic,
    /// Long non-coding transcript that contains a coding gene in its intron on the same strand.
    sense_overlapping,
    /// Has transcripts that overlap the genomic span (i.e. exon or introns) of a protein-coding locus on the opposite strand.
    antisense,
    antisense_RNA,
    known_ncrna,
    /// Have homology to proteins but generally suffer from a disrupted coding sequence and an active homologous gene can be found at another locus. Sometimes these entries have an intact coding sequence or an open but truncated ORF, in which case there is other evidence used (for example genomic polyA stretches at the 3' end) to classify them as a pseudogene. Can be further classified as one of the following.
    pseudogene,
    /// Pseudogene that lack introns and is thought to arise from reverse transcription of mRNA followed by reinsertion of DNA into the genome.
    processed_pseudogene,
    /// Pseudogene owing to a SNP/DIP but in other individuals/haplotypes/strains the gene is translated.
    polymorphic_pseudogene,
    /// Pseudogene owing to a reverse transcribed and re-inserted sequence.
    retrotransposed,
    /// Pseudogene where protein homology or genomic structure indicates a pseudogene, but the presence of locus-specific transcripts indicates expression.
    transcribed_processed_pseudogene,
    transcribed_unprocessed_pseudogene,
    transcribed_unitary_pseudogene,
    /// Pseudogene that has mass spec data suggesting that it is also translated.
    translated_processed_pseudogene,
    translated_unprocessed_pseudogene,
    /// A species-specific unprocessed pseudogene without a parent gene, as it has an active orthologue in another species.
    unitary_pseudogene,
    /// Pseudogene that can contain introns since produced by gene duplication.
    unprocessed_pseudogene,
    /// Annotated on an artifactual region of the genome assembly.
    artifact,
    /// Long, intervening noncoding (linc) RNA that can be found in evolutionarily conserved, intergenic regions.
    lincRNA,
    /// Unspliced lncRNA that is several kb in size.
    macro_lncRNA,
    /// Transcript where ditag and/or published experimental data strongly supports the existence of short non-coding transcripts transcribed from the 3'UTR.
    @serdeKeys("3prime_overlapping_ncRNA") _3prime_overlapping_ncRNA,
    /// Otherwise viable coding region omitted from this alternatively spliced transcript because the splice variation affects a region coding for a protein domain.
    disrupted_domain,
    /// Short non coding RNA gene that forms part of the vault ribonucleoprotein complex.
    vaultRNA,
    vault_RNA,
    /// A non-coding locus that originates from within the promoter region of a protein-coding gene, with transcription proceeding in the opposite direction on the other strand.
    bidirectional_promoter_lncRNA,

    /// Bare minimum as required by ANN spec
    Coding,
    Noncoding,
    nonsense_mediated_decay,
    prime3_overlapping_ncrna,
}

enum AnnErrWarnInfo {
    /// Chromosome does not exists in reference genome database. Typically
    /// indicates a mismatch between the chromosome names in the input file
    /// and the chromosome names used in the reference genome.
    E1, ERROR_CHROMOSOME_NOT_FOUND,
    /// The variant’s genomic coordinate is greater than chromosome's length.
    E2, ERROR_OUT_OF_CHROMOSOME_RANGE,
    /// This means that the ‘REF’ field in the input VCF file does not match
    /// the reference genome. This warning may indicate a conflict between
    /// input data and data from reference genome (for instance is the input
    /// VCF was aligned to a different reference genome).
    W1, WARNING_REF_DOES_NOT_MATCH_GENOME,
    /// Reference sequence is not available, thus no inference could be
    /// performed.
    W2, WARNING_SEQUENCE_NOT_AVAILABLE,
    /// A protein coding transcript having a non-multiple of 3 length. It
    /// indicates that the reference genome has missing information about this
    /// particular transcript.
    W3, WARNING_TRANSCRIPT_INCOMPLETE,
    /// A protein coding transcript has two or more STOP codons in the middle
    /// of the coding sequence (CDS). This should not happen and it usually
    /// means the reference genome may have an error in this transcript.
    W4, WARNING_TRANSCRIPT_MULTIPLE_STOP_CODONS,
    /// A protein coding transcript does not have a proper START codon. It is
    /// rare that a real transcript does not have a START codon, so this
    /// probably indicates an error or missing information in the reference
    /// genome.
    W5, WARNING_TRANSCRIPT_NO_START_CODON,
    /// A protein coding transcript does not have a proper STOP codon. It is
    /// rare that a real transcript does not have a STOP codon, so this probably
    /// indicates an error or missing information in the reference genome.
    W6, WARNING_TRANSCRIPT_NO_STOP_CODON,
    /// Variant has been realigned to the most 3-prime position within the
    /// transcript. This is usually done to to comply with HGVS specification
    /// to always report the most 3-prime annotation.
    I1, INFO_REALIGN_3_PRIME,
    /// This effect is a result of combining more than one variants (e.g. two
    /// consecutive SNPs that conform an MNP, or two consecutive
    /// frame_shift variants that compensate frame).
    I2, INFO_COMPOUND_ANNOTATION,
    /// An alternative reference sequence was used to calculate this annotation
    /// (e.g. cancer sample comparing somatic vs. germline).
    I3, INFO_NON_REFERENCE_ANNOTATION
    
}

// Calculated from values in our vcfs
// TODO: Need a way to turn this into managable enum(s)
//       Will likely also need to allow string non-match
// 
// enum FeatureType {
//     "active-site:curator_inference_used_in_manual_assertion"
//     "active-site:Glycyl_thioester_intermediate"
//     "active-site:Nucleophile"
//     "active-site:Phosphocysteine_intermediate"
//     "active-site:Proton_acceptor"
//     "active-site:Proton_donor"
//     "active-site:sequence_similarity_evidence_used_in_manual_assertion"
//     "active-site:Tele-phosphohistidine_intermediate"
//     "antibody-mapping:heterologous_protein_expression_evidence"
//     "beta-strand:combinatorial_evidence_used_in_manual_assertion"
//     "BHLHE40"
//     "binding-site:2-oxoglutarate"
//     "binding-site:Allosteric_activator_fructose_2_6-bisphosphate"
//     "binding-site:ATP"
//     "binding-site:Chloride"
//     "binding-site:DNA"
//     "binding-site:FAD"
//     "binding-site:FMN"
//     "binding-site:Glutathione"
//     "binding-site:GTP"
//     "binding-site:Inhibitor"
//     "binding-site:Inositol_hexakisphosphate"
//     "binding-site:N-acetyl-D-glucosamine"
//     "binding-site:NADP"
//     "binding-site:Phosphate"
//     "binding-site:Poly-ADP-ribose"
//     "binding-site:S-adenosyl-L-homocysteine"
//     "binding-site:S-adenosyl-L-methionine"
//     "binding-site:Substrate"
//     "calcium-binding-region:1"
//     "Cfos"
//     "Cjun"
//     "cleavage-site:Cleavage"
//     "Cmyc"
//     "coiled-coil-region:match_to_sequence_model_evidence_used_in_manual_assertion"
//     "compositionally-biased-region:Ala-rich"
//     "compositionally-biased-region:Asp/Glu-rich_(acidic)"
//     "compositionally-biased-region:Cys-rich"
//     "compositionally-biased-region:Glu-rich"
//     "compositionally-biased-region:Gly/Pro-rich"
//     "compositionally-biased-region:Poly-Ala"
//     "compositionally-biased-region:Poly-Arg"
//     "compositionally-biased-region:Poly-Gln"
//     "compositionally-biased-region:Poly-Glu"
//     "compositionally-biased-region:Poly-Gly"
//     "compositionally-biased-region:Poly-Ser"
//     "compositionally-biased-region:Ser-rich"
//     "compositionally-biased-region:Ser/Thr-rich"
//     "cross-link:Glycyl_lysine_isopeptide_(Lys-Gly)_(interchain_with_G-Cter_in_ISG15)"
//     "cross-link:Glycyl_lysine_isopeptide_(Lys-Gly)_(interchain_with_G-Cter_in_SUMO2)"
//     "cross-link:Glycyl_lysine_isopeptide_(Lys-Gly)_(interchain_with_G-Cter_in_ubiquitin)"
//     "CTCF"
//     "CTCFL"
//     "disulfide-bond:experimental_evidence_used_in_manual_assertion"
//     "disulfide-bond:Interchain"
//     "disulfide-bond:match_to_sequence_model_evidence_used_in_manual_assertion"
//     "disulfide-bond:sequence_similarity_evidence_used_in_manual_assertion"
//     "dna-binding-region:match_to_sequence_model_evidence_used_in_manual_assertion"
//     "domain:Actin-binding"
//     "domain:Alpha-type_protein_kinase"
//     "domain:B30.2/SPRY"
//     "domain:BEN"
//     "domain:BTB"
//     "domain:C2_2"
//     "domain:CAP-Gly"
//     "domain:CARD"
//     "domain:CH_1"
//     "domain:Collagen-like"
//     "domain:Collagen-like_1"
//     "domain:CS"
//     "domain:CTCK"
//     "domain:C-type_lectin"
//     "domain:Death"
//     "domain:DHR-1"
//     "domain:EF-hand_1"
//     "domain:EF-hand_2"
//     "domain:EGF-like"
//     "domain:EGF-like_1"
//     "domain:EGF-like_10"
//     "domain:EGF-like_11"
//     "domain:EGF-like_12"
//     "domain:EGF-like_13"
//     "domain:EGF-like_14"
//     "domain:EGF-like_15"
//     "domain:EGF-like_16"
//     "domain:EGF-like_18"
//     "domain:EGF-like_19"
//     "domain:EGF-like_2"
//     "domain:EGF-like_22"
//     "domain:EGF-like_23"
//     "domain:EGF-like_24"
//     "domain:EGF-like_25"
//     "domain:EGF-like_26"
//     "domain:EGF-like_28"
//     "domain:EGF-like_3"
//     "domain:EGF-like_31"
//     "domain:EGF-like_35"
//     "domain:EGF-like_36"
//     "domain:EGF-like_37"
//     "domain:EGF-like_4"
//     "domain:EGF-like_40"
//     "domain:EGF-like_41"
//     "domain:EGF-like_42"
//     "domain:EGF-like_44"
//     "domain:EGF-like_46"
//     "domain:EGF-like_5"
//     "domain:EGF-like_6"
//     "domain:EGF-like_7"
//     "domain:EGF-like_8"
//     "domain:EGF-like_9"
//     "domain:ELM2"
//     "domain:EMI"
//     "domain:F-box"
//     "domain:Fibronectin_type-I_11"
//     "domain:Fibronectin_type-III"
//     "domain:Fibronectin_type-III_1"
//     "domain:Fibronectin_type-III_2"
//     "domain:Fibronectin_type-III_3"
//     "domain:Fibronectin_type-III_4"
//     "domain:Fibronectin_type-III_5"
//     "domain:Fibronectin_type-III_6"
//     "domain:Fibronectin_type-III_7"
//     "domain:Fibronectin_type-III_8"
//     "domain:Fibronectin_type-III_9"
//     "domain:GAE"
//     "domain:Guanylate_cyclase_1"
//     "domain:Helicase_ATP-binding"
//     "domain:Ig-like_2"
//     "domain:Ig-like_C2-type"
//     "domain:Ig-like_C2-type_2"
//     "domain:Ig-like_V-type"
//     "domain:Ig-like_V-type_5"
//     "domain:IQ"
//     "domain:Josephin"
//     "domain:Kringle"
//     "domain:Laminin_G-like"
//     "domain:Laminin_G-like_1"
//     "domain:LCCL"
//     "domain:LDL-receptor_class_A_1"
//     "domain:LDL-receptor_class_A_2"
//     "domain:LDL-receptor_class_A_3"
//     "domain:LDL-receptor_class_A_5"
//     "domain:LDL-receptor_class_A_6"
//     "domain:LIM_zinc-binding_1"
//     "domain:Link_2"
//     "domain:MAM"
//     "domain:MAM_4"
//     "domain:NBPF_1"
//     "domain:NBPF_2"
//     "domain:NBPF_3"
//     "domain:NBPF_4"
//     "domain:NBPF_5"
//     "domain:Nudix_hydrolase"
//     "domain:Olfactomedin-like"
//     "domain:Paired"
//     "domain:PDZ_7"
//     "domain:Peptidase_S1"
//     "domain:PH"
//     "domain:PLAC"
//     "domain:PSI"
//     "domain:P-type"
//     "domain:Rab-GAP_TBC"
//     "domain:RanBD1"
//     "domain:Reticulon"
//     "domain:RRM_1"
//     "domain:RRM_3"
//     "domain:SANT"
//     "domain:SEA"
//     "domain:SH2"
//     "domain:SH3"
//     "domain:SOCS_box"
//     "domain:SRCR"
//     "domain:SUN"
//     "domain:Sushi"
//     "domain:Sushi_1"
//     "domain:Sushi_2"
//     "domain:Sushi_3"
//     "domain:Sushi_5"
//     "domain:Sushi_6"
//     "domain:Sushi_7"
//     "domain:Thioredoxin_2"
//     "domain:Thyroglobulin_type-1"
//     "domain:Thyroglobulin_type-1_2"
//     "domain:TSP_type-1_3"
//     "domain:UBA"
//     "domain:v-SNARE_coiled-coil_homology"
//     "domain:VWFA_1"
//     "domain:VWFC_3"
//     "domain:WH1"
//     "domain:WSC"
//     "domain:WW"
//     "domain:WW_2"
//     "E2F4"
//     "E2F6"
//     "EBF1"
//     "EcR::usp"
//     "Egr1"
//     "ELF1"
//     "ETS1"
//     "FOSL1"
//     "FOSL2"
//     "FOXA1"
//     "Gabp"
//     "gene_variant"
//     "glycosylation-site:N-linked_(Glc)_(glycation)"
//     "glycosylation-site:N-linked_(GlcNAc...)"
//     "glycosylation-site:O-linked_(GalNAc...)"
//     "helix:combinatorial_evidence_used_in_manual_assertion"
//     "HNF4A"
//     "HNF4G"
//     "initiator-methionine:Removed"
//     "interacting-region:Interaction_with_DNMT3B"
//     "interacting-region:Interaction_with_PPARG"
//     "interaction"
//     "intergenic_region"
//     "Junb"
//     "Jund"
//     "JUN::FOS"
//     "lipidation-site:GPI-anchor_amidated_aspartate"
//     "Max"
//     "MEF2A"
//     "metal-binding-site:Calcium"
//     "metal-binding-site:Calcium_1"
//     "metal-binding-site:Calcium_2"
//     "metal-binding-site:Divalent_metal_cation_2"
//     "metal-binding-site:Iron-sulfur_1_(4Fe-4S-S-AdoMet)"
//     "metal-binding-site:Iron-sulfur_(2Fe-2S)"
//     "metal-binding-site:Magnesium"
//     "metal-binding-site:Magnesium_1"
//     "metal-binding-site:Potassium"
//     "metal-binding-site:Zinc"
//     "metal-binding-site:Zinc_2"
//     "metal-binding-site:Zinc_3"
//     "miscellaneous-region:12_X_approximate_tandem_repeats"
//     "miscellaneous-region:16_X_approximate_tandem_repeats"
//     "miscellaneous-region:3_X_8_AA_tandem_repeats_of_P-G-P-G-P-G-P-S"
//     "miscellaneous-region:5_X_2_AA_tandem_repeats_of_P-G"
//     "miscellaneous-region:7-methylguanosine-containing_mRNA_cap_binding"
//     "miscellaneous-region:Alpha-1"
//     "miscellaneous-region:Alpha-3"
//     "miscellaneous-region:Axin-binding"
//     "miscellaneous-region:Beta-propeller_3"
//     "miscellaneous-region:Binds_to_FBLN1"
//     "miscellaneous-region:B-linker"
//     "miscellaneous-region:DNA/RNA_binding"
//     "miscellaneous-region:G1"
//     "miscellaneous-region:GAG-alpha_(glucosaminoglycan_attachment_domain)"
//     "miscellaneous-region:Globular_or_compact_configuration_stabilized_by_disulfide_bonds"
//     "miscellaneous-region:Glutathione_binding"
//     "miscellaneous-region:GMP_binding"
//     "miscellaneous-region:Guanine-nucleotide_binding_in_RNA_target"
//     "miscellaneous-region:Histone_deacetylase"
//     "miscellaneous-region:Important_for_dimerization_and_interaction_with_PSMF1"
//     "miscellaneous-region:Involved_in_chromatin-binding"
//     "miscellaneous-region:Ligand-binding"
//     "miscellaneous-region:MHC_class_I_alpha-1_like"
//     "miscellaneous-region:Necessary_for_interaction_with_TCEA1_and_transactivation_activity"
//     "miscellaneous-region:Neurite_growth_inhibition"
//     "miscellaneous-region:Peptidyl-alpha-hydroxyglycine_alpha-amidating_lyase"
//     "miscellaneous-region:Regulatory/phosphorylation_domain"
//     "miscellaneous-region:Required_for_GRB10-binding"
//     "miscellaneous-region:Required_for_interaction_with_NR2C2"
//     "miscellaneous-region:Required_for_interaction_with_retromer"
//     "miscellaneous-region:S-adenosyl-L-methionine_binding"
//     "miscellaneous-region:Spacer"
//     "miscellaneous-region:Substrate_binding"
//     "miscellaneous-region:Sufficient_for_interaction_with_AGO2"
//     "miscellaneous-region:Sufficient_for_microtubule_severing"
//     "miscellaneous-region:Tail"
//     "miscellaneous-region:Triple-helical_region_1_(COL1)"
//     "miscellaneous-site:Alternative_splice_site_to_produce_'z'_isoforms"
//     "miscellaneous-site:Breakpoint_for_insertion_to_form_PDE4DIP-PDGFRB_fusion_protein"
//     "miscellaneous-site:Breakpoint_for_translocation"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_AML1-MTG8_in_AML-M2"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_BCAS4-BCAS3"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_BIRC2-MALT1"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_BIRC3-MALT1"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_chimeric_EWSR1/ATF1_protein"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_chimeric_MASL1"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_gamma-heregulin"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_JAZF1-SUZ12_oncogene"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_KAT6A-NCOA2"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_KAT6B-CREBBP"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_NPM1-MLF1"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_NPM-MLF1"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_PAX3-NCOA1_oncogene"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_PAX5-ETV6"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_PLZF-RAR-alpha_RAR-alpha1-PLZF_and_PML-RAR-alpha_oncogenes"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_RBM15-MKL1"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_RUNX1-MACROD1"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_TRIP11-PDGFRB"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_type-1_RUNX1-CBFA2T3_fusion_protein"
//     "miscellaneous-site:Breakpoint_for_translocation_to_form_type-2_RUNX1-CBFA2T3_fusion_protein"
//     "miscellaneous-site:Hydrophobic_interaction_with_UBC9"
//     "miscellaneous-site:Important_for_enzyme_activity"
//     "miscellaneous-site:Important_for_FDH_activity_and_activation_by_fatty_acids"
//     "miscellaneous-site:Interaction_with_target_DNA"
//     "miscellaneous-site:KMT2A/MLL1_fusion_point_(in_acute_myeloid_leukemia_patient_A)"
//     "miscellaneous-site:Mediates_interaction_with_PLCG1_and_SHB"
//     "miscellaneous-site:Required_for_ubiquitin-thioester_formation"
//     "modified-residue:Asymmetric_dimethylarginine"
//     "modified-residue:Citrulline"
//     "modified-residue:Dimethylated_arginine"
//     "modified-residue:N6-acetyllysine"
//     "modified-residue:N6-succinyllysine"
//     "modified-residue:N-acetylalanine"
//     "modified-residue:N-acetylmethionine"
//     "modified-residue:N-acetylserine"
//     "modified-residue:Omega-N-methylated_arginine"
//     "modified-residue:phosphoserine"
//     "modified-residue:Phosphoserine"
//     "modified-residue:phosphothreonine"
//     "modified-residue:Phosphothreonine"
//     "modified-residue:phosphotyrosine"
//     "modified-residue:Phosphotyrosine"
//     "modified-residue:PolyADP-ribosyl_aspartic_acid"
//     "modified-residue:S-nitrosocysteine"
//     "MYC::MAX"
//     "NFKB"
//     "Nr1h3::Rxra"
//     "Nrf1"
//     "Nrsf"
//     "nucleotide-phosphate-binding-region:AMP"
//     "nucleotide-phosphate-binding-region:ATP"
//     "nucleotide-phosphate-binding-region:GTP"
//     "nucleotide-phosphate-binding-region:NADP"
//     "Pax5"
//     "Pbx3"
//     "POU2F2"
//     "PPARG::RXRA"
//     "propeptide:match_to_sequence_model_evidence_used_in_manual_assertion"
//     "propeptide:N-terminal_propeptide"
//     "propeptide:Removed_for_receptor_activation"
//     "propeptide:Removed_in_mature_form"
//     "PU1"
//     "repeat:1"
//     "repeat:10"
//     "repeat:17"
//     "repeat:2"
//     "repeat:20"
//     "repeat:3"
//     "repeat:5"
//     "repeat:8"
//     "repeat:9"
//     "repeat:ANK_15"
//     "repeat:ANK_7"
//     "repeat:ARM_5"
//     "repeat:ARM_8"
//     "repeat:BNR_5"
//     "repeat:BNR_9"
//     "repeat:GTF2I-like_2"
//     "repeat:GTF2I-like_5"
//     "repeat:Kelch_3"
//     "repeat:Kelch_4"
//     "repeat:Kelch_5"
//     "repeat:LDL-receptor_class_B_13"
//     "repeat:LDL-receptor_class_B_19"
//     "repeat:LDL-receptor_class_B_2"
//     "repeat:LDL-receptor_class_B_23"
//     "repeat:LDL-receptor_class_B_28"
//     "repeat:LDL-receptor_class_B_3"
//     "repeat:LDL-receptor_class_B_32"
//     "repeat:LDL-receptor_class_B_33"
//     "repeat:LDL-receptor_class_B_36"
//     "repeat:LDL-receptor_class_B_6"
//     "repeat:LDL-receptor_class_B_8"
//     "repeat:LRR_1"
//     "repeat:LRR_15"
//     "repeat:LRR_6"
//     "repeat:RCC1_1"
//     "repeat:RCC1_2"
//     "repeat:RCC1_3-7"
//     "repeat:TPR_2"
//     "repeat:TPR_4"
//     "repeat:WD_1"
//     "repeat:WD_2"
//     "repeat:WD_3"
//     "repeat:WD_4"
//     "repeat:WD_5"
//     "repeat:WD_6"
//     "repeat:WD_7"
//     "RXRA"
//     "RXRA::VDR"
//     "RXR::RAR_DR5"
//     "short-sequence-motif:FTZ-F1_box"
//     "short-sequence-motif:Nuclear_localization_signal"
//     "short-sequence-motif:Nudix_box"
//     "short-sequence-motif:Selectivity_filter_part_1"
//     "signal-peptide:match_to_sequence_model_evidence_used_in_manual_assertion"
//     "SP1"
//     "SP2"
//     "Srf"
//     "Tcf12"
//     "topological-domain:Cytoplasmic"
//     "topological-domain:Extracellular"
//     "topological-domain:Lumenal"
//     "topological-domain:Vacuolar"
//     "Tr4"
//     "transcript"
//     "transmembrane-region:Transmembrane_region"
//     "turn:combinatorial_evidence_used_in_manual_assertion"
//     "USF1"
//     "Yy1"
//     "ZBTB33"
//     "ZEB1"
//     "zinc-finger-region:B_box-type"
//     "zinc-finger-region:C2H2-type_14"
//     "zinc-finger-region:C2H2-type_17"
//     "zinc-finger-region:C2H2-type_4"
//     "zinc-finger-region:C2H2-type_7"
//     "zinc-finger-region:C3H1-type_1"
//     "zinc-finger-region:RING-CH-type"
//     "zinc-finger-region:RING-type"
//     "zinc-finger-region:ZZ-type"
//     "Znf263"
// }

struct Annotation
{
    /// Allele (or ALT)
    const(char)[] allele;

    /// Annotation (a.k.a. effect or consequence): Annotated using Sequence Ontology terms. Multiple effects can be concatenated using ‘&’.
    Buffer!Effect effect;

    /// Putative_impact: A simple estimation of putative impact / deleteriousness : {HIGH, MODERATE, LOW, MODIFIER}
    Modifier impact;

    /// Gene Name: Common gene name (HGNC).
    Option!(const(char)[]) gene_name;

    /// Gene ID: Gene ID
    Option!(const(char)[]) gene_id;

    /// Feature type: Which type of feature is in the next field
    const(char)[] feature_type;

    /// Feature ID: Depending on the annotation, this may be: Transcript ID, Motif ID, miRNA, ChipSeq peak, Histone mark, etc.
    const(char)[] feature_id;

    /// Transcript biotype. The bare minimum is at least a description on whether the transcript is {“Coding”, “Noncoding”}. Whenever possible, use ENSEMBL biotypes.
    Option!(TranscriptBiotype) transcript_biotype;

    /// Rank / total : Exon or Intron rank / total number of exons or introns.
    Option!long rank; // not required
    Option!long rtotal; // not required

    /// HGVS.c: Variant using HGVS notation (DNA level)
    const(char)[] hgvs_c;

    /// HGVS.p: If variant is coding, this field describes the variant using HGVS notation (Protein level).
    /// Since transcript ID is already mentioned in ‘feature ID’, it may be omitted here.
    Option!(const(char)[]) hgvs_p; // not required

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
    Option!(AnnErrWarnInfo) errors_warnings_info;
    
    @safe @nogc nothrow pragma(inline, true):
    this(const(char)[] ann)
    {

        auto vals = ann.findSplit('|');
        this.allele = vals[0];

        vals = vals[1].findSplit('|');
        auto s = vals[0].findSplit('&');
        if(s != ["", ""]) {
            this.effect ~= enumFromStr!Effect(s[0]);
            while(s != ["", ""]){
                this.effect ~= enumFromStr!Effect(s[0]);
                s = s[1].findSplit('&');
            }
        } else {
            this.effect ~= enumFromStr!Effect(vals[0]);
        }

        vals = vals[1].findSplit('|');
        this.impact = enumFromStr!Modifier(vals[0]);

        vals = vals[1].findSplit('|');
        if (vals[0] != "")
        {
            this.gene_name = Some(vals[0]);
        }

        vals = vals[1].findSplit('|');
        if (vals[0] != "")
        {
            this.gene_id = Some(vals[0]);
        }

        vals = vals[1].findSplit('|');
        this.feature_type = vals[0];

        vals = vals[1].findSplit('|');
        this.feature_id = vals[0];

        vals = vals[1].findSplit('|');
        if (vals[0] != "")
        {
            this.transcript_biotype = Some(enumFromStr!TranscriptBiotype(vals[0]));
        }

        vals = vals[1].findSplit('|');
        if (vals[0] != "")
        {
            auto f = vals[0].findSplit('/');
            long r1;
            f[0].fromString(r1);
            this.rank = Some(r1);
            if (f.length == 2) {
                f[1].fromString(r1);
                this.rtotal = Some(r1);
            }
        }

        vals = vals[1].findSplit('|');
        this.hgvs_c = vals[0];

        vals = vals[1].findSplit('|');
        if (vals[0] != "")
            this.hgvs_p = Some(vals[0]);

        vals = vals[1].findSplit('|');
        if (vals[0] != "")
        {
            auto f = vals[0].findSplit('/');
            long r1;
            f[0].fromString(r1);
            this.cdna_position = Some(r1);
            if (f.length == 2) {
                f[1].fromString(r1);
                this.cdna_length = Some(r1);
            }
        }

        vals = vals[1].findSplit('|');
        if (vals[0] != "")
        {
            auto f = vals[0].findSplit('/');
            long r1;
            f[0].fromString(r1);
            this.cds_position = Some(r1);
            if (f.length == 2) {
                f[1].fromString(r1);
                this.cds_length = Some(r1);
            }
        }

        vals = vals[1].findSplit('|');
        if (vals[0] != "")
        {
            auto f = vals[0].findSplit('/');
            long r1;
            f[0].fromString(r1);
            this.protein_position = Some(r1);
            if (f.length == 2) {
                f[1].fromString(r1);
                this.protein_length = Some(r1);
            }
        }

        vals = vals[1].findSplit('|');
        if (vals[0] != "") {
            long r1;
            vals[0].fromString(r1);
            this.distance_to_feature = Some(r1);
        }

        vals = vals[1].findSplit('|');

        if (vals[0] != "")
            this.errors_warnings_info = Some(enumFromStr!AnnErrWarnInfo(vals[0]));
    }

    void serialize(ref VcfRecordSerializer serializer)
    {
        auto s = serializer.structBegin;

        serializer.putKey("allele");
        serializer.putValue(this.allele);

        serializer.putKey("effect");
        if(effect.length == 1) {
            serializer.putSymbol(enumToString(effect[0]));
            effect.deallocate;
        } else {
            auto l = serializer.listBegin;
            foreach (e; effect)
            {
                serializer.putSymbol(enumToString(e));
            }
            effect.deallocate;
            serializer.listEnd(l);
        }

        serializer.putKey("impact");
        serializer.putSymbol(enumToString(impact));

        if (!this.gene_name.isNone)
        {
            serializer.putKey("gene_name");
            serializer.putValue(this.gene_name.unwrap);
        }

        if (!this.gene_id.isNone)
        {
            serializer.putKey("gene_id");
            serializer.putValue(this.gene_id.unwrap);
        }

        serializer.putKey("feature_type");
        serializer.putSymbol(this.feature_type);

        serializer.putKey("feature_id");
        serializer.putValue(this.feature_id);

        if (!this.transcript_biotype.isNone)
        {
            serializer.putKey("transcript_biotype");
            serializer.putSymbol(enumToString(this.transcript_biotype.unwrap));
        }

        if (!this.rank.isNone)
        {
            serializer.putKey("rank");
            serializer.putValue(this.rank.unwrap);
        }

        if (!this.rtotal.isNone)
        {
            serializer.putKey("rtotal");
            serializer.putValue(this.rtotal.unwrap);
        }

        serializer.putKey("hgvs_c");
        serializer.putValue(this.hgvs_c);

        if (!this.hgvs_p.isNone)
        {
            serializer.putKey("hgvs_p");
            serializer.putValue(this.hgvs_p.unwrap);
        }

        if (!this.cdna_position.isNone)
        {
            serializer.putKey("cdna_position");
            serializer.putValue(this.cdna_position.unwrap);
        }
        if (!this.cdna_length.isNone)
        {
            serializer.putKey("cdna_length");
            serializer.putValue(this.cdna_length.unwrap);
        }

        if (!this.cds_position.isNone)
        {
            serializer.putKey("cds_position");
            serializer.putValue(this.cds_position.unwrap);
        }
        if (!this.cds_length.isNone)
        {
            serializer.putKey("cds_length");
            serializer.putValue(this.cds_length.unwrap);
        }

        if (!this.protein_position.isNone)
        {
            serializer.putKey("protein_position");
            serializer.putValue(this.protein_position.unwrap);
        }
        if (!this.protein_length.isNone)
        {
            serializer.putKey("protein_length");
            serializer.putValue(this.protein_length.unwrap);
        }

        if (!this.distance_to_feature.isNone)
        {
            serializer.putKey("distance_to_feature");
            serializer.putValue(this.distance_to_feature.unwrap);
        }

        if (!this.errors_warnings_info.isNone)
        {
            serializer.putKey("errors_warnings_info");
            serializer.putSymbol(enumToString(this.errors_warnings_info.unwrap));
        }

        serializer.structEnd(s);
    }

}

unittest
{
    import libmucor.serde.deser;

    string ann = "A|intron_variant|MODIFIER|PLCXD1|ENSG00000182378|Transcript|ENST00000381657|"
        ~ "protein_coding|1/6|ENST00000381657.2:c.-21-26C>A|||||,A|intron_variant|MODIFIER"
        ~ "|PLCXD1|ENSG00000182378|Transcript|ENST00000381663|protein_coding|1/7|ENST00000381663.3:c.-21-26C>A||"
        ~ "|||";
    auto anns = Annotations(ann);
    import std.stdio;
    import mir.ser.ion;
    import mir.ion.stream;
    import mir.ion.conv;

    auto parsed = anns.array;
    enum annFields = serdeGetSerializationKeysRecurse!Annotation.removeSystemSymbols;
    assert(serializeVcfToIon(parsed[0], annFields)[].dup.ion2text == `{allele:"A",effect:intron_variant,impact:MODIFIER,gene_name:"PLCXD1",gene_id:"ENSG00000182378",feature_type:Transcript,feature_id:"ENST00000381657",transcript_biotype:protein_coding,rank:1,rtotal:6,hgvs_c:"ENST00000381657.2:c.-21-26C>A"}`);
    assert(serializeVcfToIon(parsed[1], annFields)[].dup.ion2text == `{allele:"A",effect:intron_variant,impact:MODIFIER,gene_name:"PLCXD1",gene_id:"ENSG00000182378",feature_type:Transcript,feature_id:"ENST00000381663",transcript_biotype:protein_coding,rank:1,rtotal:7,hgvs_c:"ENST00000381663.3:c.-21-26C>A"}`);

    // assert(serializeVcfToIon(Effect._5_prime_UTR_premature_start_codon_gain_variant).ion2text == "'5_prime_UTR_premature_start_codon_gain_variant'");

}
