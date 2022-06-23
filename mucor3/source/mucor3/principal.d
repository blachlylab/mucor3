module mucor3.principal;

import std.algorithm : map, filter, each, countUntil, splitter, maxIndex;
import std.array : array;
import std.traits : ReturnType, EnumMembers;
import std.typecons : No;
import std.range;

import dhtslib.vcf;
import dhtslib.gff;
import libmucor.error;
import libmucor.vcfops.fields;


/// Tags that are penalized in the Principal Isoform score
/// Hopefully these are mutually exlusive with principal isoforms
enum NonCanonical : ubyte {
    /// the transcript has a non-canonical splice site conserved in other species.
    non_canonical_conserved = 1,
    /// the transcript has a non-canonical splice site explained by a genomic sequencing error.
    non_canonical_genome_sequence_error = 1 << 1,
    /// the transcript has a non-canonical splice site explained by other reasons.
    non_canonical_other = 1 << 2,
    /// the transcript has a non-canonical splice site explained by a SNP.
    non_canonical_polymorphism = 1 << 3,
    /// the transcript has a non-canonical splice site that needs experimental confirmation.
    non_canonical_TEC = 1 << 4,

    /// shares an identical CDS but has alternative 5' UTR with respect to a reference variant.
    alternative_3_UTR = 1 << 5,
    /// shares an identical CDS but has alternative 3' UTR with respect to a reference variant.
    alternative_5_UTR = 1 << 6,

    /// member of the pseudogene set predicted by YALE, UCSC and HAVANA.
    pseudo_consens = 1 << 7,

}

/// APPRIS score Tag
enum Appris : ushort {
    /// (This flag corresponds to the older flag "appris_principal") Where the transcript expected to 
    /// code for the main functional isoform based solely on the core modules in the APPRIS database. 
    /// The APPRIS core modules map protein structural and functional information and cross-species 
    /// conservation to the annotated variants.
    appris_principal_1 = 1 << 13,
    /// (This flag corresponds to the older flag "appris_candidate_ccds") Where the APPRIS core modules 
    /// are unable to choose a clear principal variant (approximately 25% of human protein coding genes), 
    /// the database chooses two or more of the CDS variants as "candidates" to be the principal variant. 
    /// If one (but no more than one) of these candidates has a distinct CCDS identifier it is selected 
    /// as the principal variant for that gene. A CCDS identifier shows that there is consensus between 
    /// RefSeq and GENCODE/Ensembl for that variant, guaranteeing that the variant has cDNA support.    
    appris_principal_2= 1 << 12,
    /// Where the APPRIS core modules are unable to choose a clear principal variant and there more than 
    /// one of the variants have distinct CCDS identifiers, APPRIS selects the variant with lowest CCDS 
    /// identifier as the principal variant. The lower the CCDS identifier, the earlier it was annotated. 
    /// Consensus CDS annotated earlier are likely to have more cDNA evidence. Consecutive CCDS identifiers
    /// are not included in this flag, since they will have been annotated in the same release of CCDS. 
    /// These are distinguished with the next flag.
    appris_principal_3 = 1 << 11,
    /// (This flag corresponds to the Ensembl 78 flag "appris_candidate_longest_ccds") Where the APPRIS core 
    /// modules are unable to choose a clear principal CDS and there is more than one variant with a distinct 
    /// (but consecutive) CCDS identifiers, APPRIS selects the longest CCDS isoform as the principal variant.
    appris_principal_4 = 1 << 10,
    /// (This flag corresponds to the Ensembl 78 flag "appris_candidate_longest_seq") Where the APPRIS core 
    /// modules are unable to choose a clear principal variant and none of the candidate variants are 
    /// annotated by CCDS, APPRIS selects the longest of the candidate isoforms as the principal variant.
    appris_principal_5 = 1 << 9,
    /// Candidate transcript(s) models that are conserved in at least three tested non-primate species.
    appris_alternative_1 = 1 << 8,
    /// Candidate transcript(s) models that appear to be conserved in fewer than three tested non-primate species.
    appris_alternative_2 = 1 << 7,
    /// transcript expected to code for the main functional isoform based on a range of protein features 
    /// (APPRIS pipeline).
    appris_principal = 1 << 6,
    
    /// where there is no 'appris_principal' variant, the candidate with highest APPRIS score is selected as 
    /// the primary variant.
    appris_candidate_highest_score = 1 << 5,

    /// where there is no 'appris_principal' variant, the longest of the 'appris_candidate' variants is 
    /// selected as the primary variant.
    appris_candidate_longest = 1 << 4,

    /// the "appris_candidate" transcripts where there are several CCDS, in this case APPRIS labels 
    /// the longest CCDS.
    appris_candidate_longest_ccds = 1 << 3,

    /// where there is no "appris_candidate_ccds" or "appris_candidate_longest_ccds" variant, the longest 
    /// protein of the "appris_candidate" variants is selected as the primary variant.
    appris_candidate_longest_seq = 1 << 2,

    /// the "appris_candidate" transcript that has an unique CCDS.
    appris_candidate_ccds = 1 << 1,

    /// where there is no single 'appris_principal' variant the main functional isoform will be translated 
    /// from one of the 'appris_candidate' genes.
    appris_candidate = 1,

}

/// MANE score tag
/// The Matched Annotation from NCBI and EMBL-EBI project (MANE)
enum MANE {
    /// the transcript belongs to the MANE Plus Clinical data set. Within the MANE project, these are 
    /// additional transcripts per locus necessary to support clinical variant reporting, for example 
    /// transcripts containing known pathogenic or likely pathogenic clinical variants not reportable 
    /// using the MANE Select data set. This transcript set matches GRCh38 and is 100% identical between 
    /// RefSeq and Ensembl-GENCODE for 5' UTR, CDS, splicing and 3' UTR.
    MANE_Plus_Clinical = 1,
    /// the transcript belongs to the MANE Select data set. The Matched Annotation from NCBI and EMBL-EBI 
    /// project (MANE) is a collaboration between Ensembl-GENCODE and RefSeq to select a default transcript 
    /// per human protein coding locus that is representative of biology, well-supported, expressed and 
    /// conserved. This transcript set matches GRCh38 and is 100% identical between RefSeq and 
    /// Ensembl-GENCODE for 5' UTR, CDS, splicing and 3' UTR.
    MANE_Select,
}

enum OtherTags: ubyte {
    /// identifies a subset of representative transcripts for each gene; prioritises full-length 
    /// protein coding transcripts over partial or non-protein coding transcripts within the same gene, 
    /// and intends to highlight those transcripts that will be useful to the majority of users.
    basic = 1,
}

auto scoreTags(T)(string[] tags) {
    uint score;
    foreach (i, tag; tags)
    {
        sw: switch(tag) {
            static foreach (v; EnumMembers!T)
            {
                case v.stringof:
                    score |= v;
                    break sw;
            }
            default:
                break;
        }
    }
    return score;
}

struct PrincipalScore {
    ulong score;
    uint penalty;

    this(GFF3Record rec) {
        auto tags = rec["tag"].splitter(",").array;
        this.score = scoreTags!OtherTags(tags);
        this.score |= scoreTags!Appris(tags) << 1;
        this.score |= scoreTags!MANE(tags) << 15;
        this.penalty = scoreTags!NonCanonical(tags);

    }

    auto combinedScore() {
        import core.bitop;
        return this.score - popcnt(penalty) * 2;
    }
}

/// Filter annotation to principal isoform
void filterAnnotationToPrincipalIsoform(VCFRecord rec, string annotationFile, string infoField = "ANN", string annField = "feature_id", string[] annFieldNames = ANN_FIELDS) {

    auto infos = rec.getInfos;
    auto field = infoField in infos;
    if(field is null) return;

    auto region = GFF3Reader(annotationFile, rec.chrom, rec.coordinates);
    auto anns = Annotations((*field).to!string, annFieldNames);

    auto featureNames = anns.filter!(x => !x[annField].isNull).map!(x => x[annField].value[0]).array;
    auto matchingRecords = region.filter!(rec => featureNames.countUntil(rec["ID"]) != -1).array;
    
    auto scores = matchingRecords.map!(x => PrincipalScore(x).combinedScore).array;
    auto bestIdx = scores.maxIndex;
    if(bestIdx == -1) {
        auto newAnn = anns[0].original;
        rec.addInfo(infoField, newAnn);
        return;
    }
    auto best = matchingRecords[bestIdx];

    string newAnn;
    foreach (i, f; featureNames)
    {
        if(f == best["ID"]) {
            newAnn = anns[i].original;
            break;
        }
    }
    
    rec.addInfo(infoField, newAnn);
}

void principal(string[] args) {
    if(args.length < 3 || args.length > 3) 
        log_err(__FUNCTION__, "Incorrect number of arguments. Usage: mucor3 principal [VCF/BCF] [tabix'd GFF3] > out.vcf");
    import std.stdio;
    auto vcfr = VCFReader(args[1]);
    auto vcfw = VCFWriter("-", vcfr.vcfhdr, VCFWriterTypes.VCF);
    vcfw.writeHeader;
    foreach (rec; vcfr)
    {
        filterAnnotationToPrincipalIsoform(rec, args[2]);
        vcfw.writeRecord(rec);
    }
}
