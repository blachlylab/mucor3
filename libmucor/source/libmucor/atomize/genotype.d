module libmucor.atomize.genotype;

import dhtslib.vcf : BcfRecordType;
import htslib.vcf : bcf_fmt_t, bcf_info_t;
import dhtslib.memory : Bcf1;
import mir.appender: scopedBuffer, ScopedBuffer;
import libmucor.error;
import mir.ser;
import libmucor.serde.ser;

import memory;

/// Represents individual GT values as encoded in BCF. 
/// This is described in the VCF 4.2 spec section 6.3.3.
/// In summary it can be an int, short, or byte and 
/// encodes an allele value and a phased flag
/// e.g (allele + 1) << 1 | phased
union GT(T) 
{
    import std.bitmanip : bitfields;
    T raw;
    mixin(bitfields!(
        bool, "phased", 1,
        T, "allele", (T.sizeof * 8) - 1,
        ));
    
    @nogc nothrow @safe:
    /// get allele value
    auto getAllele() const {
        return this.allele - 1;
    }

    /// set allele value
    void setAllele(int val) {
        this.allele = cast(T)(val + 1);
    }

    /// check if value is missing
    bool isMissing() const {
        return this.allele == 0;
    }

    /// set value as missing
    void setMissing() {
        this.allele = T(0);
    }

    /// check if value is padding
    bool isPadding() {
        static if (is(T == byte)) {
            return cast(bool)((this.raw & 0xFE) == 0x80);
        } else static if (is(T == short)) {
            return cast(bool)((this.raw & 0xFFFE) == 0x8000);
        } else static if (is(T == int)) {
            return cast(bool)((this.raw & 0xFFFFFFFE) == 0x80000000);
        }
    }

}

/// Struct to represent VCF/BCF genotype data
struct Genotype
{
    /// Type of underlying format data
    BcfRecordType type;
    /// Array of encoded genotypes
    void[] data;

    /// ctor from FormatField
    this(bcf_fmt_t* gt, ulong sampleIdx) @nogc @trusted nothrow
    {
        this.type = cast(BcfRecordType)gt.type;
        final switch(this.type){
            case BcfRecordType.Int8:
                this.data = (cast(byte*)gt.p)[sampleIdx*gt.size..(sampleIdx+1)*gt.size];
                break;
            case BcfRecordType.Int16:
                this.data = (cast(short*)gt.p)[sampleIdx*gt.n ..(sampleIdx+1)*gt.n];
                break;
            case BcfRecordType.Int32:
                this.data = (cast(int*)gt.p)[sampleIdx*gt.n ..(sampleIdx+1)*gt.n];
                break;
            case BcfRecordType.Int64:
            case BcfRecordType.Float:
            case BcfRecordType.Char:
            case BcfRecordType.Null:
                assert(0);
        }
    }

    bool isNullOrRef() @nogc @trusted nothrow
    {
        final switch(this.type){
            case BcfRecordType.Int8:
                foreach(GT!byte g; cast(GT!byte[])(this.data)) {
                    if(g.isMissing) return true;
                    if(g.isPadding) continue;
                    if(g.getAllele() != 0) return false;
                }
                return true;
            case BcfRecordType.Int16:
                foreach(GT!short g; cast(GT!short[])(this.data)) {
                    if(g.isMissing) return true;
                    if(g.isPadding) continue;
                    if(g.getAllele() != 0) return false;
                }
                return true;
            case BcfRecordType.Int32:
                foreach(GT!int g; cast(GT!int[])(this.data)) {
                    if(g.isMissing) return true;
                    if(g.isPadding) continue;
                    if(g.getAllele() != 0) return false;
                }
                return true;
            case BcfRecordType.Int64:
            case BcfRecordType.Float:
            case BcfRecordType.Char:
            case BcfRecordType.Null:
                assert(0);
        }
    }

    void reset() @nogc @trusted nothrow
    {
        this.data = [];
    }

    void serialize(ref VcfRecordSerializer s) @nogc @trusted nothrow {
        import mir.format;
        auto ret = scopedBuffer!char;
        final switch(this.type){
            static immutable PHASE_CHARS = ['/','|']; 
            case BcfRecordType.Int8:
                foreach(i, gt; cast(GT!byte[]) this.data) {
                    if(i)
                        ret.put(cast(char)PHASE_CHARS[gt.phased]);
                    if(gt.isPadding) {
                        ret.shrinkTo(ret.length-1);
                        break;
                    }
                    if (gt.getAllele() == -1) {
                        print(ret, '.');
                    } else { 
                        print(ret, gt.getAllele());
                    }
                }
                serializeValue(s.serializer, ret.data);
                return ;
            case BcfRecordType.Int16:
                foreach(i, gt; cast(GT!short[]) this.data) {
                    if(i)
                        ret.put(cast(char)PHASE_CHARS[gt.phased]);
                    if(gt.isPadding) {
                        ret.shrinkTo(ret.length-1);
                        break;
                    }
                    if (gt.getAllele() == -1) {
                        print(ret, '.');
                    } else { 
                        print(ret, gt.getAllele());
                    }
                }
                serializeValue(s.serializer, ret.data);
                return;
            case BcfRecordType.Int32:
            foreach(i, gt; cast(GT!int[]) this.data) {
                    if(i)
                        ret.put(cast(char)PHASE_CHARS[gt.phased]);
                    if(gt.isPadding) {
                        ret.shrinkTo(ret.length-1);
                        break;
                    }
                    if (gt.getAllele() == -1) {
                        print(ret, '.');
                    } else { 
                        print(ret, gt.getAllele());
                    }
                }
                serializeValue(s.serializer, ret.data);
                return;
            case BcfRecordType.Int64:
            case BcfRecordType.Float:
            case BcfRecordType.Char:
            case BcfRecordType.Null:
                assert(0);
        }
    }
}