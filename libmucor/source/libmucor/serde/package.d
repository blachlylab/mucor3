module libmucor.serde;

import mir.ion.symbol_table : IonSymbolTable, IonSystemSymbolTable_v1;
import mir.ion.value;
import mir.utility : _expect;
import mir.bignum.integer;
import libmucor.spookyhash;
import libmucor.error;
public import libmucor.memory;
public import htslib.bgzf;

public import libmucor.serde.ser;
public import libmucor.serde.deser;
public import libmucor.serde.symbols;


public import mir.serde : SerdeTarget;

size_t SEED1 = 0x48e9a84eeeb9f629;
size_t SEED2 = 0x2e1869d4e0b37fcb;

alias Bgzf = SafePtr!(BGZF, bgzf_close);

const(char[])[] removeSystemSymbols(const(char[])[] keys) @safe pure nothrow
{
    const(char[])[] ret;
    F: foreach (key; keys) switch (key)
    {
        static foreach (skey; IonSystemSymbolTable_v1)
        {
    case skey:
            continue F;
        }
    default:
        ret ~= key;
    }
    return ret;
}

static immutable ubyte[] ionPrefix = [0xe0, 0x01, 0x00, 0xea];

BigInt!2 hashIon(ubyte[] data)
{
    BigInt!2 ret = BigInt!2([SEED1, SEED2]);
    SpookyHash.Hash128(data.ptr, data.length, &ret.data[0], &ret.data[1]);
    return ret;
}

void handleIonError(IonErrorCode err) @nogc nothrow
{
    debug
    {
        assert(!err, ionErrorMsg(err));
    }
    else
    {
        if (_expect(err, false))
            log_err(__FUNCTION__, "Ion error: %s", ionErrorMsg(err));
    }
}
