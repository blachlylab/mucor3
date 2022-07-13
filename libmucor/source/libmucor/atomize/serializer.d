module libmucor.atomize.serializer;

import mir.utility: _expect;
import mir.ser: serializeValue;
import mir.ser.ion : IonSerializer, ionSerializer;
import mir.ion.symbol_table: IonSymbolTable, IonSystemSymbolTable_v1;
import mir.bignum.integer;
import mir.serde : SerdeTarget, serdeGetSerializationKeysRecurse;

import libmucor.spookyhash;
import libmucor.atomize.ann;
import libmucor.wideint;

size_t SEED1 = 0x48e9a84eeeb9f629;
size_t SEED2 = 0x2e1869d4e0b37fcb;

string[] removeSystemSymbols(const(string)[] keys) @safe pure nothrow
{
    string[] ret;
    F: foreach (key; keys) switch(key)
    {
        static foreach (skey; IonSystemSymbolTable_v1)
        {
            case skey: continue F;
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


struct VcfSerializer(T) {
    IonSymbolTable!true table;

    enum nMax = 4096u;
    enum keys = serdeGetSerializationKeysRecurse!T.removeSystemSymbols;

    IonSerializer!(nMax * 8, keys, true) serializer;

    this(SerdeTarget target) {
        this.serializer = ionSerializer!(nMax * 8, keys, true);
        this.serializer.initialize(table, target);
    }

    void put(ref T val) {
        val.serialize(this.serializer);
    }

    auto finalize() {
        serializer.finalize;

        static immutable ubyte[] compiletimePrefixAndTableTapeData = ionPrefix ~ serializer.compiletimeTableTape;

        // use runtime table
        if (_expect(table.initialized, false))
        {
            table.finalize; 
            return () @trusted { return  cast(immutable) (ionPrefix ~ table.data ~ serializer.data); } ();
        }
        // compile time table
        else
        {
            return () @trusted { return  cast(immutable) (compiletimePrefixAndTableTapeData ~ serializer.data); } ();
        }
    }
}