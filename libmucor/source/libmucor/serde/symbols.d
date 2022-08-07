module libmucor.serde.symbols;

import mir.ion.exception;
import mir.ion.type_code;
import mir.ion.value;
import mir.utility: _expect;
import mir.appender: ScopedBuffer;
import mir.ion.symbol_table;
import mir.ion.stream;
import mir.serde : serdeGetSerializationKeysRecurse;
import mir.ser.ion : IonSerializer;

import libmucor.atomize.header;
import libmucor.atomize.ann;
import libmucor.serde;
import libmucor.serde.deser : parseValue;
import libmucor.khashl;

struct SymbolTableBuilder {

    IonSerializer!(1024, null, false) serializer;

    /// previously serialized symbol hashmap
    khashl!(const(char)[], size_t, true) currentSymbolsMap;
    /// new yet-to-be serialized symbol hashmap
    khashl!(const(char)[], size_t, true) newSymbolsMap;
    /// new symbols
    const(char)[][] syms;
    /// concatenated new symbols
    /// just used for hashing
    ubyte[] dataForHashing;
    /// total number of symbols
    size_t numSymbols = IonSystemSymbol.max + 1;
    bool first = true;

    /// insert symbol  
    size_t insert(const(char)[] key) {
        auto p = key in currentSymbolsMap;
        if(p)
            return *p;
        p = key in newSymbolsMap;
        if(p)
            return *p;
        newSymbolsMap[key] = numSymbols++;
        syms ~= key;
        return numSymbols - 1;
    }

    ubyte[] getRawSymbols() {
        import std.algorithm;
        import std.array;
        return cast(ubyte[])this.syms.joiner.array;
    }

    // $ion_symbol_table::
    // {
    //     symbols:[ ... ]
    // }
    const(ubyte)[] serialize() {
        if(this.syms.length > 0) {
            this.serializer.initializeNoTable;

            auto annotationWrapperState = serializer.annotationWrapperBegin;

            serializer.putAnnotationId(IonSystemSymbol.ion_symbol_table);
            auto annotationsState = serializer.annotationsEnd(annotationWrapperState);

            auto structState = serializer.structBegin();
            if(_expect(!first, true)){
                serializer.putKeyId(IonSystemSymbol.imports);
                serializer.putSymbolId(IonSystemSymbol.ion_symbol_table);
            }
            serializer.putKeyId(IonSystemSymbol.symbols);
            auto listState = serializer.listBegin();

            auto n = currentSymbolsMap.kh_size + IonSystemSymbol.max + 1;
            foreach (i, const(char)[] key; syms)
            {
                serializer.putValue(key);
                currentSymbolsMap[key] = n + i;
            }

            serializer.listEnd(listState);
            serializer.structEnd(structState);
            serializer.annotationWrapperEnd(annotationsState, annotationWrapperState);

            this.syms.length = 0;
            this.newSymbolsMap.kh_clear();
            this.first = false;
            return this.serializer.data;
        }
        return [];
    }
}

struct SymbolTable {
    ScopedBuffer!(const(char)[]) symbolTableBuffer = void;
    const(char[])[] table;

    void initialize() {
        symbolTableBuffer.initialize;
    }

    @trusted pure nothrow createFromHeaderConfig(ref HeaderConfig hdrInfo) {
        IonSymbolTable!false tmptable;
        tmptable.initialize;
        tmptable.insert("CHROM");
        tmptable.insert("POS");
        tmptable.insert("ID");
        tmptable.insert("REF");
        tmptable.insert("ALT");
        tmptable.insert("QUAL");
        tmptable.insert("FILTER");
        tmptable.insert("INFO");
        tmptable.insert("checksum");
        tmptable.insert("sample");
        if(hdrInfo.fmts.byAllele.names.length > 0 || hdrInfo.fmts.other.names.length > 0)
            tmptable.insert("FORMAT");
        foreach (f; hdrInfo.filters){
            tmptable.insert(f);
        }
        foreach (sam; hdrInfo.samples)
        {
            tmptable.insert(sam);
        }
        if(hdrInfo.fmts.byAllele.names.length > 0 || hdrInfo.infos.byAllele.names.length > 0)
            tmptable.insert("byAllele");
        foreach (name; hdrInfo.infos.byAllele.names)
        {
            tmptable.insert(name);
        }

        foreach (name; hdrInfo.infos.other.names)
        {
            tmptable.insert(name);
        }

        foreach (name; hdrInfo.infos.annotations.names)
        {
            if(name == "ANN"){
                
                import mir.serde : SerdeTarget, serdeGetSerializationKeysRecurse;
                enum annFields = serdeGetSerializationKeysRecurse!Annotation.removeSystemSymbols;
                static foreach (const(string) key; annFields)
                {
                    tmptable.insert(key);    
                }   
            }
            tmptable.insert(name);
        }

        foreach (name; hdrInfo.fmts.byAllele.names)
        {
            tmptable.insert(name);
        }

        foreach (name; hdrInfo.fmts.other.names)
        {
            tmptable.insert(name);
        }
        tmptable.finalize;
        auto d = cast(const(ubyte)[])tmptable.data;
        auto err = this.loadSymbolTable(d);
        assert(!err, ionErrorMsg(err));
    }

    @trusted pure nothrow createFromStrings(string[] symbols) {
        IonSymbolTable!false tmptable;
        tmptable.initialize;
        tmptable.insert("CHROM");
        tmptable.insert("POS");
        tmptable.insert("ID");
        tmptable.insert("REF");
        tmptable.insert("ALT");
        tmptable.insert("QUAL");
        tmptable.insert("FILTER");
        tmptable.insert("INFO");
        tmptable.insert("checksum");
        tmptable.insert("FORMAT");
        foreach (string key; symbols)
        {
            tmptable.insert(key);
        }
        tmptable.finalize;
        auto d = cast(const(ubyte)[]) tmptable.data;
        auto err = this.loadSymbolTable(d);
        assert(!err, ionErrorMsg(err));
    }

    /// scraped and modified from here: 
    @trusted pure nothrow @nogc
    scope IonErrorCode loadSymbolTable(ref const(ubyte)[] d)
    {

        void resetSymbolTable()
        {
            symbolTableBuffer.reset;
            symbolTableBuffer.put(IonSystemSymbolTable_v1);
        }

        IonErrorCode error;
        IonDescribedValue describedValue;
        error = d.parseValue(describedValue);
        // check if describedValue is symbol table
        if (describedValue.descriptor.type == IonTypeCode.annotations)
        {
            auto annotationWrapper = describedValue.trustedGet!IonAnnotationWrapper;
            IonAnnotations annotations;
            IonDescribedValue symbolTableValue;
            error = annotationWrapper.unwrap(annotations, symbolTableValue);
            if (!error && !annotations.empty)
            {
                // check first annotation is $ion_symbol_table
                {
                    bool nextAnnotation;
                    foreach (IonErrorCode annotationError, size_t annotationId; annotations)
                    {
                        error = annotationError;
                        if (error)
                            goto C;
                        if (nextAnnotation)
                            continue;
                        nextAnnotation = true;
                        if (annotationId != IonSystemSymbol.ion_symbol_table)
                            goto C;
                    }
                }
                IonStruct symbolTableStruct;
                if (symbolTableValue.descriptor.type != IonTypeCode.struct_)
                {
                    error = IonErrorCode.expectedStructValue;
                    goto C;
                }
                if (symbolTableValue != null)
                {
                    symbolTableStruct = symbolTableValue.trustedGet!IonStruct;
                }

                {
                    bool preserveCurrentSymbols;
                    IonList symbols;

                    foreach (IonErrorCode symbolTableError, size_t symbolTableKeyId, IonDescribedValue elementValue; symbolTableStruct)
                    {
                        error = symbolTableError;
                        if (error)
                            goto C;
                        switch (symbolTableKeyId)
                        {
                            case IonSystemSymbol.imports:
                            {
                                if (preserveCurrentSymbols || (elementValue.descriptor.type != IonTypeCode.symbol && elementValue.descriptor.type != IonTypeCode.list))
                                {
                                    error = IonErrorCode.invalidLocalSymbolTable;
                                    goto C;
                                }
                                if (elementValue.descriptor.type == IonTypeCode.list)
                                {
                                    error = IonErrorCode.sharedSymbolTablesAreUnsupported;
                                    goto C;
                                }
                                size_t id;
                                error = elementValue.trustedGet!IonSymbolID.get(id);
                                if (error)
                                    goto C;
                                if (id != IonSystemSymbol.ion_symbol_table)
                                {
                                    error = IonErrorCode.invalidLocalSymbolTable;
                                    goto C;
                                }
                                preserveCurrentSymbols = true;
                                break;
                            }
                            case IonSystemSymbol.symbols:
                            {
                                if (symbols != symbols.init || elementValue.descriptor.type != IonTypeCode.list)
                                {
                                    error = IonErrorCode.invalidLocalSymbolTable;
                                    goto C;
                                }
                                if (elementValue != null)
                                {
                                    symbols = elementValue.trustedGet!IonList;
                                }
                                if (error)
                                    goto C;
                                break;
                            }
                            default:
                            {
                                //CHECK: should other symbols be ignored?
                                continue;
                            }
                        }
                    }

                    if (!preserveCurrentSymbols)
                    {
                        resetSymbolTable();
                    }

                    foreach (IonErrorCode symbolsError, IonDescribedValue symbolValue; symbols)
                    {
                        error = symbolsError;
                        if (error)
                            goto C;
                        const(char)[] symbol;
                        error = symbolValue.get(symbol);
                        if (error)
                            goto C;
                        symbolTableBuffer.put(symbol);
                    }
                }
            }
            this.table = this.symbolTableBuffer.data;
            // TODO: continue work
            C:
                return error;
        }
        return error;
    }

    ref auto opIndex(size_t index)
    {
        return this.table[index];
    }

    auto toBytes() {
        IonSymbolTable!true tmptable;
        tmptable.initialize;
        foreach (const(char[]) key; this.table[10..$])
        {
            tmptable.insert(key);
        }
        tmptable.finalize;
        return cast(const(ubyte)[]) tmptable.data;
    }

}
