module drocks.merge;

import rocksdb;
import drocks.options;
import core.stdc.stdlib;

extern (C) @system nothrow @nogc
{

    alias FullMergeFn = char* function(void*, const(char)* key, size_t key_length,
            const(char)* existing_value, size_t existing_value_length, const(char*)* operands_list,
            const(size_t)* operands_list_length, int num_operands,
            ubyte* success, size_t* new_value_length);
    alias MergeDtr = void function(void*);
    alias PartialMergeFn = char* function(void*, const(char)* key, size_t key_length,
            const(char*)* operands_list, const(size_t)* operands_list_length,
            int num_operands, ubyte* success, size_t* new_value_length);
    alias DeleteFn = void function(void*, const(char)* value, size_t value_length);
    alias NameFn = const(char)* function(void*);

    struct MergeOperator
    {
        void* state;

        MergeDtr destructor;
        FullMergeFn full_merge;
        PartialMergeFn partial_merge;
        DeleteFn delete_value;
        NameFn name;

    }

    struct merge_operator_state
    {
        int val;
    }

    char* AppendFullMerge(void* state, const(char)* key, size_t key_length,
            const(char)* existing_value, size_t existing_value_length, const(char*)* operands_list,
            const(size_t)* operands_list_length, int num_operands,
            ubyte* success, size_t* new_value_length)
    {
        import core.stdc.stdio;

        auto sum = 0;
        for (int i = 0; i < num_operands; i++)
        {
            sum += operands_list_length[i];
        }
        auto newLen = existing_value_length + sum;
        *new_value_length = newLen;
        auto ret = (cast(char*) malloc(newLen))[0 .. newLen];
        size_t start = 0;
        size_t end = existing_value_length;
        ret[start .. end] = existing_value[start .. end];
        for (auto i = 0; i < num_operands; i++)
        {
            start = end;
            end += operands_list_length[i];
            ret[start .. end] = operands_list[i][0 .. operands_list_length[i]];
        }
        *success = 1;
        return ret.ptr;
    }

    char* AppendPartialMerge(void* state, const(char)* key, size_t key_length,
            const(char*)* operands_list, const(size_t)* operands_list_length,
            int num_operands, ubyte* success, size_t* new_value_length)
    {
        import core.stdc.stdio;

        auto newLen = 0;
        for (auto i = 0; i < num_operands; i++)
        {
            newLen += operands_list_length[i];
        }
        *new_value_length = newLen;
        auto ret = (cast(char*) malloc(newLen))[0 .. newLen];
        size_t start, end = 0;
        for (auto i = 0; i < num_operands; i++)
        {
            end += operands_list_length[i];
            ret[start .. end] = operands_list[i][0 .. operands_list_length[i]];
            start = end;
        }
        *success = 1;
        return ret.ptr;
    }

    void AppendDtr(void* state)
    {

    }

    const(char)* AppendName(void* state)
    {
        return "AppendOperator\0";
    }

}

auto createAppendMergeOperator()
{
    auto state = cast(merge_operator_state*) malloc(merge_operator_state.sizeof);
    return rocksdb_mergeoperator_create(state, &AppendDtr, &AppendFullMerge,
            &AppendPartialMerge, null, &AppendName);
}

unittest
{
    import std.stdio : writefln;
    import std.datetime.stopwatch : benchmark;
    import drocks.env : Env;
    import drocks.database : RocksDB;
    import drocks.columnfamily : ColumnFamily;

    writefln("Testing Database merge ops");

    Env env;
    env.initialize;
    env.backgroundThreads = 2;
    env.highPriorityBackgroundThreads = 1;

    RocksDBOptions opts;
    opts.initialize;
    opts.createIfMissing = true;
    opts.errorIfExists = false;
    opts.compression = CompressionType.None;
    opts.env = env;
    opts.setMergeOperator(createAppendMergeOperator());

    auto db = RocksDB(opts, "/tmp/test_rocksdb_merge");

    // Test string putting and getting
    db[cast(ubyte[]) "key"] = cast(ubyte[]) "value";
    assert(db[cast(ubyte[]) "key"].unwrap.unwrap == cast(ubyte[]) "value");
    db[cast(ubyte[]) "key"] = cast(ubyte[]) "value2";
    assert(db[cast(ubyte[]) "key"].unwrap.unwrap == cast(ubyte[]) "value2");

    db[cast(ubyte[]) "key"] ~= cast(ubyte[]) "value3";
    db[cast(ubyte[]) "key"] ~= cast(ubyte[]) "value4";

    db[cast(ubyte[]) "key2"] ~= cast(ubyte[]) "value3";
    db[cast(ubyte[]) "key2"] ~= cast(ubyte[]) "value4";

    assert(db[cast(ubyte[]) "key"].unwrap.unwrap == cast(ubyte[]) "value2value3value4");
    assert(db[cast(ubyte[]) "key2"].unwrap.unwrap == cast(ubyte[]) "value3value4");

    import std.file;

    rmdirRecurse("/tmp/test_rocksdb_merge");
}
