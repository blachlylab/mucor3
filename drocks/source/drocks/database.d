module drocks.database;

import rocksdb;

import drocks.options;
import drocks.columnfamily;
import drocks.iter;
import drocks.memory;

import option;

import std.conv : to;
import std.file : isDir, exists;
import std.array : array;
import std.string : fromStringz, toStringz;
import std.format : format;

import core.memory : GC;
import core.stdc.string : strlen;

alias RocksResult(T) = Result!(T, string);
alias RocksError = Result!(typeof(null), string);

auto convertErr(char * err) {
    return format("Error: %s", fromStringz(err));
}

struct RocksDB {

    rocksdb_t* db;

    RocksDBOptions opts;
    WriteOptions writeOptions;
    ReadOptions readOptions;

    ColumnFamily[string] columnFamilies;

    @disable this(this);

    this(RocksDBOptions opts, string path, RocksDBOptions[string] columnFamilies = null) {
        char* err = null;
        this.opts = opts;

        string[] existingColumnFamilies;

        // If there is an existing database we can check for existing column families
        if (exists(path) && isDir(path)) {
            // First check if the database has any column families
            existingColumnFamilies = RocksDB.listColumnFamilies(opts, path);
        }

        if (columnFamilies || existingColumnFamilies.length >= 1) {
            immutable(char*)[] columnFamilyNames;
            rocksdb_options_t*[] columnFamilyOptions;

            foreach (k; existingColumnFamilies) {
                columnFamilyNames ~= toStringz(k);

                if ((k in columnFamilies) !is null) {
                    columnFamilyOptions ~= columnFamilies[k].opts;
                } else {
                    columnFamilyOptions ~= opts.opts;
                }
            }

            rocksdb_column_family_handle_t*[] result;
            result.length = columnFamilyNames.length;

            this.db = rocksdb_open_column_families(
                opts.opts,
                toStringz(path),
                cast(int)columnFamilyNames.length,
                columnFamilyNames.ptr,
                columnFamilyOptions.ptr,
                result.ptr,
                &err);

            foreach (idx, handle; result) {
                this.columnFamilies[existingColumnFamilies[idx]] = ColumnFamily(
                    this,
                    existingColumnFamilies[idx],
                    handle,
                );
            }
        } else {
            this.db = rocksdb_open(opts.opts, toStringz(path), &err);
        }

        if(err) {
            throw new Exception(format("Error: %s", fromStringz(err)));
        }

        this.writeOptions.initialize;
        this.readOptions.initialize;
    }

    ~this() {
        if (this.db) {
        foreach (k, ref v; this.columnFamilies) {
            rocksdb_column_family_handle_destroy(v.cf);
        }

        rocksdb_close(this.db);
        }
    }

    RocksResult!(ColumnFamily *) createColumnFamily(string name) {
        RocksResult!(ColumnFamily *) ret;
        char* err = null;

        auto cfh = rocksdb_create_column_family(this.db, this.opts.opts, toStringz(name), &err);
        if(err) {
            ret = Err(format("Error: %s", fromStringz(err)));
        } else {
            this.columnFamilies[name] = ColumnFamily(this, name, cfh);
            ret = Ok(&this.columnFamilies[name]);
        }

        return ret;
    }
    RocksResult!(ColumnFamily*) createColumnFamily(string name, RocksDBOptions opts) {
        RocksResult!(ColumnFamily*) ret;
        char* err = null;

        auto cfh = rocksdb_create_column_family(this.db, opts.opts, toStringz(name), &err);
        if(err) {
            ret = Err(format("Error: %s", fromStringz(err)));
        } else {
            this.columnFamilies[name] = ColumnFamily(this, name, cfh);
            ret = Ok(&this.columnFamilies[name]);
        }

        return ret;
    }

    static string[] listColumnFamilies(RocksDBOptions opts, string path) {
        char* err = null;
        size_t numColumnFamilies;

        char** columnFamilies = rocksdb_list_column_families(
            opts.opts,
            toStringz(path),
            &numColumnFamilies,
            &err
        );

        if(err) {
            throw new Exception(format("Error: %s", fromStringz(err)));
        }

        string[] result = new string[](numColumnFamilies);

        // Iterate over and convert/copy all column family names
        for (size_t i = 0; i < numColumnFamilies; i++) {
            result[i] = fromStringz(columnFamilies[i]).to!string;
        }

        rocksdb_list_column_families_destroy(columnFamilies, numColumnFamilies);

        return result;
    }

    ref auto opIndex(const(ubyte)[] key)
    {
        return this.get(key);
    }

    ref auto opIndex(const(ubyte)[] key, string familyName)
    {
        return this.get(key, &this.columnFamilies[familyName]);
    }

    RocksResult!(Option!(ubyte[])) get(const(ubyte)[] key, ColumnFamily * family = null) {
        RocksResult!(Option!(ubyte[])) ret;
        size_t len;
        char* err;
        ubyte* value;
        if(family) {
            value = cast(ubyte*)rocksdb_get_cf(
                this.db,
                this.readOptions.opts,
                family.cf,
                cast(char*)key.ptr,
                key.length,
                &len,
                &err
            );
        } else {
            value = cast(ubyte*)rocksdb_get(
                this.db,
                this.readOptions.opts,
                cast(char*)key.ptr,
                key.length,
                &len,
                &err
            );
        }
        if(err) {
            ret = Err(format("Error: %s", fromStringz(err)));
        } else {
            Option!(ubyte[]) val;
            if(value) {
                GC.addRange(value, len);
                val = Some(cast(ubyte[])value[0..len]);
            } else {
                val = None;
            }
            ret = Ok(val);
        }
        return ret;
    }

    RocksError opIndexAssign(const(ubyte)[] value, const(ubyte)[] key)
    {
        return this.put(value, key);
    }

    RocksError opIndexAssign(const(ubyte)[] value, const(ubyte)[] key, string familyName)
    {
        return this.put(value, key, &this.columnFamilies[familyName]);
    }

    RocksError opIndexOpAssign(string op: "~")(const(ubyte)[] value, const(ubyte)[] key)
    {
        return this.merge(value, key, null);
    }

    RocksError put(const(ubyte)[] value, const(ubyte)[] key, ColumnFamily * family = null) {
        RocksError ret;
        ret = Ok(null);
        char* err;
        if(family) {
            rocksdb_put_cf(this.db,
                this.writeOptions.opts,
                family.cf,
                cast(char*)key.ptr, key.length,
                cast(char*)value.ptr, value.length,
                &err);
        } else {
            rocksdb_put(this.db,
                this.writeOptions.opts,
                cast(char*)key.ptr, key.length,
                cast(char*)value.ptr, value.length,
                &err);
        }

        if(err) ret = Err(format("Error: %s", fromStringz(err)));
        return ret;
    }

    RocksError remove(const(ubyte)[] key, string familyName) {
        return this.remove_(key, &this.columnFamilies[familyName]);
    }

    RocksError remove(const(ubyte)[] key) {
        return this.remove_(key, null);
    }

    RocksError remove_(const(ubyte)[] key, ColumnFamily * family = null) {
        RocksError ret;
        ret = Ok(null);
        char* err;

        if (family) {
            rocksdb_delete_cf(
                this.db,
                this.writeOptions.opts,
                family.cf,
                cast(char*)key.ptr,
                key.length,
                &err);
        } else {
            rocksdb_delete(
                this.db,
                this.writeOptions.opts,
                cast(char*)key.ptr,
                key.length,
                &err);
        }
        if(err) ret = Err(format("Error: %s", fromStringz(err)));
        return ret;
    }

    RocksError merge(const(ubyte)[] value, const(ubyte)[] key, ColumnFamily * family = null) {
        RocksError ret;
        ret = Ok(null);
        char* err;

        if (family) {
            rocksdb_merge_cf(
                this.db,
                this.writeOptions.opts,
                family.cf,
                cast(char*)key.ptr,
                key.length,
                cast(char*)value.ptr,
                value.length,
                &err);
        } else {
            rocksdb_merge(
                this.db,
                this.writeOptions.opts,
                cast(char*)key.ptr,
                key.length,
                cast(char*)value.ptr,
                value.length,
                &err);
        }
        if(err) ret = Err(format("Error: %s", fromStringz(err)));
        return ret;
    }

    // ubyte[][] multiGet(ubyte[][] keys, ColumnFamily * family = null, ReadOptions * opts = null) {
    //     char*[] ckeys = new char*[](keys.length);
    //     size_t[] ckeysSizes = new size_t[](keys.length);

    //     foreach (idx, key; keys) {
    //         ckeys[idx] = cast(char*)key;
    //         ckeysSizes[idx] = key.length;
    //     }

    //     char*[] vals = new char*[](keys.length);
    //     size_t[] valsSizes = new size_t[](keys.length);
    //     char*[] errs = new char*[](keys.length);

    //     if (family) {
    //         rocksdb_multi_get_cf(
    //             this.db,
    //             (opts ? opts : this.readOptions).opts,
    //             family.cf,
    //             keys.length,
    //             ckeys.ptr,
    //             ckeysSizes.ptr,
    //             vals.ptr,
    //             valsSizes.ptr,
    //             errs.ptr);
    //     } else {
    //         rocksdb_multi_get(
    //             this.db,
    //             (opts ? opts : this.readOptions).opts,
    //             keys.length,
    //             ckeys.ptr,
    //             ckeysSizes.ptr,
    //             vals.ptr,
    //             valsSizes.ptr,
    //             errs.ptr);
    //     }

    //     ubyte[][] result = new ubyte[][](keys.length);
    //     for (int idx = 0; idx < ckeys.length; idx++) {
    //         errs[idx].checkErr;
    //         result[idx] = cast(ubyte[])vals[idx][0..valsSizes[idx]];
    //     }

    //     return result;
    // }

    // string[] multiGetString(string[] keys, ColumnFamily * family = null, ReadOptions * opts = null) {
    //     char*[] ckeys = new char*[](keys.length);
    //     size_t[] ckeysSizes = new size_t[](keys.length);

    //     foreach (idx, key; keys) {
    //         ckeys[idx] = cast(char*)key.ptr;
    //         ckeysSizes[idx] = key.length;
    //     }

    //     char*[] vals = new char*[](keys.length);
    //     size_t[] valsSizes = new size_t[](keys.length);
    //     char*[] errs = new char*[](keys.length);

    //     if (family) {
    //         rocksdb_multi_get_cf(
    //             this.db,
    //             (opts ? opts : this.readOptions).opts,
    //             family.cf,
    //             keys.length,
    //             ckeys.ptr,
    //             ckeysSizes.ptr,
    //             vals.ptr,
    //             valsSizes.ptr,
    //             errs.ptr);
    //     } else {
    //         rocksdb_multi_get(
    //             this.db,
    //             (opts ? opts : this.readOptions).opts,
    //             keys.length,
    //             ckeys.ptr,
    //             ckeysSizes.ptr,
    //             vals.ptr,
    //             valsSizes.ptr,
    //             errs.ptr);
    //     }

    //     string[] result = new string[](keys.length);
    //     for (int idx = 0; idx < ckeys.length; idx++) {
    //         errs[idx].checkErr;
    //         result[idx] = cast(string)vals[idx][0..valsSizes[idx]];
    //     }

    //     return result;
    // }

    // void write(WriteBatch batch, WriteOptions * opts = null) {
    //     char* err;
    //     rocksdb_write(this.db, (opts ? opts : &this.writeOptions).opts, batch.batch, &err);
    //     err.checkErr();
    // }

    Iterator iter() {
        return Iterator(&this, &this.readOptions);
    }

    Iterator iter(ref ReadOptions opts) {
        return Iterator(&this, &opts);
    }

    void withIter(void delegate(ref Iterator) dg, ReadOptions opts) {
        Iterator iter = this.iter(opts);
        scope (exit) destroy(iter);
        dg(iter);
    }

    // void withBatch(void delegate(WriteBatch) dg, WriteOptions * opts = null) {
    //     WriteBatch batch;
    //     scope (exit) destroy(batch);
    //     scope (success) this.write(batch, opts);
    //     dg(batch);
    // }
}

unittest {
    import std.stdio : writefln;
    import std.datetime.stopwatch : benchmark;
    import drocks.env : Env;
    import std.file;
    import std.path;

    writefln("Testing Database");

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

    if("/tmp/test_rocksdb".exists)
        rmdirRecurse("/tmp/test_rocksdb");
    
    auto db = RocksDB(opts, "/tmp/test_rocksdb");

    // Test string putting and getting
    db[cast(ubyte[])"key"] = cast(ubyte[])"value";
    assert(db[cast(ubyte[]) "key"].unwrap.unwrap == cast(ubyte[]) "value");
    db[cast(ubyte[])"key"] = cast(ubyte[])"value2";
    assert(db[cast(ubyte[]) "key"].unwrap.unwrap == cast(ubyte[]) "value2");

    ubyte[] key = ['\x00', '\x00'];
    ubyte[] value = ['\x01', '\x02'];

    // Test byte based putting / getting
    db[key] = value;
    assert(db[key].unwrap.unwrap == value);
    db.remove(key);

    // Benchmarks

    void writeBench(int times) {
        for (int i = 0; i < times; i++) {
            db[cast(ubyte[]) i.to!string] = cast(ubyte[]) i.to!string;
        }
    }

    void readBench(int times) {
        for (int i = 0; i < times; i++) {
            assert(db[cast(ubyte[]) i.to!string].unwrap.unwrap == cast(ubyte[]) i.to!string);
        }
    }

    auto writeRes = benchmark!(() => writeBench(100_000))(1);
    writefln("  writing a value 100000 times: %sms", writeRes[0].total!"msecs");

    auto readRes = benchmark!(() => readBench(100_000))(1);
    writefln("  reading a value 100000 times: %sms", readRes[0].total!"msecs");

    // // Test batch
    // void writeBatchBench(int times) {
    //     db.withBatch((batch) {
    //         for (int i = 0; i < times; i++) {
    //             batch.putString(i.to!string, i.to!string);
    //         }

    //         assert(batch.count() == times);
    //     });
    // }

    // auto writeBatchRes = benchmark!(() => writeBatchBench(100_000))(1);
    // writefln("  batch writing 100000 values: %sms", writeBatchRes[0].total!"msecs");
    // readBench(100_000);

    // Test scanning from a location
    bool found = false;
    auto iterFrom = db.iter();
    iterFrom.seek("key");
    foreach (key, value; iterFrom) {
        assert(value == "value2");
        assert(!found);
        found = true;
    }
    assert(found);

    found = false;
    int keyCount = 0;
    auto iter = db.iter();

    foreach (key, value; iter) {
        if (key == "key") {
            assert(value == "value2");
            found = true;
        }
        keyCount++;
    }
    assert(found);
    assert(keyCount == 100001);
}
