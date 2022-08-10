module drocks.columnfamily;

import rocksdb;
import option;
import std.format;
import std.string;

import drocks.options : ReadOptions, WriteOptions;
import drocks.iter : Iterator;
import drocks.database : RocksDB, convertErr, RocksResult, RocksError;

struct ColumnFamily
{

    RocksDB* db;
    string name;
    rocksdb_column_family_handle_t* cf;

    @disable this(this);

    this(ref RocksDB db, string name, rocksdb_column_family_handle_t* cf)
    {
        this.db = &db;
        this.name = name;
        this.cf = cf;
    }

    Iterator iter()
    {
        return Iterator(this.db, &this.db.readOptions, &this);
    }

    Iterator iter(ref ReadOptions opts)
    {
        return Iterator(this.db, &opts, &this);
    }

    void withIter(void delegate(ref Iterator) dg)
    {
        Iterator iter = this.iter();
        scope (exit)
            destroy(iter);
        dg(iter);
    }

    void withIter(void delegate(ref Iterator) dg, ReadOptions opts)
    {
        Iterator iter = this.iter(opts);
        scope (exit)
            destroy(iter);
        dg(iter);
    }

    RocksError drop()
    {
        RocksError ret;
        char* err = null;
        rocksdb_drop_column_family(this.db.db, this.cf, &err);
        if (err)
            ret = Err(format("Error: %s", fromStringz(err)));
        else
            ret = Ok(null);
        return ret;
    }

    ref auto opIndex(const(ubyte)[] key)
    {
        return this.db.get(key, &this);
    }

    void opIndexAssign(const(ubyte)[] value, const(ubyte)[] key)
    {
        this.db.put(value, key, &this);
    }

    void remove(const(ubyte)[] key)
    {
        this.db.remove_(key, &this);
    }

    void opIndexOpAssign(string op : "~")(const(ubyte)[] value, const(ubyte)[] key)
    {
        this.db.merge(value, key, &this);
    }

    // ubyte[][] multiGet(ubyte[][] keys, ReadOptions * opts = null) {
    //     return this.db.multiGet(keys, this, opts);
    // }

    // string[] multiGetString(string[] keys, ReadOptions * opts = null) {
    //     return this.db.multiGetString(keys, this, opts);
    // }

    // ubyte[] getImpl(ubyte[] key, ColumnFamily * family, ReadOptions * opts = null) {
    //     assert(*family == this || family is null);
    //     return this.db.getImpl(key, this, opts);
    // }

    // void putImpl(ubyte[] key, ubyte[] value, ColumnFamily * family, WriteOptions * opts = null) {
    //     assert(*family == this || family is null);
    //     this.db.putImpl(key, value, this, opts);
    // }

    // void removeImpl(ubyte[] key, ColumnFamily * family, WriteOptions * opts = null) {
    //     assert(*family == this || family is null);
    //     this.db.removeImpl(key, this, opts);
    // }
}

unittest
{
    import std.stdio : writefln;
    import std.datetime.stopwatch : benchmark;
    import std.conv : to;
    import std.algorithm.searching : startsWith;
    import drocks.options : RocksDBOptions, CompressionType;
    import std.file;
    import std.path;

    if ("/tmp/test_rocksdb_cf".exists)
        rmdirRecurse("/tmp/test_rocksdb_cf");

    writefln("Testing Column Families");

    // DB Options
    RocksDBOptions opts;
    opts.initialize;
    opts.createIfMissing = true;
    opts.errorIfExists = false;
    opts.compression = CompressionType.None;

    string[] columnFamilies = [
        "test", "test1", "test2", "test3", "test4", "wow",
    ];
    {
        // Create the database (if it does not exist)
        auto db = RocksDB(opts, "/tmp/test_rocksdb_cf");

        // create a bunch of column families
        foreach (cf; columnFamilies)
        {
            if ((cf in db.columnFamilies) is null)
            {
                db.createColumnFamily(cf);
            }
        }
    }
    auto db = RocksDB(opts, "/tmp/test_rocksdb_cf");

    // Test column family listing
    assert(RocksDB.listColumnFamilies(opts, "/tmp/test_rocksdb_cf")
            .length == columnFamilies.length + 1);

    void testColumnFamily(ref ColumnFamily cf, int times)
    {
        for (int i = 0; i < times; i++)
        {
            cf[cast(ubyte[])(cf.name ~ i.to!string)] = cast(ubyte[]) i.to!string;
        }

        for (int i = 0; i < times; i++)
        {
            assert(cf[cast(ubyte[])(cf.name ~ i.to!string)].unwrap.unwrap == cast(
                    ubyte[]) i.to!string);
        }

        cf.withIter((ref iter) {
            foreach (key, value; iter)
            {
                assert(key.startsWith(cf.name));
            }
        });
    }

    foreach (name, ref cf; db.columnFamilies)
    {
        if (name == "default")
            continue;

        writefln("  %s", name);
        testColumnFamily(cf, 1000);
    }
}
