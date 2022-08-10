module drocks.iter;

import rocksdb;

import std.conv : to;
import std.string : fromStringz, toStringz;

import drocks.options : ReadOptions;
import drocks.database : RocksDB;
import drocks.columnfamily : ColumnFamily;
import drocks.memory;

struct Iterator
{
    RocksDB* db;
    ReadOptions* opts;
    ColumnFamily* family;
    bool forward;
    bool end;

    ubyte[] frontKey;
    ubyte[] backKey;

    SafePtr!(rocksdb_iterator_t, rocksdb_iter_destroy) iter;

    this(this)
    {
        this.iter = iter;
    }

    this(RocksDB* db, ReadOptions* opts, ColumnFamily* family = null)
    {
        this.db = db;
        this.opts = opts;
        if (family)
        {
            this.family = family;
            this.iter = rocksdb_create_iterator_cf(db.db, this.opts.opts, this.family.cf);
        }
        else
            this.iter = rocksdb_create_iterator(db.db, this.opts.opts);
        this.forward = true;
        this.seekToLast;
        this.backKey = this.key.dup;
        this.seekToFirst();
        this.frontKey = this.key.dup;
    }

    auto save()
    {
        Iterator ret;
        ret = Iterator(this.db, this.opts, this.family);
        ret.frontKey = frontKey.dup;
        ret.backKey = backKey.dup;
        ret.forward = forward;
        ret.end = end;
        if (this.forward)
            ret.seek(this.frontKey);
        else
            ret.seek(this.backKey);
        return ret;
    }

    void seekToFirst()
    {
        rocksdb_iter_seek_to_first(this.iter);
    }

    void seekToLast()
    {
        rocksdb_iter_seek_to_last(this.iter);
    }

    void seek(string key)
    {
        this.seek(cast(ubyte[]) key);
    }

    void seek(in const(ubyte)[] key)
    {
        rocksdb_iter_seek(this.iter, cast(char*) key.ptr, key.length);
    }

    void seekPrev(string key)
    {
        this.seekPrev(cast(ubyte[]) key);
    }

    void seekPrev(in const(ubyte)[] key)
    {
        rocksdb_iter_seek_for_prev(this.iter, cast(char*) key.ptr, key.length);
    }

    /// remove first element
    void popFront()
    {
        /// if we are moving backwards
        /// seek to front and then iterate in reverse 
        if (!forward)
            seek(frontKey);

        this.forward = true;

        if (this.key == this.backKey)
        {
            this.end = true;
            return;
        }

        rocksdb_iter_next(this.iter);
        this.frontKey = this.key.dup;
    }

    /// remove last element
    void popBack()
    {
        /// if we are moving forward
        /// seek to back and then iterate in reverse 
        if (forward)
            seek(backKey);

        this.forward = false;

        if (this.key == this.frontKey)
        {
            this.end = true;
            return;
        }

        rocksdb_iter_prev(this.iter);
        this.backKey = this.key.dup;
    }

    bool empty()
    {
        return !cast(bool) rocksdb_iter_valid(this.iter) || end;
    }

    auto front()
    {
        if (!forward)
            seek(frontKey);
        this.forward = true;
        return [this.key, this.value];
    }

    auto back()
    {
        if (forward)
            seek(backKey);
        this.forward = false;
        return [this.key, this.value];
    }

    ubyte[] key()
    {
        size_t size;
        const(char)* ckey = rocksdb_iter_key(this.iter, &size);
        return cast(ubyte[]) ckey[0 .. size];
    }

    ubyte[] value()
    {
        size_t size;
        const(char)* cvalue = rocksdb_iter_value(this.iter, &size);
        return cast(ubyte[]) cvalue[0 .. size];
    }

    ref auto lt(ubyte[] key)
    {
        this.seekPrev(key);
        this.backKey = this.key.dup;
        if (key >= this.key)
            this.popBack;
        this.seek(frontKey);
        return this;
    }

    ref auto lte(ubyte[] key)
    {
        this.seekPrev(key);
        this.backKey = this.key.dup;
        if (key > this.key)
            this.popBack;
        this.seek(frontKey);
        return this;
    }

    ref auto gt(ubyte[] key)
    {
        this.seek(key);
        this.frontKey = this.key.dup;
        if (key <= this.key)
            this.popFront;
        return this;
    }

    ref auto gte(ubyte[] key)
    {
        this.seek(key);
        this.frontKey = this.key.dup;
        if (key < this.key)
            this.popFront;
        return this;
    }

    int opApply(scope int delegate(ubyte[], ubyte[]) dg)
    {
        int result = 0;

        while (!this.empty())
        {
            result = dg(this.key(), this.value());
            if (result)
                break;
            this.popFront();
        }

        return result;
    }

}

unittest
{
    import std.stdio : writefln;
    import std.datetime.stopwatch : benchmark;
    import drocks.env : Env;
    import drocks.options;
    import std.file;
    import std.path;
    import std.algorithm : map;
    import std.range : retro;
    import std.array : array;

    writefln("Testing Iterator");

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

    if ("/tmp/test_rocksdb_itr".exists)
        rmdirRecurse("/tmp/test_rocksdb_itr");

    auto db = RocksDB(opts, "/tmp/test_rocksdb_itr");

    // Test string putting and getting
    db[cast(ubyte[]) "key1"] = cast(ubyte[]) "1";
    db[cast(ubyte[]) "key2"] = cast(ubyte[]) "2";
    db[cast(ubyte[]) "key3"] = cast(ubyte[]) "3";
    db[cast(ubyte[]) "key4"] = cast(ubyte[]) "4";
    db[cast(ubyte[]) "key5"] = cast(ubyte[]) "5";
    db[cast(ubyte[]) "key6"] = cast(ubyte[]) "6";

    assert(db.iter.map!(x => x[0].idup).array == [
            "key1", "key2", "key3", "key4", "key5", "key6"
            ]);
    assert(db.iter.retro.map!(x => x[0].idup).array == [
            "key6", "key5", "key4", "key3", "key2", "key1"
            ]);

    auto itr = db.iter;
    itr.popFront;
    itr.popFront;

    auto itr2 = itr.save;
    assert(itr2.map!(x => x[0].idup).array == ["key3", "key4", "key5", "key6"]);
    assert(itr2.retro.map!(x => x[0].idup).array == [
            "key6", "key5", "key4", "key3"
            ]);

    auto itr3 = itr.save;

    itr3.popBack;
    itr3.popBack;

    assert(itr3.map!(x => x[0].idup).array == ["key3", "key4"]);
    assert(itr3.retro.map!(x => x[0].idup).array == ["key4", "key3"]);

    assert(db.iter.gt(cast(ubyte[]) "key2").map!(x => x[0].idup)
            .array == ["key3", "key4", "key5", "key6"]);
    assert(db.iter.gt(cast(ubyte[]) "key2.1").map!(x => x[0].idup)
            .array == ["key3", "key4", "key5", "key6"]);

    assert(db.iter.gte(cast(ubyte[]) "key2").map!(x => x[0].idup)
            .array == ["key2", "key3", "key4", "key5", "key6"]);
    assert(db.iter.gte(cast(ubyte[]) "key1.5").map!(x => x[0].idup)
            .array == ["key2", "key3", "key4", "key5", "key6"]);

    assert(db.iter.lt(cast(ubyte[]) "key1").map!(x => x[0].idup).array == []);
    assert(db.iter.lt(cast(ubyte[]) "key2").map!(x => x[0].idup).array == [
            "key1",
            ]);
    assert(db.iter.lt(cast(ubyte[]) "key2.1").map!(x => x[0].idup).array == [
            "key1", "key2"
            ]);

    assert(db.iter.lte(cast(ubyte[]) "key2").map!(x => x[0].idup).array == [
            "key1", "key2"
            ]);
    assert(db.iter.lte(cast(ubyte[]) "key1.5").map!(x => x[0].idup).array == [
            "key1"
            ]);
}
