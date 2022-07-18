module drocks.iter;

import rocksdb;

import std.conv : to;
import std.string : fromStringz, toStringz;

import drocks.options : ReadOptions;
import drocks.database : RocksDB;
import drocks.columnfamily : ColumnFamily;
import drocks.memory;

struct Iterator {
    SafePtr!(rocksdb_iterator_t, rocksdb_iter_destroy) iter;

    this(this) {
        this.iter = iter;
    }

    this(RocksDB * db, ReadOptions opts) {
        this.iter = rocksdb_create_iterator(db.db, opts.opts);
        this.seekToFirst();
    }

    this(RocksDB * db, ColumnFamily * family, ReadOptions opts) {
        this.iter = rocksdb_create_iterator_cf(db.db, opts.opts, family.cf);
        this.seekToFirst();
    }

    void seekToFirst() {
        rocksdb_iter_seek_to_first(this.iter);
    }

    void seekToLast() {
        rocksdb_iter_seek_to_last(this.iter);
    }

    void seek(string key) {
        this.seek(cast(ubyte[])key);
    }

    void seek(in const(ubyte)[] key) {
        rocksdb_iter_seek(this.iter, cast(char*)key.ptr, key.length);
    }

    void seekPrev(string key) {
        this.seekPrev(cast(ubyte[])key);
    }

    void seekPrev(in const(ubyte)[] key) {
        rocksdb_iter_seek_for_prev(this.iter, cast(char*)key.ptr, key.length);
    }

    void popFront() {
        rocksdb_iter_next(this.iter);
    }

    void popBack() {
        rocksdb_iter_prev(this.iter);
    }

    bool empty() {
        return !cast(bool)rocksdb_iter_valid(this.iter);
    }
    
    auto front() {
        return [this.key, this.value];
    }

    ubyte[] key() {
        size_t size;
        const(char)* ckey = rocksdb_iter_key(this.iter, &size);
        return cast(ubyte[])ckey[0..size];
    }

    ubyte[] value() {
        size_t size;
        const(char)* cvalue = rocksdb_iter_value(this.iter, &size);
        return cast(ubyte[])cvalue[0..size];
    }

    int opApply(scope int delegate(ubyte[], ubyte[]) dg) {
        int result = 0;

        while (!this.empty()) {
            result = dg(this.key(), this.value());
            if (result) break;
            this.popFront();
        }

        return result;
    }

}