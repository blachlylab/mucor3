module drocks.snapshot;

import rocksdb;
import drocks.database;
import drocks.memory;

struct Snapshot {
    RocksDB * db;
    const(rocksdb_snapshot_t)* snap;

    @disable this(this);
    
    this(ref RocksDB db) {
        this.db = &db;
        this.snap = rocksdb_create_snapshot(db.db);
    }

    ~this() {
        if(snap) rocksdb_release_snapshot(this.db.db, this.snap);
    }
}