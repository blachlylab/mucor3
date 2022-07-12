module drocks.options;

import rocksdb;

enum CompressionType : int {
    None = 0x0,
    Snappy = 0x1,
    Zlib = 0x2,
    Bzip2 = 0x3,
    Lz4 = 0x4,
    Lz4hc = 0x5,
    Xpress = 0x6,
    Zstd = 0x7,
}

enum CompactionStyle : int {
    Level = 0,
    Universal = 1,
    Fifo = 2,
}

enum ReadTier : int {
    ReadAll = 0x0,
    BlockCache = 0x1,
    Persisted = 0x2,
}

struct WriteOptions {
    rocksdb_writeoptions_t* opts;

    this() {
        this.opts = rocksdb_writeoptions_create();
    }

    ~this() {
        rocksdb_writeoptions_destroy(this.opts);
    }

    @property void sync(bool v) {
        rocksdb_writeoptions_set_sync(this.opts, cast(ubyte)v);
    }

    @property void disableWAL(bool v) {
        rocksdb_writeoptions_disable_WAL(this.opts, cast(int)v);
    }
}

struct ReadOptions {
    rocksdb_readoptions_t* opts;

    this() {
        this.opts = rocksdb_readoptions_create();
    }

    ~this() {
        rocksdb_readoptions_destroy(this.opts);
    }

    @property void verifyChecksums(bool v) {
        rocksdb_readoptions_set_verify_checksums(this.opts, cast(ubyte)v);
    }

    @property void fillCache(bool v) {
        rocksdb_readoptions_set_fill_cache(this.opts, cast(ubyte)v);
    }

    @property void readTier(ReadTier tier) {
        rocksdb_readoptions_set_read_tier(this.opts, tier);
    }

    @property void tailing(bool v) {
        rocksdb_readoptions_set_tailing(this.opts, cast(ubyte)v);
    }

    @property void readAheadSize(size_t size) {
        rocksdb_readoptions_set_readahead_size(this.opts, size);
    }

    @property void snapshot(Snapshot snap) {
        rocksdb_readoptions_set_snapshot(this.opts, snap.snap);
    }
}

struct DBOptions {
    rocksdb_options_t* opts;

    this() {
        this.opts = rocksdb_options_create();
    }

    ~this() {
        rocksdb_options_destroy(this.opts);
    }

    @property void parallelism(int totalThreads) {
        rocksdb_options_increase_parallelism(this.opts, totalThreads);
    }

    @property void createIfMissing(bool value) {
        rocksdb_options_set_create_if_missing(this.opts, cast(ubyte)value);
    }

    @property void createMissingColumnFamilies(bool value) {
        rocksdb_options_set_create_missing_column_families(this.opts, cast(ubyte)value);
    }

    @property void errorIfExists(bool value) {
        rocksdb_options_set_error_if_exists(this.opts, cast(ubyte)value);
    }

    @property void paranoidChecks(bool value) {
        rocksdb_options_set_paranoid_checks(this.opts, cast(ubyte)value);
    }

    @property void env(Env env) {
        rocksdb_options_set_env(this.opts, env.env);
    }

    @property void compression(CompressionType type) {
        rocksdb_options_set_compression(this.opts, type);
    }

    @property void compactionStyle(CompactionStyle style) {
        rocksdb_options_set_compaction_style(this.opts, style);
    }

    @property void comparator(Comparator cmp) {
        rocksdb_options_set_comparator(this.opts, cmp.cmp);
    }

    void enableStatistics() {
        rocksdb_options_enable_statistics(this.opts);
    }

    string getStatisticsString() {
        char* cresult = rocksdb_options_statistics_get_string(this.opts);
        string result = fromStringz(cresult).to!string;
        cfree(cresult);
        return result;
    }
}