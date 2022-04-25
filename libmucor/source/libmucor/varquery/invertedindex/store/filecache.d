module libmucor.varquery.invertedindex.store.filecache;
import libmucor.varquery.invertedindex.store.binary;
import libmucor.wideint;
import libmucor.khashl;
import std.container: BinaryHeap;
import core.stdc.stdlib: malloc, free;
import std.format: format;
import htslib.hts_log;
import std.algorithm: filter, map;
import std.array: array;

struct AccessedIdFile {
    uint128 id; 
    ulong accessCount;
    BinaryStore!ulong file;

    void openWrite(string prefix, uint128 id) {
        this.id = id;
        this.file = BinaryStore!ulong(format("%s_%x", prefix, id), "wbu");
    }

    void openRead(string prefix, uint128 id) {
        this.id = id;
        this.file = BinaryStore!ulong(format("%s_%x", prefix, id), "rb");
    }

    void openAppend(string prefix, uint128 id) {
        this.id = id;
        this.file = BinaryStore!ulong(format("%s_%x", prefix, id), "ab");
    }

    void write(ulong id) {
        this.accessCount++;
        this.file.write(id);
    }

    auto getIds() {
        return this.file.getAll;
    }

    void close() {
        this.file.close;
    }
}

struct IdFileCacheWriter {
    
    /// cache keys that have a single id
    khashl!(uint128, ulong) singletons;

    /// ids with files that have been opened
    /// mutually exclusive with openedFiles
    /// 
    /// value is accessCount
    khashl!(uint128, ulong) openedFiles;

    /// ids with currently open files
    khashl!(uint128, AccessedIdFile * ) openFiles;

    BinaryHeap!(AccessedIdFile*[], "a.accessCount > b.accessCount") cache;
    ulong cacheSize;
    string prefix;

    this(string prefix, ulong cacheSize = 100000) {
        this.cacheSize = cacheSize;
        this.prefix = prefix;
        AccessedIdFile*[] cacheStore;
        this.cache = BinaryHeap!(AccessedIdFile*[], "a.accessCount > b.accessCount")(cacheStore);
    }

    void insert(uint128 key, ulong id) {
        auto p1 = key in openFiles;
        /// id file is currently open
        if(p1){
            hts_log_debug(__FUNCTION__, format("id %x found in cache", key));
            (*p1).write(id);
            return;
        }
        
        /// id file has been opened before
        auto p2 = key in openedFiles;
        if(p2) {
            hts_log_debug(__FUNCTION__, format("id %x has been opened prior", key));
            AccessedIdFile f;
            f.accessCount = *p2;
            f.openAppend(this.prefix, key);
            f.write(id);
            *p2 += 1;
            /// insert
            if(this.cache.length < this.cacheSize) {
                hts_log_debug(__FUNCTION__, format("inserting id %x in cache", key));
                AccessedIdFile * fp = cast(AccessedIdFile *)malloc(AccessedIdFile.sizeof);
                *fp = f;
                this.cache.insert(fp);
                this.openedFiles.remove(key);
                this.openFiles[key] = fp;
            /// replace lowest
            } else if(f.accessCount > this.cache.front.accessCount) {
                AccessedIdFile * fp = cast(AccessedIdFile *)malloc(AccessedIdFile.sizeof);
                *fp = f;
                /// replace lowest open file
                auto old_file = cache.front;
                this.openFiles.remove(old_file.id);
                this.cache.replaceFront(fp);
                hts_log_debug(__FUNCTION__, format("replaced id %x  with id %x in cache", old_file.id,  key));
                this.openFiles[key] = fp;
                this.openedFiles[old_file.id] = old_file.accessCount;
                old_file.close;
                free(old_file);
            } else {
                f.close();
            }
            return;
        }
        /// id file has not been opened
        auto p3 = key in singletons;
        if(p3) {
            hts_log_debug(__FUNCTION__, format("removing id %x from singletons", key));
            AccessedIdFile f;
            f.openWrite(this.prefix, key);
            f.write(*p3);
            f.write(id);
            f.close;
            this.singletons.remove(key);
            this.openedFiles[key] = 1;
        } else {
            hts_log_debug(__FUNCTION__, format("inserting id %x in singletons", key));
            singletons[key] = id;
        }
    }

    void close() {

        auto f = BinaryStore!uint128(this.prefix ~ ".hashes", "wbu");
        foreach(kv;openFiles.byKeyValue) {
            f.write(kv.key);
            (cast(AccessedIdFile*)kv.value).close();
            free(cast(AccessedIdFile*)kv.value);
        }
        auto sf = BinaryStore!SingletonId(this.prefix ~ ".singletons", "wbu");
        foreach (kv; this.singletons.byKeyValue)
        {
            sf.write(SingletonId(kv[0], kv[1]));
        }
        sf.close;
        
        foreach (k; this.openedFiles.byKey)
        {
            f.write(k);
        }
        f.close;
    }
}

struct IdFileCacheReader {
    
    /// cache keys that have a single id
    BinaryStore!SingletonId * singletons;

    khashlSet!(uint128) hashes;

    string prefix;

    this(string prefix) {
        this.prefix = prefix;
        this.singletons = new BinaryStore!SingletonId(this.prefix ~ ".singletons", "rb");
        auto f = BinaryStore!uint128(this.prefix ~ ".hashes", "rb");
        foreach (key; f.getAll)
        {
            this.hashes.insert(key);
        }
        f.close;
    }

    ulong[] getIds(uint128 key) {
        /// id file is currently open
        if(key in hashes){
            AccessedIdFile f;
            f.openRead(this.prefix, key);
            return f.getIds.array;
        }

        auto r = this.singletons.getAll.filter!(x => x.key == key);
        if(r.empty) {
            hts_log_warning(__FUNCTION__, format("Key %x not preset", key));
            return [];
        } else {
            return r.map!(x => x.id).array;
        }
    }

    void close() {
        this.singletons.close;
    }
}

unittest
{
    import htslib.hts_log;
    import std.stdio;
    import std.array: array;
    hts_set_log_level(htsLogLevel.HTS_LOG_DEBUG);
    {
        auto fcache = new IdFileCacheWriter("/tmp/test_fcache", 2);
        fcache.insert(uint128(0), 0); // 0 enters singletons

        assert(fcache.singletons.count == 1);

        fcache.insert(uint128(1), 1); // 1 enters singletons
        assert(fcache.singletons.count == 2);
        fcache.insert(uint128(1), 2); // 1 enters opened
        assert(fcache.singletons.count == 1);
        assert(fcache.openedFiles[uint128(1)] == 1);
        fcache.insert(uint128(1), 3); // 1 enters cache
        fcache.insert(uint128(1), 4);
        assert(fcache.cache.front.accessCount == 3);

        assert(fcache.openFiles[uint128(1)].accessCount == 3);
        assert(fcache.cache.length == 1);

        fcache.insert(uint128(2), 5); // 2 enters singletons
        assert(fcache.singletons.count == 2);
        fcache.insert(uint128(2), 6); // 2 enters opened
        assert(fcache.singletons.count == 1);
        assert(fcache.openedFiles[uint128(2)] == 1);
        fcache.insert(uint128(2), 7); // 2 enters cache (cache full)

        assert(fcache.cache.front.accessCount == 2);
        assert(fcache.openFiles[uint128(2)].accessCount == 2);
        
        assert(fcache.cache.length == 2);

        fcache.insert(uint128(0), 8); // 0 enters opened
        assert(fcache.openedFiles[uint128(0)] == 1);
        assert(fcache.singletons.count == 0);

        fcache.insert(uint128(0), 9);
        fcache.insert(uint128(0), 10); // enters cache displaces 2

        assert(fcache.cache.front.accessCount == 3);
        assert(fcache.openFiles[uint128(0)].accessCount == 3);
        assert(!(uint128(2) in fcache.openFiles));
        assert(fcache.singletons.count == 0);

        fcache.insert(uint128(4), 11);
        assert(fcache.singletons.count == 1);
        fcache.close;
    }
    {
        auto fcache = new IdFileCacheReader("/tmp/test_fcache");
        // f.openRead("/tmp/test_fcache", uint128(0));
        assert(fcache.getIds(uint128(0)).array == [0, 8, 9, 10]);
        assert(fcache.getIds(uint128(1)).array == [1, 2, 3, 4]);
        assert(fcache.getIds(uint128(2)).array == [5, 6, 7]);
        assert(fcache.getIds(uint128(4)).array == [11]);
    }
    
}