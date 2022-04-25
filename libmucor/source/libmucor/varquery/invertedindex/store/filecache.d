module libmucor.varquery.invertedindex.store.filecache;
import libmucor.varquery.invertedindex.store.binary;
import libmucor.wideint;
import libmucor.khashl;
import std.container: BinaryHeap;
import core.stdc.stdlib: malloc, free;
import std.format: format;
import htslib.hts_log;
import std.algorithm: filter, map, joiner;
import std.array: array;

struct AccessedIdFile {
    uint128 id; 
    ulong accessCount;
    BinaryStore!ulong file;

    void openWrite(string prefix, uint128 id) {
        this.id = id;
        this.file = BinaryStore!ulong(format("%s_%x.ids", prefix, id), "wb");
    }

    void openRead(string prefix, uint128 id) {
        this.id = id;
        this.file = BinaryStore!ulong(format("%s_%x.ids", prefix, id), "rb");
    }

    void openAppend(string prefix, uint128 id) {
        this.id = id;
        this.file = BinaryStore!ulong(format("%s_%x.ids", prefix, id), "ab");
    }

    void write(ulong id) {
        this.accessCount++;
        this.file.write(id);
    }

    void write(ulong[] ids) {
        foreach (key; ids)
        {
            this.write(key);    
        }
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
    khashl!(uint128, ulong[]) smalls;

    ulong smallsMax;

    /// ids with files that have been opened
    /// 
    /// value is accessCount
    khashl!(uint128, ulong) openedFiles;

    /// ids with currently open files
    khashl!(uint128, AccessedIdFile * ) openFiles;

    BinaryHeap!(AccessedIdFile*[], "a.accessCount > b.accessCount") cache;
    ulong cacheSize;
    string prefix;

    this(string prefix, ulong cacheSize = 4096, ulong smallsMax = 128) {
        this.smallsMax = smallsMax;
        this.cacheSize = cacheSize;
        this.prefix = prefix;
        AccessedIdFile*[] cacheStore;
        this.cache = BinaryHeap!(AccessedIdFile*[], "a.accessCount > b.accessCount")(cacheStore);
    }

    void insert(uint128 key, ulong id) {
        auto p1 = key in openFiles;
        /// id file is currently open
        if(p1){
            debug hts_log_trace(__FUNCTION__, format("id %x found in cache", key));
            (*p1).write(id);
            return;
        }
        
        /// id file has been opened before
        auto p2 = key in openedFiles;
        if(p2) {
            debug hts_log_trace(__FUNCTION__, format("id %x has been opened prior", key));
            AccessedIdFile f;
            f.accessCount = *p2;
            f.openAppend(this.prefix, key);
            f.write(id);
            *p2 += 1;
            /// insert
            this.insertToCache(f, key);
            return;
        }
        /// id file has not been opened
        auto p3 = key in smalls;
        if(p3) {
            if(p3.length < this.smallsMax) {
                debug hts_log_trace(__FUNCTION__, format("id %x already in smalls", key));
                (*p3) ~= id;
            } else {
                debug hts_log_trace(__FUNCTION__, format("removing id %x from smalls", key));
                AccessedIdFile f;
                f.openWrite(this.prefix, key);
                f.write(*p3);
                f.write(id);
                auto inserted = insertToCache(f, key);
                this.smalls.remove(key);
                this.openedFiles[key] = this.smallsMax;
            }
        } else {
            debug hts_log_trace(__FUNCTION__, format("inserting id %x in smalls", key));
            ulong[] arr;
            arr.reserve(this.smallsMax);
            arr ~= id;
            smalls[key] = arr;
        }
    }

    bool insertToCache(AccessedIdFile f, uint128 key) {
        /// if cache is not full, insert
        if(this.cache.length < this.cacheSize) {
            debug hts_log_trace(__FUNCTION__, format("inserting id %x in cache", key));
            ///
            AccessedIdFile * fp = cast(AccessedIdFile *)malloc(AccessedIdFile.sizeof);
            *fp = f;
            this.cache.insert(fp);
            this.openFiles[key] = fp;
            return true;
        /// if cache is full and this file is accessed more, replace lowest
        } else if(f.accessCount > this.cache.front.accessCount) {
            AccessedIdFile * fp = cast(AccessedIdFile *)malloc(AccessedIdFile.sizeof);
            *fp = f;
            /// replace lowest open file
            auto old_file = cache.front;
            this.cache.replaceFront(fp);
            debug hts_log_trace(__FUNCTION__, format("replaced id %x  with id %x in cache", old_file.id,  key));
            this.openFiles[key] = fp;
            this.openedFiles[old_file.id] = old_file.accessCount;
            old_file.close;
            free(old_file);
            return true;
        /// if cache is full and this file is not accessed more, just close
        } else {
            f.close();
            return false;
        }
    }

    void close() {

        auto f = BinaryStore!uint128(this.prefix ~ ".hashes", "wb");
        foreach(kv;openFiles.byKeyValue) {
            (cast(AccessedIdFile*)kv.value).close();
            free(cast(AccessedIdFile*)kv.value);
        }
        auto sf = BinaryStore!SmallsIds(this.prefix ~ ".smalls", "wb");
        foreach (kv; this.smalls.byKeyValue)
        {
            sf.write(SmallsIds(kv[0], cast(ulong[])kv[1]));
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
    BinaryStore!SmallsIds * smalls;

    khashlSet!(uint128) hashes;

    string prefix;

    this(string prefix) {
        this.prefix = prefix;
        this.smalls = new BinaryStore!SmallsIds(this.prefix ~ ".smalls", "rb");
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

        auto r = this.smalls.getAll.filter!(x => x.key == key);
        if(r.empty) {
            hts_log_warning(__FUNCTION__, format("Key %x not preset", key));
            return [];
        } else {
            return r.map!(x => x.ids).joiner.array;
        }
    }

    void close() {
        this.smalls.close;
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
        fcache.insert(uint128(0), 0); // 0 enters smalls

        assert(fcache.smalls.count == 1);

        fcache.insert(uint128(1), 1); // 1 enters smalls
        assert(fcache.smalls.count == 2);
        fcache.insert(uint128(1), 2); // 1 enters opened
        assert(fcache.smalls.count == 1);
        assert(fcache.openedFiles[uint128(1)] == 1);
        fcache.insert(uint128(1), 3); // 1 enters cache
        fcache.insert(uint128(1), 4);
        assert(fcache.cache.front.accessCount == 3);

        assert(fcache.openFiles[uint128(1)].accessCount == 3);
        assert(fcache.cache.length == 1);

        fcache.insert(uint128(2), 5); // 2 enters smalls
        assert(fcache.smalls.count == 2);
        fcache.insert(uint128(2), 6); // 2 enters opened
        assert(fcache.smalls.count == 1);
        assert(fcache.openedFiles[uint128(2)] == 1);
        fcache.insert(uint128(2), 7); // 2 enters cache (cache full)

        assert(fcache.cache.front.accessCount == 2);
        assert(fcache.openFiles[uint128(2)].accessCount == 2);
        
        assert(fcache.cache.length == 2);

        fcache.insert(uint128(0), 8); // 0 enters opened
        assert(fcache.openedFiles[uint128(0)] == 1);
        assert(fcache.smalls.count == 0);

        fcache.insert(uint128(0), 9);
        fcache.insert(uint128(0), 10); // enters cache displaces 2

        assert(fcache.cache.front.accessCount == 3);
        assert(fcache.openFiles[uint128(0)].accessCount == 3);
        assert(!(uint128(2) in fcache.openFiles));
        assert(fcache.smalls.count == 0);

        fcache.insert(uint128(4), 11);
        assert(fcache.smalls.count == 1);
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