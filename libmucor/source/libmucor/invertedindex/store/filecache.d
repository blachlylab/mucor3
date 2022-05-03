module libmucor.invertedindex.store.filecache;
import libmucor.invertedindex.store.binary;
import libmucor.invertedindex.metadata;
import libmucor.wideint;
import libmucor.khashl;
import std.container : BinaryHeap;
import core.stdc.stdlib : malloc, free;
import std.format : format;
import libmucor.error;
import std.algorithm : filter, map, joiner;
import std.array : array;
import std.path: buildPath;

struct AccessedIdFile
{
    uint128 id;
    ulong accessCount;
    BinaryStore!ulong file;

    void openWrite(string prefix, uint128 id)
    {
        this.id = id;
        this.file = BinaryStore!ulong(buildPath(prefix, format("%x.ids", id)), "wb");
    }

    void openRead(string prefix, uint128 id)
    {
        this.id = id;
        this.file = BinaryStore!ulong(buildPath(prefix, format("%x.ids", id)), "rb");
    }

    void openAppend(string prefix, uint128 id)
    {
        this.id = id;
        this.file = BinaryStore!ulong(buildPath(prefix, format("%x.ids", id)), "ab");
    }

    void write(ulong id)
    {
        this.accessCount++;
        this.file.write(id);
    }

    void write(ulong[] ids)
    {
        foreach (key; ids)
        {
            this.write(key);
        }
    }

    auto getIds()
    {
        return this.file.getAll;
    }

    void close()
    {
        this.file.close;
    }
}

struct IdFileCacheWriter
{

    /// cache keys that have a single id
    khashl!(uint128, ulong[]) smalls;

    ulong smallsMax;

    /// ids with files that have been opened
    /// 
    /// value is accessCount
    khashl!(uint128, ulong) openedFiles;

    /// ids with currently open files
    khashl!(uint128, AccessedIdFile*) openFiles;

    BinaryHeap!(AccessedIdFile*[], "a.accessCount > b.accessCount") cache;
    ulong cacheSize;
    string prefix;
    ulong fileBufferSize;

    this(string prefix, ulong cacheSize = 8192, ulong smallsMax = 128)
    {
        this.fileBufferSize = fileBufferSize;
        this.smallsMax = smallsMax;
        this.cacheSize = cacheSize;
        this.prefix = prefix;
        AccessedIdFile*[] cacheStore;
        this.cache = BinaryHeap!(AccessedIdFile*[], "a.accessCount > b.accessCount")(cacheStore);
    }

    void insert(uint128 key, ulong id)
    {
        auto p1 = key in openFiles;
        /// id file is currently open
        if (p1)
        {
            log_trace(__FUNCTION__, "id %x found in cache", key);
            (*p1).write(id);
            return;
        }

        /// id file has been opened before
        auto p2 = key in openedFiles;
        if (p2)
        {
            log_trace(__FUNCTION__, "id %x has been opened prior", key);
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
        if (p3)
        {
            if (p3.length < this.smallsMax)
            {
                log_trace(__FUNCTION__, "id %x already in smalls", key);
                (*p3) ~= id;
            }
            else
            {
                log_trace(__FUNCTION__, "removing id %x from smalls", key);
                AccessedIdFile f;
                f.openWrite(this.prefix, key);
                f.write(*p3);
                f.write(id);
                auto inserted = insertToCache(f, key);
                this.smalls.remove(key);
                this.openedFiles[key] = this.smallsMax;
            }
        }
        else
        {
            log_trace(__FUNCTION__, format("inserting id %x in smalls", key));
            ulong[] arr;
            arr.reserve(this.smallsMax);
            arr ~= id;
            smalls[key] = arr;
        }
    }

    bool insertToCache(AccessedIdFile f, uint128 key)
    {
        /// if cache is not full, insert
        if (this.cache.length < this.cacheSize)
        {
            log_trace(__FUNCTION__, "inserting id %x in cache", key);
            ///
            AccessedIdFile* fp = cast(AccessedIdFile*) malloc(AccessedIdFile.sizeof);
            *fp = f;
            this.cache.insert(fp);
            this.openFiles[key] = fp;
            return true;
            /// if cache is full and this file is accessed more, replace lowest
        }
        else if (f.accessCount > this.cache.front.accessCount)
        {
            AccessedIdFile* fp = cast(AccessedIdFile*) malloc(AccessedIdFile.sizeof);
            *fp = f;
            /// replace lowest open file
            auto old_file = cache.front;
            this.cache.replaceFront(fp);
            log_trace(__FUNCTION__, "replaced id %x  with id %x in cache", old_file.id, key);
            this.openFiles[key] = fp;
            this.openFiles.remove(old_file.id);
            this.openedFiles[old_file.id] = old_file.accessCount;
            old_file.close;
            free(old_file);
            return true;
            /// if cache is full and this file is not accessed more, just close
        }
        else
        {
            f.close();
            return false;
        }
    }

    void close()
    {

        auto f = new BinaryStore!uint128(buildPath(this.prefix, "hashes"), "wb");
        foreach (kv; openFiles.byKeyValue)
        {
            (cast(AccessedIdFile*) kv.value).close();
            free(cast(AccessedIdFile*) kv.value);
        }
        auto smeta = new BinaryStore!SmallsIdMetaData(buildPath(this.prefix, "smalls.meta"), "wb");
        auto sids = new BinaryStore!ulong(buildPath(this.prefix, "smalls.ids"), "wb");
        foreach (kv; this.smalls.byKeyValue)
        {
            SmallsIdMetaData meta;
            meta.key = kv[0];
            meta.dataOffset = sids.tell;
            foreach(k;kv[1])
                sids.write(k);
            meta.dataLength = kv[1].length;
            smeta.write(meta);
        }
        smeta.close;
        sids.close;

        foreach (k; this.openedFiles.byKey)
        {
            f.write(k);
        }
        f.close;
    }
}

struct IdFileCacheReader
{

    /// cache keys that have a single id
    SmallsIdMetaData[] smallsMeta;

    khashlSet!(uint128) hashes;

    string prefix;
    ulong fileBufferSize;

    this(string prefix)
    {
        this.fileBufferSize = fileBufferSize;
        this.prefix = prefix;
        auto md = new BinaryStore!SmallsIdMetaData(buildPath(this.prefix, "smalls.meta"), "rb");
        foreach (key; md.getAll)
        {
            smallsMeta ~= key;   
        }
        md.close();
        auto f = BinaryStore!uint128(buildPath(this.prefix, "hashes"), "rb");
        foreach (key; f.getAll)
        {
            this.hashes.insert(key);
        }
        f.close;
    }

    khashlSet!(ulong) getIds(uint128 key)
    {
        khashlSet!(ulong) ret;
        /// id file is currently open
        if (key in hashes)
        {
            AccessedIdFile f;
            f.openRead(this.prefix, key);
            foreach (x; f.getIds)
            {
                ret.insert(x);
            }
            f.close;
            return ret;
        }
        auto smallsOffsets = this.smallsMeta
            .filter!(x => x.key == key)
            .map!(x => OffsetTuple(x.dataOffset, x.dataLength)).array;
        // log_info(__FUNCTION__, "%x metadata collected", key);
        auto smallsIds = BinaryStore!ulong(buildPath(this.prefix, "smalls.ids"), "rb");
        foreach (x; smallsIds.getArrayFromOffsets(smallsOffsets))
        {
            foreach (k; x)
            {
                ret.insert(k);
            }
        }
        smallsIds.close();
        // log_info(__FUNCTION__, "%x ids collected", key);
        return ret;
    }
}

unittest
{
    import htslib.hts_log;
    import std.stdio;
    import std.array : array;
    import std.file: mkdirRecurse;

    hts_set_log_level(htsLogLevel.HTS_LOG_DEBUG);
    {
        mkdirRecurse("/tmp/test_fcache");
        auto fcache = new IdFileCacheWriter("/tmp/test_fcache", 2, 2);
        fcache.insert(uint128(0), 0); // 0 enters smalls

        assert(fcache.smalls.count == 1);

        fcache.insert(uint128(1), 1); // 1 enters smalls
        fcache.insert(uint128(1), 2);
        assert(fcache.smalls.count == 2);
        fcache.insert(uint128(1), 3); // 1 enters opened
        // assert(fcache.smalls.count == 1);
        // assert(fcache.openedFiles[uint128(1)] == 3);
        // fcache.insert(uint128(1), 3); // 1 enters cache
        fcache.insert(uint128(1), 4);
        // assert(fcache.cache.front.accessCount == 3);

        // assert(fcache.openFiles[uint128(1)].accessCount == 3);
        // assert(fcache.cache.length == 1);

        fcache.insert(uint128(2), 5); // 2 enters smalls
        // assert(fcache.smalls.count == 2);
        fcache.insert(uint128(2), 6); // 2 enters opened
        // assert(fcache.smalls.count == 1);
        // assert(fcache.openedFiles[uint128(2)] == 1);
        fcache.insert(uint128(2), 7); // 2 enters cache (cache full)

        // assert(fcache.cache.front.accessCount == 2);
        // assert(fcache.openFiles[uint128(2)].accessCount == 2);

        // assert(fcache.cache.length == 2);

        fcache.insert(uint128(0), 8); // 0 enters opened
        // assert(fcache.openedFiles[uint128(0)] == 1);
        // assert(fcache.smalls.count == 0);

        fcache.insert(uint128(0), 9);
        fcache.insert(uint128(0), 10); // enters cache displaces 2

        // assert(fcache.cache.front.accessCount == 3);
        // assert(fcache.openFiles[uint128(0)].accessCount == 3);
        // assert(!(uint128(2) in fcache.openFiles));
        // assert(fcache.smalls.count == 0);

        fcache.insert(uint128(4), 11);
        fcache.insert(uint128(4), 12);
        fcache.insert(uint128(4), 13);
        fcache.insert(uint128(4), 14);
        fcache.insert(uint128(4), 15);
        // assert(fcache.smalls.count == 1);
        fcache.close;
    }
    {
        auto fcache = new IdFileCacheReader("/tmp/test_fcache");
        // f.openRead("/tmp/test_fcache", uint128(0));
        assert(fcache.getIds(uint128(0)).byKey.array == [0, 8, 9, 10]);
        assert(fcache.getIds(uint128(1)).byKey.array == [2, 4, 1, 3]);
        assert(fcache.getIds(uint128(2)).byKey.array == [7, 6, 5]);
        writeln(fcache.getIds(uint128(4)).byKey.array);
        assert(fcache.getIds(uint128(4)).byKey.array == [11, 12, 14, 13, 15]);
    }

}
