module libmucor.invertedindex.store.filecache;
import libmucor.invertedindex.store.binary;
import libmucor.invertedindex.metadata;
import libmucor.wideint;
import libmucor.khashl;
import std.container : BinaryHeap;
import std.format : format;
import libmucor.error;
import std.algorithm : filter, map, joiner;
import std.array : array;
import std.path: buildPath;

struct AccessedIdFile
{
    uint128 fid;
    ulong[] buffer;
    ulong accessCount;
    ulong bufferMax;
    bool openedPrior;

    BinaryStore!ulong file;
    bool closed;
    string prefix;

    this(string prefix, uint128 fid, ulong bufferMax)
    {
        this.fid = fid;
        this.buffer = [];
        this.accessCount = 0;
        this.bufferMax = bufferMax;
        this.openedPrior = false;
        this.closed = true;
        this.prefix = prefix;
    }

    void openRead()
    {
        assert(this.closed);
        
        this.file = BinaryStore!ulong(buildPath(prefix, format("%x.ids", fid)), "rb");
        this.closed = false;
    }

    private void openWrite()
    {
        assert(this.closed);
        if(this.openedPrior){
            this.openAppend;
        } else {
            this.file = BinaryStore!ulong(buildPath(prefix, format("%x.ids", fid)), "wb");
            this.closed = false;
            this.openedPrior = true;
        }
        
    }

    private void openAppend()
    {
        assert(this.closed);
        
        this.file = BinaryStore!ulong(buildPath(prefix, format("%x.ids", fid)), "ab");
        this.closed = false;
    }

    void write(ulong id)
    {
        this.accessCount++;
        if(this.buffer.length > this.bufferMax){
            this.flushBuffer;
        }
        this.buffer ~= id;
    }

    void flushBuffer()
    {
        if(closed) this.openWrite;
        this.file.write(this.buffer);
        this.buffer = [];
    }

    auto getIds()
    {
        return this.file.getAll;
    }

    void close()
    {
        assert(!closed);
        this.file.close;
        this.closed = true;
    }
}

struct IdFileCacheWriter
{

    /// ids with files that have been opened
    /// 
    /// value is accessCount
    khashl!(uint128, AccessedIdFile*) allFiles;

    khashl!(uint128, AccessedIdFile*) cached;

    BinaryHeap!(AccessedIdFile*[], "a.accessCount > b.accessCount") cache;
    ulong cacheSize;
    string prefix;
    ulong fileBufferSize;
    ulong smallsMax;

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
        import std.algorithm : count;
        auto p1 = key in cached;
        auto cachedfile = p1 ? *p1 : null;
        

        /// id file is currently open
        if (cachedfile)
        {
            cachedfile.write(id);
            return;
        } else {
            auto p2 = key in allFiles;
            auto file = p2 ? *p2 : null;

            /// id file has been opened before
            if(file) {
                
                /// append file
                file.write(id);

                if(!file.closed) {
                    auto inserted = this.insertToCache(file, key);
                    if(!inserted)
                        file.close;
                }
            } else {
                AccessedIdFile *fp = new AccessedIdFile(prefix, key, smallsMax);
                fp.write(id);
                this.allFiles[key] = fp;
            }
        }
    }

    bool insertToCache(AccessedIdFile * f, uint128 key)
    {
        /// if cache is not full, insert
        if (this.cache.length < this.cacheSize)
        {
            this.cache.insert(f);
            this.cached[key] = f;
            return true;
        }
        else if (f.accessCount > this.cache.front.accessCount)
        {
            /// replace lowest open file
            auto old_file = cache.front;
            this.cache.replaceFront(f);
            this.cached[key] = f;
            this.cached.remove(old_file.fid);
            old_file.close;
            return true;
        }
        else
            return false;
    }

    void close()
    {

        auto f = new BinaryStore!uint128(buildPath(this.prefix, "hashes"), "wb");
        foreach (kv; cached.byKeyValue)
        {
            (cast(AccessedIdFile*) kv.value).close();
        }
        auto smeta = new BinaryStore!SmallsIdMetaData(buildPath(this.prefix, "smalls.meta"), "wb");
        auto sids = new BinaryStore!ulong(buildPath(this.prefix, "smalls.ids"), "wb");
        foreach (kv; this.allFiles.byKeyValue)
        {
            auto file = cast(AccessedIdFile *)kv[1];
            if(file.openedPrior){
                file.flushBuffer;
                file.close();
                f.write(kv[0]);
            } else {
                SmallsIdMetaData meta;
                meta.key = kv[0];
                meta.dataOffset = sids.tell;
                foreach(k;file.buffer)
                    sids.write(k);
                meta.dataLength = file.buffer.length;
                smeta.write(meta);
            }
        }
        smeta.close;
        sids.close;
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

    khashlSet!(ulong) * getIds(uint128 key)
    {
        auto ret = new khashlSet!(ulong);
        /// id file is currently open
        if (key in this.hashes)
        {
            auto f = AccessedIdFile(this.prefix, key, 0);
            f.openRead();
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
        // log_info(__FUNCTION__, "%x metadata collected: %x", key, Thread.getThis.id);
        auto smallsIds = BinaryStore!ulong(buildPath(this.prefix, "smalls.ids"), "rb");
        foreach (x; smallsIds.getArrayFromOffsets(smallsOffsets))
        {
            foreach (k; x)
            {
                ret.insert(k);
            }
        }
        smallsIds.close();
        // log_info(__FUNCTION__, "%d ids collected for %x ", ret.count, key);
        return ret;
    }
}



unittest
{
    import htslib.hts_log;
    import std.stdio;
    import std.array : array;
    import std.file: mkdirRecurse;

    // hts_set_log_level(htsLogLevel.HTS_LOG_DEBUG);
    {
        mkdirRecurse("/tmp/test_fcache");
        auto fcache = new IdFileCacheWriter("/tmp/test_fcache", 2, 2);
        fcache.insert(uint128(0), 0); // 0 enters smalls

        // assert(fcache.smalls.count == 1);

        fcache.insert(uint128(1), 1); // 1 enters smalls
        fcache.insert(uint128(1), 2);
        // assert(fcache.smalls.count == 2);
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
        assert(fcache.getIds(uint128(4)).byKey.array == [11, 12, 14, 13, 15]);
    }

}
