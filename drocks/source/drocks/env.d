module drocks.env;

import rocksdb;

struct Env {
    rocksdb_env_t* env;

    void initialize() {
        this.env = rocksdb_create_default_env();
    }

    this(rocksdb_env_t* env) {
        this.env = env;
    }

    static Env createMemoryEnv() {
        return Env(rocksdb_create_mem_env());
    }

    ~this() {
        if(env) rocksdb_env_destroy(this.env);
    }

    void joinAll() {
        rocksdb_env_join_all_threads(this.env);
    }

    @property backgroundThreads(int n) {
        rocksdb_env_set_background_threads(this.env, n);
    }

    @property highPriorityBackgroundThreads(int n) {
        rocksdb_env_set_high_priority_background_threads(this.env, n);
    }
}
