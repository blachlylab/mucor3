module drocks.env;

import rocksdb;
import drocks.memory;

alias EnvPtr = SafePtr!(rocksdb_env_t, rocksdb_env_destroy);
struct Env {
    EnvPtr env;

    void initialize() {
        this.env = EnvPtr(rocksdb_create_default_env());
    }

    this(this) { 
        this.env = env;
    }

    this(rocksdb_env_t* env) {
        this.env = env;
    }

    static Env createMemoryEnv() {
        return Env(rocksdb_create_mem_env());
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
