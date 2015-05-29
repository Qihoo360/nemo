#ifndef NEMO_INCLUDE_NEMO_H_
#define NEMO_INCLUDE_NEMO_H_

//#include <stdio.h>
//#include <string>

//#include "rocksdb/status.h"
//#include "rocksdb/options.h"
#include <pthread.h>
#include "rocksdb/db.h"
#include "nemo_iterator.h"

namespace nemo {
//using namespace rocksdb;
//typedef rocksdb::Options Options;
//typedef rocksdb::ReadOptions WriteOptions;
//typedef rocksdb::Status Status;
//typedef rocksdb::Slice Slice;
//typedef rocksdb::DB DB;
#define NEMO_KEY_LEN_MAX 1024

typedef struct Kv {
    std::string key;
    std::string val;
}Kv;
typedef struct Kvs {
    std::string key;
    std::string val;
    rocksdb::Status status;
}Kvs;
typedef struct MutexWrite {
    pthread_mutex_t writer_mutex;
    rocksdb::WriteBatch writebatch;
}MutexWriter;

class Nemo
{
public:
    Nemo(const std::string &db_path, rocksdb::Options options);
    ~Nemo() {
//        pthread_mutex_destroy(&(writer_kv_.writer_mutex));
        pthread_mutex_destroy(&(writer_hash_.writer_mutex));
    };
//    void LockKv();
//    void UnlockKv();
    void LockHash();
    void UnlockHash();

// =================KV=====================

    rocksdb::Status Set(const std::string &key, const std::string &val);
    rocksdb::Status Get(const std::string &key, std::string *val);
    rocksdb::Status Delete(const std::string &key);
    rocksdb::Status MultiSet(const std::vector<Kv> &kvs);
    rocksdb::Status MultiDel(const std::vector<std::string> &keys);
    rocksdb::Status MultiGet(const std::vector<std::string> &keys, std::vector<Kvs> &kvss);
    rocksdb::Status Incr(const std::string &key, int64_t by, std::string &new_val);
    KIterator* scan(const std::string &start, const std::string &end, uint64_t limit);

// ==============HASH=====================

    rocksdb::Status HSet(const std::string &key, const std::string &field, const std::string &val);
    rocksdb::Status HGet(const std::string &key, const std::string &field, std::string *val);
    rocksdb::Status HDel(const std::string &key, const std::string &field);
    int64_t HSize(const std::string &key);

private:

    std::string db_path_;
    std::unique_ptr<rocksdb::DB> db_;
//    MutexWriter writer_kv_;
    MutexWriter writer_hash_;

    int hset_one(const std::string &key, const std::string &field, const std::string &val);
    int hdel_one(const std::string &key, const std::string &field);
    int incr_hsize(const std::string &key, int64_t incr);

    Nemo(const Nemo &rval);
    void operator =(const Nemo &rval);

};

}

#endif
