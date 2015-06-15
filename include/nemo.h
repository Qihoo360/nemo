#ifndef NEMO_INCLUDE_NEMO_H_
#define NEMO_INCLUDE_NEMO_H_

#include "nemo_mutex.h"
#include "rocksdb/db.h"
#include "nemo_iterator.h"

namespace nemo {

class Nemo
{
public:
    Nemo(const std::string &db_path, rocksdb::Options options);
    ~Nemo() {
        pthread_mutex_destroy(&(mutex_kv_));
        pthread_mutex_destroy(&(mutex_hash_));
        pthread_mutex_destroy(&(mutex_list_));
    };

    // =================KV=====================

    rocksdb::Status Set(const std::string &key, const std::string &val);
    rocksdb::Status Get(const std::string &key, std::string *val);
    rocksdb::Status Del(const std::string &key);
    rocksdb::Status MSet(const std::vector<KV> &kvs);
    rocksdb::Status MDel(const std::vector<std::string> &keys);
    rocksdb::Status MGet(const std::vector<std::string> &keys, std::vector<KVS> &kvss);
    rocksdb::Status Incrby(const std::string &key, int64_t by, std::string &new_val);
    rocksdb::Status GetSet(const std::string &key, const std::string &new_val, std::string *old_val);
    KIterator* scan(const std::string &start, const std::string &end, uint64_t limit);

    // ==============HASH=====================

    rocksdb::Status HSet(const std::string &key, const std::string &field, const std::string &val);
    rocksdb::Status HGet(const std::string &key, const std::string &field, std::string *val);
    rocksdb::Status HDel(const std::string &key, const std::string &field);
    bool HExists(const std::string &key, const std::string &field);
    rocksdb::Status HKeys(const std::string &key, std::vector<std::string> &keys);
    rocksdb::Status HGetall(const std::string &key, std::vector<FV> &fvs);
    int64_t HLen(const std::string &key);
    rocksdb::Status HMSet(const std::string &key, const std::vector<FV> &fvs);
    rocksdb::Status HMGet(const std::string &key, const std::vector<std::string> &keys, std::vector<FVS> &fvss);
    rocksdb::Status HSetnx(const std::string &key, const std::string &field, const std::string &val);
    int64_t HStrlen(const std::string &key, const std::string &field);
    HIterator* HScan(const std::string &key, const std::string &start, const std::string &end, uint64_t limit);
    rocksdb::Status HVals(const std::string &key, std::vector<std::string> &vals);
    rocksdb::Status HIncrby(const std::string &key, const std::string &field, int64_t by, std::string &new_val);
    
    // ==============List=====================
    uint64_t LLen(const std::string &key);
    rocksdb::Status LPush(const std::string &key, const std::string &val);
    rocksdb::Status LPop(const std::string &key, std::string *val);
    rocksdb::Status LPushx(const std::string &key, const std::string &val);
    rocksdb::Status LRange(const std::string &key, const int32_t begin, const int32_t end, std::vector<IV> &ivs);

private:

    std::string db_path_;
    std::unique_ptr<rocksdb::DB> db_;

    pthread_mutex_t mutex_kv_;
    pthread_mutex_t mutex_hash_;
    pthread_mutex_t mutex_list_;

    int DoHSet(const std::string &key, const std::string &field, const std::string &val, rocksdb::WriteBatch &writebatch);
    int DoHDel(const std::string &key, const std::string &field, rocksdb::WriteBatch &writebatch);
    int IncrHLen(const std::string &key, int64_t incr, rocksdb::WriteBatch &writebatch);

    Nemo(const Nemo &rval);
    void operator =(const Nemo &rval);

};

}

#endif
