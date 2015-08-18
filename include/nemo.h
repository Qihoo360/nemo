#ifndef NEMO_INCLUDE_NEMO_H_
#define NEMO_INCLUDE_NEMO_H_

#include "nemo_mutex.h"
#include "rocksdb/db.h"
#include "nemo_iterator.h"
#include "nemo_option.h"

namespace nemo {
typedef rocksdb::Status Status;


class Nemo
{
public:
    Nemo(const std::string &db_path);
    ~Nemo() {
        pthread_mutex_destroy(&(mutex_kv_));
        pthread_mutex_destroy(&(mutex_hash_));
        pthread_mutex_destroy(&(mutex_list_));
        pthread_mutex_destroy(&(mutex_zset_));
    };

    // =================KV=====================

    Status Set(const std::string &key, const std::string &val);
    Status Get(const std::string &key, std::string *val);
    Status Del(const std::string &key);
    Status MSet(const std::vector<KV> &kvs);
    Status MDel(const std::vector<std::string> &keys);
    Status MGet(const std::vector<std::string> &keys, std::vector<KVS> &kvss);
    Status Incrby(const std::string &key, int64_t by, std::string &new_val);
    Status Decrby(const std::string &key, int64_t by, std::string &new_val);
    Status GetSet(const std::string &key, const std::string &new_val, std::string *old_val);
    KIterator* Scan(const std::string &start, const std::string &end, uint64_t limit);

    // ==============HASH=====================

    Status HSet(const std::string &key, const std::string &field, const std::string &val);
    Status HGet(const std::string &key, const std::string &field, std::string *val);
    Status HDel(const std::string &key, const std::string &field);
    bool HExists(const std::string &key, const std::string &field);
    Status HKeys(const std::string &key, std::vector<std::string> &keys);
    Status HGetall(const std::string &key, std::vector<FV> &fvs);
    int64_t HLen(const std::string &key);
    Status HMSet(const std::string &key, const std::vector<FV> &fvs);
    Status HMGet(const std::string &key, const std::vector<std::string> &keys, std::vector<FVS> &fvss);
    Status HSetnx(const std::string &key, const std::string &field, const std::string &val);
    int64_t HStrlen(const std::string &key, const std::string &field);
    HIterator* HScan(const std::string &key, const std::string &start, const std::string &end, uint64_t limit);
    Status HVals(const std::string &key, std::vector<std::string> &vals);
    Status HIncrby(const std::string &key, const std::string &field, int64_t by, std::string &new_val);
    Status HIncrbyfloat(const std::string &key, const std::string &field, double by, std::string &new_val);
    
    // ==============List=====================
    Status LIndex(const std::string &key, const int64_t index, std::string *val);
    Status LLen(const std::string &key, uint64_t *llen);
    Status LPush(const std::string &key, const std::string &val, uint64_t *llen);
    Status LPop(const std::string &key, std::string *val);
    Status LPushx(const std::string &key, const std::string &val, uint64_t *llen);
    Status LRange(const std::string &key, const int32_t begin, const int32_t end, std::vector<IV> &ivs);
    Status LSet(const std::string &key, const int32_t index, const std::string &val);
    Status LTrim(const std::string &key, const int32_t begin, const int32_t end);
    Status RPush(const std::string &key, const std::string &val, uint64_t *llen);
    Status RPop(const std::string &key, std::string *val);
    Status RPushx(const std::string &key, const std::string &val, uint64_t *llen);
    Status RPopLPush(const std::string &src, const std::string &dest, std::string &val);

    // ==============ZSet=====================
    Status ZAdd(const std::string &key, const int64_t score, const std::string &member);
    int64_t ZCard(const std::string &key);
    int64_t ZCount(const std::string &key, const int64_t begin, const int64_t end);
    ZIterator* ZScan(const std::string &key, int64_t begin, int64_t end, uint64_t limit);
    Status ZIncrby(const std::string &key, const std::string &member, const int64_t by);

    //TODO modify range
    Status ZRange(const std::string &key, const int64_t start, const int64_t stop, std::vector<SM> &sms);
    Status ZRangebyscore(const std::string &key, const int64_t start, const int64_t stop, std::vector<SM> &sms);

private:

    std::string db_path_;
    std::unique_ptr<rocksdb::DB> kv_db_;
    std::unique_ptr<rocksdb::DB> hash_db_;
    std::unique_ptr<rocksdb::DB> list_db_;
    std::unique_ptr<rocksdb::DB> zset_db_;

    pthread_mutex_t mutex_kv_;
    pthread_mutex_t mutex_hash_;
    pthread_mutex_t mutex_list_;
    pthread_mutex_t mutex_zset_;

    int DoHSet(const std::string &key, const std::string &field, const std::string &val, rocksdb::WriteBatch &writebatch);
    int DoHDel(const std::string &key, const std::string &field, rocksdb::WriteBatch &writebatch);
    int IncrHLen(const std::string &key, int64_t incr, rocksdb::WriteBatch &writebatch);

    int DoZSet(const std::string &key, const int64_t score, const std::string &member, rocksdb::WriteBatch &writebatch);
    int IncrZLen(const std::string &key, int64_t incr, rocksdb::WriteBatch &writebatch);

    Nemo(const Nemo &rval);
    void operator =(const Nemo &rval);

};

}

#endif
