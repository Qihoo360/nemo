#ifndef NEMO_INCLUDE_NEMO_H_
#define NEMO_INCLUDE_NEMO_H_

#include "nemo_mutex.h"
#include "rocksdb/db.h"
#include "nemo_iterator.h"
#include "nemo_const.h"
#include "nemo_options.h"

namespace nemo {
typedef rocksdb::Status Status;

class Nemo
{
public:
    Nemo(const std::string &db_path, const Options &options);
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
    Status MDel(const std::vector<std::string> &keys, int64_t* count);
    Status MGet(const std::vector<std::string> &keys, std::vector<KVS> &kvss);
    Status Incrby(const std::string &key, int64_t by, std::string &new_val);
    Status Decrby(const std::string &key, int64_t by, std::string &new_val);
    Status Incrbyfloat(const std::string &key, double by, std::string &new_val);
    Status GetSet(const std::string &key, const std::string &new_val, std::string *old_val);
    KIterator* Scan(const std::string &start, const std::string &end, uint64_t limit, bool use_snapshot = false);

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
    HIterator* HScan(const std::string &key, const std::string &start, const std::string &end, uint64_t limit, bool use_snapshot = false);
    Status HVals(const std::string &key, std::vector<std::string> &vals);
    Status HIncrby(const std::string &key, const std::string &field, int64_t by, std::string &new_val);
    Status HIncrbyfloat(const std::string &key, const std::string &field, double by, std::string &new_val);
    
    // ==============List=====================
    Status LIndex(const std::string &key, const int64_t index, std::string *val);
    Status LLen(const std::string &key, int64_t *llen);
    Status LPush(const std::string &key, const std::string &val, int64_t *llen);
    Status LPop(const std::string &key, std::string *val);
    Status LPushx(const std::string &key, const std::string &val, int64_t *llen);
    Status LRange(const std::string &key, const int64_t begin, const int64_t end, std::vector<IV> &ivs);
    Status LSet(const std::string &key, const int64_t index, const std::string &val);
    Status LTrim(const std::string &key, const int64_t begin, const int64_t end);
    Status RPush(const std::string &key, const std::string &val, int64_t *llen);
    Status RPop(const std::string &key, std::string *val);
    Status RPushx(const std::string &key, const std::string &val, int64_t *llen);
    Status RPopLPush(const std::string &src, const std::string &dest, std::string &val);
    Status LInsert(const std::string &key, Position pos, const std::string &pivot, const std::string &val, int64_t *llen);
    Status LRem(const std::string &key, const int64_t count, const std::string &val, int64_t *rem_count);

    // ==============ZSet=====================
    //Status ZAdd(const std::string &key, const int64_t score, const std::string &member);
    Status ZAdd(const std::string &key, const double score, const std::string &member, int64_t *res);
    int64_t ZCard(const std::string &key);
    int64_t ZCount(const std::string &key, const double begin, const double end, bool is_lo = false, bool is_ro = false);
    ZIterator* ZScan(const std::string &key, const double begin, const double end, uint64_t limit, bool use_snapshot = false);
    Status ZIncrby(const std::string &key, const std::string &member, const double by, std::string &new_val);

    Status ZRange(const std::string &key, const int64_t start, const int64_t stop, std::vector<SM> &sms);
    Status ZUnionStore(const std::string &destination, const int numkeys, const std::vector<std::string> &keys, const std::vector<double> &weights, Aggregate agg, int64_t *res);
    Status ZInterStore(const std::string &destination, const int numkeys, const std::vector<std::string> &keys, const std::vector<double> &weights, Aggregate agg, int64_t *res);
    Status ZRangebyscore(const std::string &key, const double start, const double stop, std::vector<SM> &sms, int64_t offset = 0, bool is_lo = false, bool is_ro = false);
    Status ZRem(const std::string &key, const std::string &member, int64_t *res);
    Status ZRank(const std::string &key, const std::string &member, int64_t *rank);
    Status ZRevrank(const std::string &key, const std::string &member, int64_t *rank);
    Status ZScore(const std::string &key, const std::string &member, double *score);
    Status ZRangebylex(const std::string &key, const std::string &min, const std::string &max, std::vector<std::string> &members, int64_t offset = 0);
    Status ZLexcount(const std::string &key, const std::string &min, const std::string &max, int64_t* count);
    Status ZRemrangebylex(const std::string &key, const std::string &min, const std::string &max, bool is_lo, bool is_ro, int64_t* count);
    Status ZRemrangebyrank(const std::string &key, const int64_t start, const int64_t stop, int64_t* count);
    Status ZRemrangebyscore(const std::string &key, const double start, const double stop, int64_t* count, bool is_lo = false, bool is_ro = false);

private:

    std::string db_path_;
    rocksdb::Options open_options_;
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
    Status HSetNoLock(const std::string &key, const std::string &field, const std::string &val);
    int IncrHLen(const std::string &key, int64_t incr, rocksdb::WriteBatch &writebatch);

    Status ZAddNoLock(const std::string &key, const double score, const std::string &member, int64_t *res);
    ZLexIterator* ZScanbylex(const std::string &key, const std::string &min, const std::string &max, uint64_t limit, bool use_snapshot = false);
    int DoZSet(const std::string &key, const double score, const std::string &member, rocksdb::WriteBatch &writebatch);
    int32_t L2R(const std::string &key, const int64_t index, const int64_t left, int64_t *priv, int64_t *cur, int64_t *next);
    int32_t R2L(const std::string &key, const int64_t index, const int64_t right, int64_t *priv, int64_t *cur, int64_t *next);

    int IncrZLen(const std::string &key, int64_t incr, rocksdb::WriteBatch &writebatch);

    Nemo(const Nemo &rval);
    void operator =(const Nemo &rval);

};

}

#endif
