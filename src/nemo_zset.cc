#include "nemo.h"
#include "nemo_zset.h"
#include "nemo_iterator.h"
#include "utilities/strings.h"
#include "xdebug.h"
using namespace nemo;

Status Nemo::ZAdd(const std::string &key, const int64_t score, const std::string &member) {
    Status s;
    std::string db_key = EncodeZSetKey(key, member);
    std::string size_key = EncodeZSizeKey(key);
    std::string score_key = EncodeZScoreKey(key, member, score); 
    rocksdb::WriteBatch batch;
    MutexLock l(&mutex_zset_);
    int ret = DoZSet(key, score, member, batch);
    if (ret == 2) {
        if (IncrZLen(key, 1, batch) == 0) {
            s = zset_db_->Write(rocksdb::WriteOptions(), &batch);
            return s;
        } else {
            return Status::Corruption("incr zsize error");
        }
    } else if (ret == 1) {
        s = zset_db_->Write(rocksdb::WriteOptions(), &batch);
        return s;
    } else if (ret == 0) {
        return Status::OK();
    } else {
        return Status::Corruption("zadd error");
    }
}

int64_t Nemo::ZCard(const std::string &key) {
    Status s;
    std::string size;
    std::string size_key = EncodeZSizeKey(key);
    s = zset_db_->Get(rocksdb::ReadOptions(), size_key, &size);
    if (s.ok()) {
        return StrToInt64(size);
    } else if (s.IsNotFound()) {
        return 0;
    } else {
        return -1;
    }
}

ZIterator* Nemo::ZScan(const std::string &key, int64_t begin, int64_t end, uint64_t limit) {
    std::string key_start, key_end;
    key_start = EncodeZScoreKey(key, "", begin);
    key_end = EncodeZScoreKey(key, "", end);
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = zset_db_->NewIterator(iterate_options);
    it->Seek(key_start);
    if (it->Valid() && it->key() == key_start) {
        it->Next();
    }
    return new ZIterator(new Iterator(it, key_end, limit), key); 
}

int64_t Nemo::ZCount(const std::string &key, int64_t begin, int64_t end) {
    ZIterator* it = ZScan(key, begin, end + 1, -1);
    int64_t s;
    int64_t n = 0;
    while (it->Next()) {
        s = it->Score();
        if (s >= begin && s <= end + 1 ) {
            n++;
        } else {
            break;
        }
    }
    return n;
}

Status Nemo::ZIncrby(const std::string &key, const std::string &member, const int64_t by) {
    Status s;
    std::string old_score;
    std::string score_key;
    std::string db_key = EncodeZSetKey(key, member);
    rocksdb::WriteBatch writebatch;
    MutexLock l(&mutex_zset_);
    s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);
    if (s.ok()) {
        int64_t old_score_int;
        old_score_int = StrToInt64(old_score);
        score_key = EncodeZScoreKey(key, member, old_score_int);
        writebatch.Delete(score_key);
        score_key = EncodeZScoreKey(key, member, old_score_int + by);
        writebatch.Put(score_key, "");
        writebatch.Put(db_key, Int64ToStr(old_score_int + by));
    } else if (s.IsNotFound()) {
        score_key = EncodeZScoreKey(key, member, by);
        writebatch.Put(score_key, "");
        writebatch.Put(db_key, Int64ToStr(by));
        if (IncrZLen(key, 1, writebatch) != 0) {
            return Status::Corruption("incr zsize error");
        }
    } else {
        return Status::Corruption("get the key error");
    }
    s = zset_db_->Write(rocksdb::WriteOptions(), &writebatch);
    return s;

}

//TODO Range not rangebyscore
Status Nemo::ZRange(const std::string &key, const int64_t start, const int64_t stop, std::vector<SM> &sms) {
    int64_t t_start = start < 0 ? INT_MAX-1 : start;
    int64_t t_stop = stop < 0 ? INT_MAX : stop+1;
    ZIterator *iter = ZScan(key, t_start, t_stop, -1);
    while(iter->Next()) {
        sms.push_back({iter->Score(), iter->Member()});
    }
    return Status::OK();
}

Status Nemo::ZRangebyscore(const std::string &key, const int64_t start, const int64_t stop, std::vector<SM> &sms) {
    int64_t t_start = start < 0 ? INT_MAX-1 : start;
    int64_t t_stop = stop < 0 ? INT_MAX : stop+1;
    ZIterator *iter = ZScan(key, t_start, t_stop, -1);
    while(iter->Next()) {
        sms.push_back({iter->Score(), iter->Member()});
    }
    return Status::OK();
}

int Nemo::DoZSet(const std::string &key, const int64_t score, const std::string &member, rocksdb::WriteBatch &writebatch) {
    Status s;
    std::string old_score;
    std::string score_key;
    std::string db_key = EncodeZSetKey(key, member);
    s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);
    if (s.ok() && score != StrToInt64(old_score)) {
        score_key = EncodeZScoreKey(key, member, StrToInt64(old_score));
        writebatch.Delete(score_key);
        score_key = EncodeZScoreKey(key, member, score);
        writebatch.Put(score_key, "");
        writebatch.Put(db_key, Int64ToStr(score));
        return 1;
    } else if (s.IsNotFound()) {
        score_key = EncodeZScoreKey(key, member, score);
        writebatch.Put(score_key, "");
        writebatch.Put(db_key, Int64ToStr(score));
        return 2;
    } else if (s.ok() && score == StrToInt64(old_score)) {
        return 0;
    } else {
        return -1;
    }

}

int Nemo::IncrZLen(const std::string &key, int64_t by, rocksdb::WriteBatch &writebatch) {
    Status s;
    std::string size_key = EncodeZSizeKey(key);
    int64_t size = ZCard(key);
    if (size == -1) {
        return -1;
    }
    size += by;
    if (size <=0 ) {
        writebatch.Delete(size_key);
    } else {
        writebatch.Put(size_key, Int64ToStr(size));
    }
    return 0;
}
