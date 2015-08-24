#include "nemo.h"
#include "nemo_zset.h"
#include "nemo_iterator.h"
#include "utilities/util.h"
#include "xdebug.h"
using namespace nemo;

Status Nemo::ZAdd(const std::string &key, const double score, const std::string &member, int64_t *res) {
    Status s;
    if (score < ZSET_SCORE_MIN || score > ZSET_SCORE_MAX) {
        return Status::Corruption("zset score overflow");
    }

    //std::string db_key = EncodeZSetKey(key, member);
    //std::string size_key = EncodeZSizeKey(key);
    //std::string score_key = EncodeZScoreKey(key, member, score); 
    rocksdb::WriteBatch batch;
    MutexLock l(&mutex_zset_);
    int ret = DoZSet(key, score, member, batch);
    if (ret == 2) {
        if (IncrZLen(key, 1, batch) == 0) {
            s = zset_db_->Write(rocksdb::WriteOptions(), &batch);
            *res = 1;
            return s;
        } else {
            return Status::Corruption("incr zsize error");
        }
    } else if (ret == 1) {
        *res = 0;
        s = zset_db_->Write(rocksdb::WriteOptions(), &batch);
        return s;
    } else if (ret == 0) {
        *res = 0;
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
        int64_t ival;
        StrToInt64(size.data(), size.size(), &ival);
        return ival;
    } else if (s.IsNotFound()) {
        return 0;
    } else {
        return -1;
    }
}

ZIterator* Nemo::ZScan(const std::string &key, const double begin, const double end, uint64_t limit) {
    double rel_begin = begin;
    double rel_end = end;
    if (begin < ZSET_SCORE_MIN) {
      rel_begin = ZSET_SCORE_MIN;
    }
    if (end > ZSET_SCORE_MAX) {
      rel_end = ZSET_SCORE_MAX;
    }
    std::string key_start, key_end;
    key_start = EncodeZScoreKey(key, "", rel_begin);
    key_end = EncodeZScoreKey(key, "", rel_end);
    rocksdb::Iterator *it;
    rocksdb::ReadOptions iterate_options;
    iterate_options.fill_cache = false;
    it = zset_db_->NewIterator(iterate_options);
    it->Seek(key_start);
    return new ZIterator(new Iterator(it, key_end, limit), key); 
}

int64_t Nemo::ZCount(const std::string &key, const double begin, const double end) {
    ZIterator* it = ZScan(key, begin, end, -1);
    double s;
    int64_t n = 0;
    while (it->Next()) {
        s = it->Score();
        if (s >= begin && s <= end ) {
            n++;
        } else {
            break;
        }
    }
    return n;
}

Status Nemo::ZIncrby(const std::string &key, const std::string &member, const double by, std::string &new_score) {
    Status s;
    std::string old_score;
    std::string score_key;
    std::string db_key = EncodeZSetKey(key, member);
    rocksdb::WriteBatch writebatch;
    MutexLock l(&mutex_zset_);
    s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);
    double dval;
    if (s.ok()) {
        dval = *((double *)old_score.data());
        score_key = EncodeZScoreKey(key, member, dval);
        writebatch.Delete(score_key);

        dval += by;
        if (dval < ZSET_SCORE_MIN || dval > ZSET_SCORE_MAX) {
            return Status::Corruption("zset score overflow");
        }
        score_key = EncodeZScoreKey(key, member, dval);
        writebatch.Put(score_key, "");

        std::string buf;
        buf.append((char *)(&dval), sizeof(double));
        writebatch.Put(db_key, buf);
    } else if (s.IsNotFound()) {
        if (by < ZSET_SCORE_MIN || by > ZSET_SCORE_MAX) {
            return Status::Corruption("zset score overflow");
        }
        dval = by;
        score_key = EncodeZScoreKey(key, member, by);
        writebatch.Put(score_key, "");

        std::string buf;
        buf.append((char *)(&by), sizeof(double));
        writebatch.Put(db_key, buf);
        if (IncrZLen(key, 1, writebatch) != 0) {
            return Status::Corruption("incr zsize error");
        }
    } else {
        return Status::Corruption("get the key error");
    }
    std::string res = std::to_string(dval); 
    size_t pos = res.find_last_not_of("0", res.size());
    pos = pos == std::string::npos ? pos : pos+1;
    new_score = res.substr(0, pos); 
    if (new_score[new_score.size()-1] == '.') {
        new_score = new_score.substr(0, new_score.size()-1);
    }
    s = zset_db_->Write(rocksdb::WriteOptions(), &writebatch);
    return s;
}

//TODO Range not rangebyscore
Status Nemo::ZRange(const std::string &key, const int64_t start, const int64_t stop, std::vector<SM> &sms) {
    int64_t t_size = ZCard(key);
    if (t_size >= 0) {
        int64_t t_start = start >= 0 ? start : t_size + start;
        int64_t t_stop = stop >= 0 ? stop : t_size + stop;
        if (t_start < 0) {
            t_start = 0;
        }
        if (t_stop > t_size - 1) {
            t_stop = t_size - 1;
        }
        if (t_start > t_stop || t_start > t_size - 1 || t_stop < 0) {
            return Status::OK();
        } else {
            ZIterator *iter = ZScan(key, ZSET_SCORE_MIN, ZSET_SCORE_MAX, -1);
            if (iter == NULL) {
                return Status::Corruption("zscan error");
            }
            int32_t n = 0;
            while (n<t_start && iter->Next()) {
                n++;
            }
            if (n<t_start) {
                delete iter;
                return Status::Corruption("ziterate error");
            } else {
                while (n<=t_stop && iter->Next()) {
                    sms.push_back({iter->Score(), iter->Member()});
                    n++;
                }
                delete iter;
                return Status::OK();
            }
        }
    } else {
        return Status::Corruption("get zsize error");
    }
}

Status Nemo::ZRangebyscore(const std::string &key, const double mn, const double mx, std::vector<SM> &sms, uint64_t limit) {
    ZIterator *iter = ZScan(key, mn, mx, limit);
    while(iter->Next()) {
        sms.push_back({iter->Score(), iter->Member()});
    }
    return Status::OK();
}

int Nemo::DoZSet(const std::string &key, const double score, const std::string &member, rocksdb::WriteBatch &writebatch) {
    Status s;
    std::string old_score;
    std::string score_key;
    std::string db_key = EncodeZSetKey(key, member);

    s = zset_db_->Get(rocksdb::ReadOptions(), db_key, &old_score);
    if (s.ok()) {
        double dval = *((double *)old_score.data());
        /* find the same value */ 
        if (fabs(dval - score) < eps) {
            return 0;
        } else {
          score_key = EncodeZScoreKey(key, member, dval);
          writebatch.Delete(score_key);
          score_key = EncodeZScoreKey(key, member, score);
          writebatch.Put(score_key, "");

          std::string buf;
          buf.append((char *)(&score), sizeof(double));
          writebatch.Put(db_key, buf);
          return 1;
        }
    } else if (s.IsNotFound()) {
        score_key = EncodeZScoreKey(key, member, score);
        writebatch.Put(score_key, "");

        std::string buf;
        buf.append((char *)(&score), sizeof(double));
        writebatch.Put(db_key, buf);
        return 2;
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
    if (size <= 0) {
        writebatch.Delete(size_key);
    } else {
        writebatch.Put(size_key, std::to_string(size));
    }
    return 0;
}
