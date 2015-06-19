#include "nemo_iterator.h"
#include "nemo_kv.h"
#include "nemo_hash.h"
#include "nemo_zset.h"
#include "xdebug.h"

nemo::Iterator::Iterator(rocksdb::Iterator *it, const std::string &end, uint64_t limit, Direction direction)
    : it_(it),
    end_(end),
    limit_(limit),
    is_first_(true),
    direction_(direction) {
}

nemo::Iterator::~Iterator() {
    delete it_;
}

rocksdb::Slice nemo::Iterator::Key() {
    rocksdb::Slice s = it_->key();
    return rocksdb::Slice(s.data(), s.size());
}

rocksdb::Slice nemo::Iterator::Val() {
    rocksdb::Slice s = it_->value();
    return rocksdb::Slice(s.data(), s.size());
}

bool nemo::Iterator::Skip(uint64_t offset) {
    while (offset-- > 0) {
        if (this->Next() == false) {
            return false;
        }
    }
    return true;
}

bool nemo::Iterator::Next() {
    if (limit_ == 0) {
        return false;
    }
    if (is_first_) {
        is_first_ = false;
    } else {
        if(direction_ == kForward){
            it_->Next();
        }else{
            it_->Prev();
        }
    }

    if (!it_->Valid()) {
        // make next() safe to be called after previous return false.
        limit_ = 0;
        return false;
    }
    if (direction_ == kForward) {
        if (!end_.empty() && it_->key().compare(end_) > 0) {
            limit_ = 0;
            return false;
        }
    }else{
        if(!end_.empty() && it_->key().compare(end_) < 0) {
            limit_ = 0;
            return false;
        }
    }
    limit_ --;
    return true;
}

/***
 * KV
***/
nemo::KIterator::KIterator(Iterator *it) : it_(it) {
}

nemo::KIterator::~KIterator() {
    delete it_;
}


bool nemo::KIterator::Next() {
    while (it_->Next()) {
        rocksdb::Slice ks = it_->Key();
        rocksdb::Slice vs = it_->Val();
        if(ks.data()[0] != DataType::kKv){
            return false;
        }
        if(DecodeKvKey(ks, &this->key_) == -1){
            continue;
        }
        this->val_.assign(vs.data(), vs.size());
        return true;
    }
    return false;
}

/***
 * HASH
***/

nemo::HIterator::HIterator(Iterator *it, const rocksdb::Slice &key) 
    : it_(it) {
    this->key_.assign(key.data(), key.size());
}

nemo::HIterator::~HIterator() {
    delete it_;
}

bool nemo::HIterator::Next() {
    while (it_->Next()) {
        rocksdb::Slice ks = it_->Key();
        rocksdb::Slice vs = it_->Val();
        if (ks.data()[0] != DataType::kHash) {
            return false;
        }
        std::string k;
        if (DecodeHashKey(ks, &k, &this->field_) == -1) {
            continue;
        }
        if (k != this->key_) {
            return false;
        }
        this->val_.assign(vs.data(), vs.size());
        return true;
    }
    return false;
}

/***
 * ZSET
***/

nemo::ZIterator::ZIterator(Iterator *it, const rocksdb::Slice &key)
    : it_(it) {
    this->key_.assign(key.data(), key.size());
}

nemo::ZIterator::~ZIterator() {
    delete it_;
}

bool nemo::ZIterator::Next() {
    while (it_->Next()) {
        rocksdb::Slice ks = it_->Key();
//        rocksdb::Slice vs = it_->Val();
        if (ks.data()[0] != DataType::kZScore) {
            return false;
        }
        std::string k;
        if (DecodeZScoreKey(ks, &k, &this->member_, &this->score_) == -1) {
            continue;
        }
        if (k != this->key_) {
            return false;
        }
        return true;
    }
    return false;
}
