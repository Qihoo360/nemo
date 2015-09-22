#include <iostream>

#include "nemo_set.h"
#include "nemo_iterator.h"
#include "nemo_hash.h"
#include "nemo_zset.h"
#include "xdebug.h"

nemo::Iterator::Iterator(rocksdb::Iterator *it, const std::string &end, uint64_t limit, rocksdb::ReadOptions options, Direction direction)
    : it_(it),
    end_(end),
    limit_(limit),
    options_(options),
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

bool nemo::Iterator::SetPrevKeyVal() {
    if (it_->Valid()) {
        prev_key_ = it_->key().ToString();
        prev_val_ = it_->value().ToString();
        return true;
    }
    return false;
}

bool nemo::Iterator::Skip(uint64_t offset) {
    if (!it_->Valid()) {    // iterator is invalid
        return false;
    }

    while (offset-- > 0) {
        SetPrevKeyVal();

        if (direction_ == kForward){
            it_->Next();
        } else {
            it_->Prev();
        }

        if (!it_->Valid()) {
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
        this->key_.assign(ks.data(), ks.size());
        this->val_.assign(vs.data(), vs.size());
        return true;
    }
    return false;
}

bool nemo::KIterator::Skip(int64_t offset) {
    if (offset < 0 || !it_->Valid()) {    // iterator is invalid
        return false;
    }

    while (offset-- > 0) {
        it_->SetPrevKeyVal();

        if (it_->GetDirection() == kForward){
            it_->RawNext();
        } else {
            it_->RawPrev();
        }

        if (!it_->Valid()) {
            std::string str = it_->PrevKey();
            this->key_.assign(str.data(), str.size());

            str = it_->PrevVal();
            this->val_.assign(str.data(), str.size());
            break;
        } else {
            rocksdb::Slice ks = it_->Key();
            rocksdb::Slice vs = it_->Val();
            this->key_.assign(ks.data(), ks.size());
            this->val_.assign(vs.data(), vs.size());
        }
    }
    return true;
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

// check valid and assign filed_
bool nemo::HIterator::Valid() {
    if (it_->Valid()) {
        rocksdb::Slice ks = it_->Key();

        if (ks.data()[0] != DataType::kHash) {
            return false;
        }

        std::string k;
        if (DecodeHashKey(ks, &k, &this->field_) == -1) {
            return false;
        }
        if (k != this->key_) {
            return  false;
        }
        return true;
    }
    return false;
}

bool nemo::HIterator::Skip(int64_t offset) {
    if (offset < 0 || !it_->Valid()) {    // iterator is invalid
        return false;
    }

    while (offset-- > 0) {
        it_->SetPrevKeyVal();

        if (it_->GetDirection() == kForward){
            it_->RawNext();
        } else {
            it_->RawPrev();
        }

        if (!Valid()) {
            std::string key_str = it_->PrevKey();
            std::string k;

            if (DecodeHashKey(key_str, &k, &this->field_) == -1) {
                return false;
            }
            this->val_ = it_->PrevVal();
            return true;
        }
    }
    this->val_.assign(it_->Val().data(), it_->Val().size());

    return true;
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

// check valid and assign member_ and score_
bool nemo::ZIterator::Valid() {
    if (it_->Valid()) {
        rocksdb::Slice ks = it_->Key();
        //rocksdb::Slice vs = it_->Val();

        if (ks.data()[0] != DataType::kZScore) {
            return false;
        }

        std::string k;
        if (DecodeZScoreKey(ks, &k, &this->member_, &this->score_) == -1) {
            return false;
        }
        if (k != this->key_) {
            return  false;
        }
        return true;
    }
    return false;
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

bool nemo::ZIterator::Skip(int64_t offset) {
    if (offset < 0 || !it_->Valid()) {    // iterator is invalid
        return false;
    }

    while (offset-- > 0) {
        it_->SetPrevKeyVal();

        if (it_->GetDirection() == kForward){
            it_->RawNext();
        } else {
            it_->RawPrev();
        }

        if (!Valid()) {
            std::string key_str = it_->PrevKey();

            std::string k;
            if (DecodeZScoreKey(key_str, &k, &this->member_, &this->score_) == -1) {
                return false;
            }
            break;
        }
    }

    return true;
}

nemo::ZLexIterator::ZLexIterator(Iterator *it, const rocksdb::Slice &key)
    : it_(it) {
    this->key_.assign(key.data(), key.size());
}

nemo::ZLexIterator::~ZLexIterator() {
    delete it_;
}

bool nemo::ZLexIterator::Next() {
    while (it_->Next()) {
        rocksdb::Slice ks = it_->Key();
//        rocksdb::Slice vs = it_->Val();
        if (ks.data()[0] != DataType::kZSet) {
            return false;
        }
        std::string k;
        if (DecodeZSetKey(ks, &k, &this->member_) == -1) {
            continue;
        }
        if (k != this->key_) {
            return false;
        }
        return true;
    }
    return false;
}

/***
 * Set
***/

nemo::SIterator::SIterator(Iterator *it, const rocksdb::Slice &key) 
    : it_(it) {
    this->key_.assign(key.data(), key.size());
}

nemo::SIterator::~SIterator() {
    delete it_;
}

bool nemo::SIterator::Next() {
    while (it_->Next()) {
        rocksdb::Slice ks = it_->Key();
        if (ks.data()[0] != DataType::kSet) {
            return false;
        }
        std::string k;
        if (DecodeSetKey(ks, &k, &this->member_) == -1) {
            continue;
        }
        if (k != this->key_) {
            return false;
        }
        return true;
    }
    return false;
}

// check valid and assign member_
bool nemo::SIterator::Valid() {
    if (it_->Valid()) {
        rocksdb::Slice ks = it_->Key();

        if (ks.data()[0] != DataType::kSet) {
            return false;
        }

        std::string k;
        if (DecodeSetKey(ks, &k, &this->member_) == -1) {
            return false;
        }
        if (k != this->key_) {
            return  false;
        }
        return true;
    }
    return false;
}

bool nemo::SIterator::Skip(int64_t offset) {
    if (offset < 0 || !it_->Valid()) {    // iterator is invalid
        return false;
    }

    while (offset-- > 0) {
        it_->SetPrevKeyVal();

        if (it_->GetDirection() == kForward){
            it_->RawNext();
        } else {
            it_->RawPrev();
        }

        if (!Valid()) {
            std::string key_str = it_->PrevKey();
            std::string k;

            if (DecodeSetKey(key_str, &k, &this->member_) == -1) {
                return false;
            }
            return true;
        }
    }

    return true;
}

